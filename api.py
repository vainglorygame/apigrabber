#!/usr/bin/python

import asyncio
import os
import logging
import datetime
import json
import random
import asyncpg

import crawler


# SEMC API is a bit strict about the iso format
def date2iso(d):
    """Convert datetime to iso8601 string."""
    date = d.isoformat()
    date = ".".join(date.split(".")[:-1])  # remove microseconds
    date = date + "Z"
    return date


def iso2date(d):
    """Convert iso8601 string to date."""
    d = d.replace(":", "").replace("-", "")
    d = datetime.datetime.strptime(d, "%Y%m%dT%H%M%SZ")
    return d


class Apigrabber(object):
    def __init__(self):
        #self.regions = ["na", "eu", "sg", "ea", "sa", "cn"]
        self.regions = ["na", "eu"]

    async def connect(self, **args):
        self._pool = await asyncpg.create_pool(**args)

    async def _db_setup(self, con):
        """Create tables and indices."""
        await con.execute("""
            CREATE TABLE IF NOT EXISTS crawljobs
                (id SERIAL, start_date TIMESTAMP,
                end_date TIMESTAMP, finished BOOL, region TEXT)
            """)
        await con.execute("""
            CREATE TABLE IF NOT EXISTS apidata(
                id TEXT PRIMARY KEY NOT NULL,
                type TEXT NOT NULL,
                attributes JSONB,
                relationships JSONB)
            """)
        # indices are not used
        #logging.warn("creating database indices")
        ## TODO use less indices: take up 1/3 of the storage!!!
        #await con.execute(
        #    "CREATE UNIQUE INDEX CONCURRENTLY ON apidata (id, type)")
        #await con.execute(
        #    "CREATE UNIQUE INDEX CONCURRENTLY ON apidata (id, type, (attributes->>'shardId'))")
        #await con.execute(
        #    "CREATE INDEX CONCURRENTLY ON apidata ((attributes->>'name')) WHERE type='player'")  # TODO make unique once the devs fixed empty shardId in responses

        # create past zombie job that marks the last data to fetch
        async with con.transaction():
            await con.execute("""
                INSERT INTO crawljobs(start_date, end_date, finished, region)
                SELECT '2017-02-14T00:00:00Z'::TIMESTAMP,
                       '2017-02-14T00:00:00Z'::TIMESTAMP,
                       TRUE,
                       region
                FROM JSONB_TO_RECORDSET($1::JSONB) AS jsn(region TEXT)
                ON CONFLICT DO NOTHING;
            """, json.dumps([{"region": r} for r in self.regions]))

    async def _db_insert(self, con, objects, upsert=False):
        # TODO need to determine based on data whether to upsert or not
        # TODO currently, a player object is never updated.
        data = json.dumps(objects)  # TODO is data -> json -> data inefficient?
        await con.execute("""
            INSERT INTO apidata
                SELECT * FROM
                JSONB_TO_RECORDSET($1::JSONB)
                AS jsn(id TEXT, type TEXT, attributes JSONB, relationships JSONB)
                WHERE type!='player' AND type!='team'
                ORDER BY id
        """, data)
        while True:
            try:
                async with con.transaction():  # create savepoint
                    # objects could include a player twice -> DISTINCT
                    # another worker could try to modify the same player as we do
                    # so we create a savepoint and retry when we fail with a deadlock
                    await con.execute("""
                    INSERT INTO apidata
                        SELECT DISTINCT ON(id) * FROM
                        JSONB_TO_RECORDSET($1::JSONB)
                        AS jsn(id TEXT, type TEXT, attributes JSONB, relationships JSONB)
                        WHERE type='player' OR type='team'
                        ORDER BY id
                    """ + ("""
                    ON CONFLICT(id) DO NOTHING
                    """ if upsert else """
                    ON CONFLICT(id) DO UPDATE SET
                        attributes=EXCLUDED.attributes,
                        relationships=EXCLUDED.relationships
                    """), data)
                    break  # success, return
            except asyncpg.exceptions.DeadlockDetectedError:
                # try again, there were other objects that don't conflict
                logging.warn("ouch! database deadlocked during data insert, retrying")
                await asyncio.sleep(random.random())

    async def crawl_timeframe(self, region, jobid, jobstart, jobend):
        """Crawl a time frame forwards from `date` in `region`."""
        async with self._pool.acquire() as con:
            api = crawler.Crawler()
            async with con.transaction():
                params = {
                    "filter[createdAt-start]": date2iso(jobstart),
                    "filter[createdAt-end]": date2iso(jobend)
                }
                logging.debug("%s: (%s) fetching from %s to %s",
                              region, jobid, jobstart, jobend)
                matches = await api.matches(region=region, params=params)
                if len(matches) == 0:
                    # TODO ensure that there is no valid query without data
                    # so we won't query the same empty query forever
                    logging.warn("%s: (%s) did not get any data!", region, jobid)
                    # will be retried once pending jobs were cleared up
                    return

                logging.info("%s: (%s) received %s data objects",
                             region, jobid, len(matches))
                # historical data doesn't override
                await self._db_insert(con, matches, False)
                logging.info("%s: (%s) inserted",
                             region, jobid)

                # mark job as done
                await con.execute("""
                    UPDATE crawljobs SET finished=true WHERE id=$1""", jobid)

    async def crawl_region(self, region):
        """Get the match history from a region."""
        default_diff = 15  # default job length in minutes
        async with self._pool.acquire() as con:
            while True:
                try:
                    async with con.transaction(isolation="serializable"):
                        # select us our job

                        row_res = await con.fetchrow("""
                        SELECT
                          start_date,  -- new job's end date
                          LEAST(EXTRACT(EPOCH FROM (start_date-previous_end))/60, $2)  -- gap in minutes or default if smaller
                          FROM (
                            SELECT
                            start_date,
                            LAG(end_date) OVER (ORDER BY start_date ASC) AS previous_end
                            FROM crawljobs
                            WHERE region=$1
                            ORDER BY start_date ASC
                           ) AS d
                        WHERE start_date-previous_end>INTERVAL '0'  -- get gaps
                        ORDER BY start_date DESC LIMIT 1
                        """, region, default_diff)
                        if row_res == None:
                            logging.warn("%s: no jobs available. idling.", region)
                            await asyncio.sleep(60)  # a minute TODO make this smarter
                            asyncio.ensure_future(self.crawl_region(region))

                        jobdate, delta_minutes = row_res
                        delta = datetime.timedelta(minutes=delta_minutes)
                        # store our job as pending
                        jobid = await con.fetchval("""
                            INSERT INTO crawljobs(start_date, end_date, finished, region)
                            VALUES ($1, $2, false, $3)
                            RETURNING id
                        """, jobdate-delta, jobdate, region)
                        # exit loop
                        break
                except asyncpg.exceptions.SerializationError:
                    await asyncio.sleep(random.random())
                    # job is being picked up by another worker, try again

        await self.crawl_timeframe(region,
                                   jobid,
                                   jobdate-delta,
                                   jobdate)

        asyncio.ensure_future(self.crawl_region(region))  # restart self

    async def request_update(self, region):
        async with self._pool.acquire() as con:
            while True:
                try:
                    async with con.transaction(isolation="serializable"):
                        # insert a job from now->now
                        # so history crawler picks up the time diff between
                        # now and the last query.
                        await con.fetchval("""
                            INSERT INTO crawljobs(start_date, end_date, finished, region)
                            VALUES (NOW(), NOW(), TRUE, $1)
                        """, region)
                        logging.info("%s: scheduled for live update", region)
                        break
                except asyncpg.exceptions.SerializationError:
                    await asyncio.sleep(random.random())

        async def _recall_later():
            await asyncio.sleep(300)  # wait 5 min and repeat
            asyncio.ensure_future(self.request_update(region))
        asyncio.ensure_future(_recall_later())


    async def start(self):
        """Start the tasks that pull the data."""
        # TODO: respawn a worker if it dies because of connection issues
        # TODO: insert API version (force update if changed)
        # TODO: create database indices (id & shardId & type)
        # TODO: make workers switch regions flexibly to meet demand?

        for region in self.regions:
            await self.request_update(region)
            for _ in range(3):
                # supports scaling :]
                asyncio.ensure_future(self.crawl_region(region))

    async def setup(self):
        async with self._pool.acquire() as con:
            await self._db_setup(con)
            ## clean up after force quit (TODO - disabled for dev)
            #await con.execute("DELETE FROM crawljobs WHERE finished=false")


async def startup():
    apigrabber = Apigrabber()
    await apigrabber.connect(
        host=os.environ["POSTGRESQL_HOST"],
        port=os.environ["POSTGRESQL_PORT"],
        user=os.environ["POSTGRESQL_USER"],
        password=os.environ["POSTGRESQL_PASSWORD"],
        database=os.environ["POSTGRESQL_DB"]
    )
    await apigrabber.setup()
    await apigrabber.start()

logging.basicConfig(level=logging.DEBUG)
loop = asyncio.get_event_loop()
loop.run_until_complete(startup())
loop.run_forever()
