#!/usr/bin/python

import asyncio
import os
import logging

import database
import crawler


db = database.Database()


async def crawl_region(region):
    """Get matches from a region and insert them
       until the DB is up to date. Repeat after five minutes."""
    api = crawler.Crawler()

    # fetch until exhausted
    while True:
        try:
            last_match_update = (await db.select(
                """
                SELECT data->'attributes'->>'createdAt' AS created
                FROM match
                WHERE data->'attributes'->>'shardId'='""" + region + """'
                ORDER BY data->'attributes'->>'createdAt' DESC LIMIT 1
                """)
            )[0]["created"]
        except:
            last_match_update = "2017-02-07T01:01:01Z"  # TODO

        logging.info("%s: fetching matches since %s",
                     region, last_match_update)

        # wait for http requests
        try:
            matches = await api.matches_since(last_match_update,
                                              region=region,
                                              params={"page[limit]": 50})
        except:
            logging.error("%s: connection error, retrying", region)
            await asyncio.sleep(5)

        if len(matches) > 0:
            logging.debug("%s: %s objects", region, len(matches))
        else:
            logging.debug("%s: no objects, stopping", region)
            break
        # wait for db inserts
        await db.upsert(matches, True)

    logging.debug("%s: going to sleep", region)
    await asyncio.sleep(300)
    asyncio.ensure_future(crawl_region(region))  # restart self


async def start_crawlers():
    """Start the tasks that pull the data."""
    # TODO: insert API version (force update if changed)
    # TODO: create database indices

    for region in ["na", "eu", "sg", "ea", "sa", "cn"]:
        # fire workers
        asyncio.ensure_future(crawl_region(region))


logging.basicConfig(level=logging.DEBUG)
loop = asyncio.get_event_loop()
loop.run_until_complete(db.connect(
    host=os.environ["POSTGRESQL_HOST"],
    port=os.environ["POSTGRESQL_PORT"],
    user=os.environ["POSTGRESQL_USER"],
    password=os.environ["POSTGRESQL_PASSWORD"],
    database=os.environ["POSTGRESQL_DB"]
))
loop.run_until_complete(
    start_crawlers()
)
loop.run_forever()
