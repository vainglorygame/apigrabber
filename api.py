#!/usr/bin/python

import asyncio
import os
import logging
import json
import asyncpg

import crawler
import joblib.joblib


class Worker(object):
    def __init__(self, apitoken):
        self._apitoken = apitoken
        self._queue = None
        self._pool = None
        self._insertquery = ""

    async def connect(self, **args):
        """Connect to database."""
        logging.info("connecting to database")
        self._queue = joblib.joblib.JobQueue()
        await self._queue.connect(**args)
        await self._queue.setup()
        self._pool = await asyncpg.create_pool(**args)

    async def setup(self):
        """Initialize the database."""
        logging.info("initializing database")
        async with self._pool.acquire() as con:
            await con.execute("""
                CREATE TABLE IF NOT EXISTS
                match (
                    id TEXT PRIMARY KEY,
                    type TEXT DEFAULT 'match',
                    attributes JSONB,
                    relations JSONB
                )
                """)
            await con.execute("""
                CREATE TABLE IF NOT EXISTS
                player (
                    id TEXT PRIMARY KEY,
                    type TEXT DEFAULT 'player',
                    attributes JSONB
                )
                """)

        root = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__)))
        with open(root + "/insert.sql", "r", encoding="utf-8-sig") as file:
            self._insertquery = file.read()

    async def _execute_job(self, jobid, payload):
        """Finish a job."""
        api = crawler.Crawler(self._apitoken)
        logging.debug("%s: getting matches from API", jobid)
        async with self._pool.acquire() as con:
            async for data in api.matches(region=payload["region"],
                                          params=payload["params"]):
                logging.debug("%s: inserting into database", jobid)
                ids = await con.fetch(self._insertquery, json.dumps(data))
                logging.info("%s: inserted %s objects", jobid, len(ids))
                object_ids = [i["id"] for i in ids]
                for object_id in object_ids:
                    await self._queue.request(jobtype="process",
                                              payload={"id": object_id})

    async def _work(self):
        """Fetch a job and run it."""
        jobid, payload = await self._queue.acquire(jobtype="grab")
        if jobid is None:
            raise LookupError("no jobs available")
        logging.debug("%s: starting job", jobid)
        await self._execute_job(jobid, payload)
        await self._queue.finish(jobid)
        logging.debug("%s: finished job", jobid)

    async def run(self):
        """Start jobs forever."""
        while True:
            try:
                await self._work()
            except LookupError:
                logging.info("nothing to do, idling")
                await asyncio.sleep(10)


async def startup():
    for _ in range(1):
        worker = Worker(
            apitoken=os.environ["VAINSOCIAL_APITOKEN"]
        )
        await worker.connect(
            host=os.environ["POSTGRESQL_HOST"],
            port=os.environ["POSTGRESQL_PORT"],
            user=os.environ["POSTGRESQL_USER"],
            password=os.environ["POSTGRESQL_PASSWORD"],
            database=os.environ["POSTGRESQL_DB"]
        )
        await worker.setup()
        await worker.run()

logging.basicConfig(level=logging.DEBUG)
loop = asyncio.get_event_loop()
loop.run_until_complete(startup())
loop.run_forever()
