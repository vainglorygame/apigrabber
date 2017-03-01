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
        logging.warning("connecting to database")
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
                match (id TEXT PRIMARY KEY, data JSONB)
            """)

        root = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__)))
        with open(root + "/insert.sql", "r", encoding="utf-8-sig") as file:
            self._insertquery = file.read()

    async def _execute_job(self, jobid, payload, priority):
        """Finish a job."""
        api = crawler.Crawler(self._apitoken)
        # if a player is queried, pass that information to processor
        if "filter[playerNames]" in payload["params"]:
            playername = payload["params"]["filter[playerNames]"]
        else:
            playername = ""

        async with self._pool.acquire() as con:
            async for data in api.matches(region=payload["region"],
                                          params=payload["params"]):
                matchids = await con.fetch(self._insertquery, json.dumps(data))
                logging.debug("%s: inserted %s matches from API into database",
                              jobid, len(matchids))
                for matchid in matchids:
                    await self._queue.request(jobtype="process",
                                              priority=priority,
                                              payload={
                                                  "id": matchid["id"],
                                                  "playername": playername
                                              })

    async def _work(self):
        """Fetch a job and run it."""
        jobid, payload, priority = await self._queue.acquire(jobtype="grab")
        if jobid is None:
            raise LookupError("no jobs available")
        try:
            await self._execute_job(jobid, payload, priority)
            await self._queue.finish(jobid)
        except crawler.ApiError as error:
            logging.warning("%s: failed with %s", jobid,
                            error.args[0])
            await self._queue.fail(jobid, error.args[0])

    async def run(self):
        """Start jobs forever."""
        while True:
            try:
                await self._work()
            except LookupError:
                await asyncio.sleep(1)

    async def start(self, number=1):
        """Start jobs in background."""
        for _ in range(number):
            asyncio.ensure_future(self.run())

async def startup():
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
    await worker.start(2)


logging.basicConfig(
    filename="logs/apigrabber.log",
    filemode="a",
    level=logging.DEBUG
)
console = logging.StreamHandler()
console.setLevel(logging.WARNING)
logging.getLogger("").addHandler(console)

loop = asyncio.get_event_loop()
loop.run_until_complete(startup())
loop.run_forever()
