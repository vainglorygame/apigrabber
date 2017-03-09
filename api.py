#!/usr/bin/python

import asyncio
import os
import logging
import json
import asyncpg

import crawler
import joblib.worker


class Apigrabber(joblib.worker.Worker):
    def __init__(self, apitoken):
        self._apitoken = apitoken
        self._con = None
        self._insertquery = None
        super().__init__(jobtype="grab")

    async def connect(self, **args):
        """Connect to database."""
        logging.warning("connecting to database")
        await super().connect(**args)
        self._con = await asyncpg.connect(**args)

    async def setup(self):
        """Initialize the database."""
        logging.info("initializing database")
        await self._con.execute("""
            CREATE TABLE IF NOT EXISTS
            match (id TEXT PRIMARY KEY, data JSONB)
        """)

        root = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__)))
        with open(root + "/insert.sql", "r", encoding="utf-8-sig") as file:
            self._insertquery = await self._con.prepare(
                file.read())

    async def _execute_job(self, jobid, payload, priority):
        """Finish a job."""
        api = crawler.Crawler(self._apitoken)
        # if a player is queried, pass that information to processor
        if "filter[playerNames]" in payload["params"]:
            playername = payload["params"]["filter[playerNames]"]
        else:
            playername = ""

        logging.debug("%s: running on %s with parameters '%s'",
                      jobid, payload["region"], payload["params"])
        try:
            async for data in api.matches(region=payload["region"],
                                          params=payload["params"]):
                async with self._con.transaction():
                    matchids = await self._insertquery.fetch(
                        json.dumps(data))
                    logging.info("%s: inserted %s matches for player '%s' from API into database",
                                 jobid, len(matchids), playername)
                payloads = [{
                    "id": mat["id"],
                    "playername": playername
                } for mat in matchids]
                await self._queue.request(jobtype="process",
                                          payload=payloads,
                                          priority=priority)
        except crawler.ApiError as error:
            logging.warning("%s: API returned error '%s'",
                            jobid, error.args[0])
            raise joblib.worker.JobFailed(error.args[0])


async def startup():
    for _ in range(1):
        worker = Apigrabber(
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
        await worker.start()


if __name__ == "__main__":
    logging.basicConfig(
        filename=os.path.realpath(
            os.path.join(os.getcwd(),
                         os.path.dirname(__file__))) +
            "/logs/apigrabber.log",
        filemode="a",
        level=logging.DEBUG
    )
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    logging.getLogger("").addHandler(console)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(startup())
    loop.run_forever()
