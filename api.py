#!/usr/bin/python

import asyncio
import os

import database
import crawler


db = database.Database()


# TODO use logging module instead of print
async def crawl_region(region):
    """Gets some matches from a region and inserts them
       until the DB is up to date."""
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
            last_match_update = "2017-02-05T01:01:01Z"

        print(region + " fetching matches after " + last_match_update)

        # wait for http requests
        matches = await api.matches_since(last_match_update,
                                          region=region,
                                          params={"page[limit]": 50})
        if len(matches) > 0:
            print(region + " got new data items: " + str(len(matches)))
        else:
            print(region + " got no new matches.")
            return
        # insert asynchronously in the background
        await db.upsert(matches, True)


async def crawl_forever():
    """Gets the latest matches from all regions every 5 minutes."""
    # repeat forever
    while True:
        print("getting recent matches")

        # TODO: insert API version (force update if changed)
        # TODO: create database indices
        # get or put when the last crawl was executed

        # crawl and upsert
        tasks = []
        for region in ["na", "eu"]:
            # fire workers
            tasks.append(asyncio.ensure_future(crawl_region(region)))

        await asyncio.gather(*tasks)  # wait until all have completed
        await asyncio.sleep(300)


loop = asyncio.get_event_loop()
loop.run_until_complete(db.connect(
    host=os.environ["POSTGRESQL_HOST"],
    port=os.environ["POSTGRESQL_PORT"],
    user=os.environ["POSTGRESQL_USER"],
    password=os.environ["POSTGRESQL_PASSWORD"],
    database=os.environ["POSTGRESQL_DB"]
))
loop.run_until_complete(
    crawl_forever()
)
