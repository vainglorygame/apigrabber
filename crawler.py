#!/usr/bin/python

import json
import asyncio
import logging
import aiohttp

APIURL = "https://api.dc01.gamelockerapp.com/"


class ApiError(Exception):
    pass

class Crawler(object):
    def __init__(self, token):
        """Sets constants."""
        self._apiurl = APIURL
        self._token = token
        self._pagelimit = 50

    async def _req(self, session, path, params):
        """Sends an API request and returns the response dict.

        :param session: aiohttp client session.
        :type session: :class:`aiohttp.ClientSession`
        :param path: URL path.
        :type path: str
        :param params: Request parameters.
        :type params: dict
        :return: API response.
        :rtype: dict
        """
        headers = {
            "Authorization": "Bearer " + self._token,
            "X-TITLE-ID": "semc-vainglory",
            "Accept": "application/vnd.api+json",
            "Accept-Encoding": "gzip"
        }
        while True:
            try:
                async with session.get(self._apiurl + path, headers=headers,
                                       params=params) as response:
                    if response.status == 429:
                        logging.warning("rate limited, retrying")
                    else:
                        return await response.json()
            except (aiohttp.errors.ContentEncodingError,
                    aiohttp.errors.ServerDisconnectedError,
                    aiohttp.errors.ClientResponseError,
                    aiohttp.errors.ClientOSError,
                    LookupError,
                    json.decoder.JSONDecodeError) as err:
                # API bug?
                logging.error("API error '%s', retrying", err)
            await asyncio.sleep(10)

    async def matches(self, params, region="na"):
        """Queries the API for matches and their related data.

        :param region: (optional) Region where the matches were played.
                       Defaults to "na" (North America).
        :type region: str
        :param params: Additional filters.
        :type params: dict
        """
        params["page[offset]"] = 0
        params["page[limit]"] = self._pagelimit
        async with aiohttp.ClientSession() as session:
            while True:
                res = await self._req(session,
                                      "shards/" + region + "/matches",
                                      params)

                if "errors" in res:
                    if res["errors"][0].get("title") == "Not Found" \
                       and params["page[offset]"] > 0:
                        # a query returned exactly 50 matches
                        # which is expected, so don't fail.
                        return
                    raise ApiError(res["errors"])

                yield res

                if len(res["data"]) < self._pagelimit:
                    # asked for 50, got less -> exhausted
                    break
                params["page[offset]"] += params["page[limit]"]
