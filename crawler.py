#!/usr/bin/python

import json
import time
import logging
import requests

APIURL = "https://api.dc01.gamelockerapp.com/"


class ApiError(Exception):
    pass

class Crawler(object):
    def __init__(self, token):
        """Sets constants."""
        self._apiurl = APIURL
        self._token = token
        self._pagelimit = 50

    def _req(self, path, params):
        """Sends an API request and returns the response dict.

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
        retries = 5
        while True:
            try:
                response = requests.get(self._apiurl + path,
                                        headers=headers,
                                        params=params)
                if response.status_code == 429:
                    logging.warning("rate limited, retrying")
                else:
                    if response.status_code > 500:
                        logging.error("API server error %s",
                                      response.status_code)
                        raise ApiError(response.status_code)
                    else:
                        return response.json()
            except (json.decoder.JSONDecodeError) as err:
                # API bug?
                logging.error("API error '%s', retrying", err)
                retries -= 1
                if retries == 0:
                    logging.error("Giving up")
                    raise ApiError(str(err))

            time.sleep(5)

    def matches(self, params, region="na"):
        """Queries the API for matches and their related data.

        :param region: (optional) Region where the matches were played.
                       Defaults to "na" (North America).
        :type region: str
        :param params: Additional filters.
        :type params: dict
        """
        params["page[offset]"] = 0
        params["page[limit]"] = self._pagelimit
        while True:
            res = self._req("shards/" + region + "/matches",
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
