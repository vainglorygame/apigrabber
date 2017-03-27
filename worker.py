#!/usr/bin/python

import os
import logging

import crawler
import joblib.joblib


RABBIT = {
    "host": os.environ.get("RABBITMQ_HOST"),
    "port": os.environ.get("RABBITMQ_PORT"),
    "credentials": os.environ.get("RABBITMQ_CREDS")
}

APITOKEN = os.environ["MADGLORY_TOKEN"]


class Apigrabber(joblib.joblib.Worker):
    def __init__(self, apitoken):
        super().__init__("grab")
        self._apitoken = apitoken

    def work(self, payload):
        """Finish a job."""
        api = crawler.Crawler(self._apitoken)
        logging.info("running on %s with parameters '%s'",
                     payload["region"], payload["params"])
        try:
            for data in api.matches(region=payload["region"],
                                    params=payload["params"]):
                items = data["data"] + data["included"]
                for item in items:
                    self.request("process",
                                 payload={
                                     "id": item["id"],
                                     "type": item["type"],
                                     "data": item
                                 })
        except crawler.ApiError as error:
            logging.warning("API returned error '%s'", error.args[0])
            raise joblib.JobFailed(error.args[0],
                                   False)  # not critical


def startup():
    worker = Apigrabber(APITOKEN)
    worker.connect(**RABBIT)
    worker.setup()
    worker.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    startup()
