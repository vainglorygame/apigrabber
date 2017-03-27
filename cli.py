#!/usr/bin/python3
import os
import argparse

import joblib.joblib


RABBIT = {
    "host": os.environ.get("RABBITMQ_HOST"),
    "port": os.environ.get("RABBITMQ_PORT"),
    "credentials": os.environ.get("RABBITMQ_CREDS")
}


def main(args):
    queue = joblib.joblib.JobQueue()
    queue.connect(**RABBIT)

    if args.player:
        payload = {
            "region": args.region,
            "params": {
                "filter[createdAt-start]": "2017-02-01T00:00:00Z",
                "filter[playerNames]": args.player
            }
        }
        queue.request("grab", payload)

parser = argparse.ArgumentParser(description="Request a Vainsocial update.")
parser.add_argument("-n", "--player",
                    help="Player name",
                    type=str)
parser.add_argument("-r", "--region",
                    help="Specify a region",
                    type=str,
                    choices=["na", "eu", "sg"],
                    default="na")

if __name__ == "__main__":
    main(parser.parse_args())
