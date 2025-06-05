#!/usr/bin/env python
from future import standard_library

standard_library.install_aliases()
import os
import sys
import json
import time
import traceback
import logging
import argparse
import random

from hysds.celery import app
import job_utils

log_format = "[%(asctime)s: %(levelname)s/watchdog_task_timeouts] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


def tag_timedout_tasks(url, timeout):
    """Tag tasks stuck in task-started that have timed out."""

    status = ["task-started"]
    source_data = ["status", "tags", "uuid"]
    query = job_utils.get_timedout_query(timeout, status, source_data)
    print(json.dumps(query, indent=2))

    results = job_utils.run_query_with_scroll(query, index="task_status-current")
    print(results)
    logging.info(
        "Found %d stuck tasks in task-started or task-offline" % len(results)
        + " older than %d seconds." % timeout
    )

    # tag each with timedout
    for res in results:
        _id = res["_id"]
        _index = res["_index"]
        src = res.get("_source", {})
        # status = src["status"]
        tags = src.get("tags", [])
        # task_id = src["uuid"]

        if "timedout" not in tags:
            tags.append("timedout")
            new_doc = {"doc": {"tags": tags}, "doc_as_upsert": True}

            response = job_utils.update_es(_id, new_doc, index=_index)
            if response["result"].strip() != "updated":
                err_str = f"Failed to update status for {_id} : {json.dumps(response, indent=2)}"
                logging.error(err_str)
                raise Exception(err_str)

            logging.info("Tagged %s as timedout." % _id)
        else:
            logging.info("%s already tagged as timedout." % _id)


def daemon(interval, url, timeout):
    """Watch for tasks that have timed out in task-started state."""

    interval_min = interval - int(interval / 4)
    interval_max = int(interval / 4) + interval

    logging.info("interval min: %d" % interval_min)
    logging.info("interval max: %d" % interval_max)
    logging.info("url: %s" % url)
    logging.info("timeout threshold: %d" % timeout)

    while True:
        try:
            tag_timedout_tasks(url, timeout)
        except Exception as e:
            logging.error("Got error: %s" % e)
            logging.error(traceback.format_exc())
        time.sleep(random.randint(interval_min, interval_max))


if __name__ == "__main__":
    desc = "Watchdog tasks stuck in task-started."
    host = app.conf.get("JOBS_ES_URL", "http://localhost:9200")
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        "-i",
        "--interval",
        type=int,
        default=3600,
        help="wake-up time interval in seconds",
    )
    parser.add_argument("-u", "--url", default=host, help="ElasticSearch URL")
    parser.add_argument(
        "-t", "--timeout", type=int, default=86400, help="timeout threshold"
    )
    args = parser.parse_args()
    daemon(args.interval, args.url, args.timeout)
