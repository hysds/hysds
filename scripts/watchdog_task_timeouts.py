#!/usr/bin/env python
from future import standard_library

standard_library.install_aliases()
import argparse
import json
import logging
import os
import random
import sys
import time
import traceback

import job_utils

from hysds.celery import app

log_format = "[%(asctime)s: %(levelname)s/watchdog_task_timeouts] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


def tag_timedout_tasks(url, timeout):
    """Tag tasks stuck in task-started that have timed out."""

    status = ["task-started"]
    # Retrieve full _source to ensure we have all fields
    # Build query inline without _source restriction to get all fields
    query = {
        "query": {
            "bool": {
                "must": [
                    {"terms": {"status": status}},
                    {"range": {"@timestamp": {"lt": f"now-{timeout}s"}}},
                ]
            }
        }
    }
    print(json.dumps(query, indent=2))

    results = job_utils.run_query_with_scroll(query, index="task_status-current")
    print(results)
    logging.info(
        f"Found {len(results)} stuck tasks in task-started or task-offline"
        + f" older than {timeout} seconds."
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
            src["tags"] = tags
            # Remove doc_as_upsert to prevent creating new documents after deletion
            # If document doesn't exist, the update will fail gracefully
            new_doc = {"doc": {"tags": tags}}

            try:
                response = job_utils.update_es(_id, new_doc, index=_index)
                if response["result"].strip() == "updated":
                    logging.info(f"Tagged task {_id} as timedout.")
                else:
                    logging.warning(
                        f"Unexpected result when updating task {_id}: {json.dumps(response, indent=2)}"
                    )
            except Exception as e:
                # Document may have been deleted - log warning but don't fail
                # This prevents the race condition from causing exceptions
                logging.warning(
                    f"Failed to update task {_id} (document may not exist): {e}"
                )


def daemon(interval, url, timeout):
    """Watch for tasks that have timed out in task-started state."""

    interval_min = interval - int(interval / 4)
    interval_max = int(interval / 4) + interval

    logging.info(f"interval min: {interval_min}")
    logging.info(f"interval max: {interval_max}")
    logging.info(f"url: {url}")
    logging.info(f"timeout threshold: {timeout}")

    while True:
        try:
            tag_timedout_tasks(url, timeout)
        except Exception as e:
            logging.error(f"Got error: {e}")
            logging.error(traceback.format_exc())
        time.sleep(random.randint(interval_min, interval_max))


if __name__ == "__main__":
    desc = "Watchdog tasks stuck in task-started."
    host = app.conf.get("JOBS_ES_URL", "https://localhost:9200")
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
