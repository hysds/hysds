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
from datetime import datetime, timezone

import job_utils

from hysds.celery import app
from hysds.log_utils import log_job_status
from hysds.utils import get_short_error, parse_iso8601

log_format = "[%(asctime)s: %(levelname)s/watchdog_job_timeouts] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

UNDETERMINED_BY_WATCHDOG = "undetermined by watchdog"


def tag_timedout_jobs(url, timeout):
    """Tag jobs stuck in job-started or job-offline that have timed out."""

    status = ["job-started", "job-offline"]
    # HC-594: Retrieve full _source to ensure we have all fields needed for log_job_status()
    # Previously only retrieved 7 partial fields which caused partial record recreation
    # Build query inline without _source restriction to get all fields
    logging.info(f"HC-594: Querying for jobs with status {status} older than {timeout}s with full _source")
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
    results = job_utils.run_query_with_scroll(query, index="job_status-current")
    logging.info(
        f"HC-594: Found {len(results)} stuck jobs in job-started or job-offline"
        + f" older than {timeout} seconds."
    )

    # tag each with timedout
    for res in results:
        _id = res["_id"]
        _index = res["_index"]
        src = res.get("_source", {})
        status = src["status"]
        tags = src.get("tags", [])
        task_id = src["uuid"]
        celery_hostname = src["celery_hostname"]

        # HC-594: Log document state for verification
        field_count = len(src.keys())
        has_timestamp = "@timestamp" in src
        has_resource = "resource" in src
        has_version = "@version" in src
        logging.info(
            f"HC-594: Processing job {_id} from index {_index}: "
            f"field_count={field_count}, status={status}, "
            f"has_@timestamp={has_timestamp}, has_resource={has_resource}, has_@version={has_version}"
        )
        logging.info(f"job_info: {json.dumps(src)}")

        # get job duration
        time_limit = src["job"]["job_info"]["time_limit"]
        logging.info(f"time_limit: {time_limit}")
        time_start = parse_iso8601(src["job"]["job_info"]["time_start"])
        logging.info(f"time_start: {time_start}")
        time_now = datetime.now(timezone.utc)
        logging.info(f"time_now: {time_now}")
        duration = (time_now - time_start).total_seconds()
        logging.info(f"duration: {duration}")

        if status == "job-started":
            # get task info, sort by latest since we only look at the first hit
            task_query = {
                "query": {"term": {"_id": task_id}},
                "_source": ["status", "event"],
                "sort": [{"@timestamp": {"order": "desc"}}],
            }
            task_res = job_utils.es_query(task_query, index="task_status-current")

            if len(task_res["hits"]["hits"]) == 0:
                logging.error(
                    f"No result found with : query\n{json.dumps(task_query, indent=2)}"
                )

            logging.info(f"task_res: {json.dumps(task_res)}")

            # get worker info
            worker_query = {
                "query": {"term": {"_id": celery_hostname}},
                "_source": ["status", "tags"],
                "sort": [{"@timestamp": "desc"}],
            }

            worker_res = job_utils.es_query(worker_query, index="worker_status-current")

            if len(worker_res["hits"]["hits"]) == 0:
                logging.error(
                    f"No result found with : query\n{json.dumps(worker_query, indent=2)}"
                )

            logging.info(f"worker_res: {json.dumps(worker_res)}")
            error = None
            short_error = None
            traceback = None
            # determine new status
            new_status = status
            if len(worker_res["hits"]["hits"]) == 0 and duration > time_limit:
                new_status = "job-offline"
            if len(worker_res["hits"]["hits"]) > 0 and (
                "timedout" in worker_res["hits"]["hits"][0]["_source"].get("tags", [])
                or worker_res["hits"]["hits"][0]["_source"]["status"]
                == "worker-offline"
            ):
                new_status = "job-offline"
            if len(task_res["hits"]["hits"]) > 0:
                task_info = task_res["hits"]["hits"][0]
                if task_info["_source"]["status"] == "task-failed":
                    new_status = "job-failed"
                    error = (
                        task_info.get("_source", {})
                        .get("event", {})
                        .get("exception", UNDETERMINED_BY_WATCHDOG)
                    )
                    short_error = get_short_error(error)
                    traceback = (
                        task_info.get("_source", {})
                        .get("event", {})
                        .get("traceback", UNDETERMINED_BY_WATCHDOG)
                    )

            # update status
            if status != new_status:
                logging.info(
                    f"HC-594: Status change detected for {_id}: {status} -> {new_status} "
                    f"(duration={duration}s, time_limit={time_limit}s)"
                )
                if duration > time_limit and "timedout" not in tags:
                    logging.info(f"HC-594: Adding 'timedout' tag to {_index}/{_id}")
                    tags.append("timedout")
                else:
                    logging.info(
                        f"HC-594: NOT adding 'timedout' tag: duration={duration}s <= time_limit={time_limit}s "
                        f"or already has tag"
                    )

                # HC-594: Update job_status_json in memory with new values
                src["status"] = new_status
                src["tags"] = tags
                if error:
                    src["error"] = error
                    src["short_error"] = short_error
                    src["traceback"] = traceback
                    logging.info(f"HC-594: Added error fields to job {_id}")

                # HC-594: Use log_job_status() to ensure all required fields are populated
                # and the update goes through the proper Redis->Logstash pipeline.
                # This prevents the race condition where doc_as_upsert recreates partial records.
                logging.info(
                    f"HC-594: Calling log_job_status() for {_id} to update via pipeline "
                    f"(field_count={len(src.keys())})"
                )
                try:
                    log_job_status(src)
                    logging.info(
                        f"HC-594: SUCCESS - Updated job {_id} to {new_status} via log_job_status(). "
                        f"Document will have all {len(src.keys())} fields plus @timestamp, @version, resource."
                    )
                except Exception as e:
                    logging.error(f"HC-594: FAILED to log job status for {_id}: {e}")
                    logging.error(traceback.format_exc())
                continue

        if "timedout" in tags:
            logging.info(f"HC-594: Job {_id} already has 'timedout' tag, no action needed")
        else:
            if duration > time_limit:
                logging.info(
                    f"HC-594: Job {_id} exceeded time limit (duration={duration}s > time_limit={time_limit}s), "
                    f"adding 'timedout' tag"
                )
                tags.append("timedout")
                src["tags"] = tags

                # HC-594: Use log_job_status() to ensure all required fields are populated
                logging.info(
                    f"HC-594: Calling log_job_status() for {_id} to add timedout tag via pipeline "
                    f"(field_count={len(src.keys())})"
                )
                try:
                    log_job_status(src)
                    logging.info(
                        f"HC-594: SUCCESS - Tagged job {_id} as timedout via log_job_status(). "
                        f"Document preserved with all {len(src.keys())} fields."
                    )
                except Exception as e:
                    logging.error(f"HC-594: FAILED to log job status for {_id}: {e}")
                    logging.error(traceback.format_exc())


def daemon(interval, url, timeout):
    """Watch for jobs that have timed out in job-started or job-offline state."""

    interval_min = interval - int(interval / 4)
    interval_max = int(interval / 4) + interval

    logging.info(f"interval min: {interval_min}")
    logging.info(f"interval max: {interval_max}")
    logging.info(f"url: {url}")
    logging.info(f"timeout threshold: {timeout}")

    while True:
        try:
            tag_timedout_jobs(url, timeout)
        except Exception as e:
            logging.error(f"Got error: {e}")
            logging.error(traceback.format_exc())
            traceback.format_exc()
        time.sleep(random.randint(interval_min, interval_max))


if __name__ == "__main__":
    desc = "Watchdog jobs stuck in job-offline or job-started."
    host = app.conf.get("JOBS_ES_URL", "http://localhost:9200")
    logging.info(f"host : {host}")
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
