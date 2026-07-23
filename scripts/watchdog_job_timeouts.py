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
from hysds.log_utils import get_job_status, is_job_finalized, log_job_status
from hysds.utils import get_short_error, parse_iso8601

log_format = "[%(asctime)s: %(levelname)s/watchdog_job_timeouts] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

UNDETERMINED_BY_WATCHDOG = "undetermined by watchdog"


def tag_timedout_jobs(url, timeout, grace_secs=300):
    """Tag jobs stuck in job-started or job-offline that have timed out."""

    status = ["job-started", "job-offline"]
    # Retrieve full _source to ensure we have all fields needed for log_job_status()
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
    results = job_utils.run_query_with_scroll(query, index="job_status-current")
    logging.info(
        f"Found {len(results)} stuck jobs in job-started or job-offline"
        + f" older than {timeout} seconds."
    )

    # tag each with timedout
    divergence_count = 0
    for res in results:
        _id = res["_id"]
        _index = res["_index"]
        src = res.get("_source", {})
        try:
            status = src["status"]
            tags = src.get("tags", [])
            task_id = src["uuid"]
            celery_hostname = src["celery_hostname"]

            # get job duration
            time_limit = src["job"]["job_info"]["time_limit"]
            if isinstance(time_limit, bool) or not isinstance(
                time_limit, (int, float)
            ):
                raise TypeError(f"non-numeric time_limit: {time_limit!r}")
            time_start = parse_iso8601(src["job"]["job_info"]["time_start"])
        except (KeyError, TypeError, ValueError) as e:
            # a partial doc must not abort the sweep: an escaped exception
            # strands every job later in the scroll
            logging.error(f"Job {_id}: skipping partial job_status doc ({e}).")
            continue
        time_now = datetime.now(timezone.utc)
        duration = (time_now - time_start).total_seconds()

        if status == "job-started":
            # get task info, sort by latest since we only look at the first hit
            task_query = {
                "query": {"term": {"_id": task_id}},
                # @timestamp: the grace period is measured from the
                # task-failed event
                "_source": ["status", "event", "@timestamp"],
                "sort": [{"@timestamp": {"order": "desc"}}],
            }
            task_res = job_utils.es_query(task_query, index="task_status-current")

            if len(task_res["hits"]["hits"]) == 0:
                logging.error(
                    f"No result found with : query\n{json.dumps(task_query, indent=2)}"
                )

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

            error = None
            short_error = None
            error_traceback = None
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
                    # grace runs from the task-failed event (not job start)
                    # so the worker's async job-failed doc can land in ES;
                    # deferral writes nothing (a write would re-stamp
                    # @timestamp and reset eligibility)
                    task_ts = task_info["_source"].get("@timestamp")
                    if task_ts is not None:
                        try:
                            age = (time_now - parse_iso8601(task_ts)).total_seconds()
                        except (ValueError, TypeError):
                            # malformed/non-string @timestamp: fail open to
                            # the redis guard rather than abort the sweep
                            age = None
                        if age is not None and age < grace_secs:
                            logging.info(
                                f"Job {_id}: task-failed {age:.0f}s ago, within "
                                f"{grace_secs}s grace; deferring to the worker."
                            )
                            continue
                    new_status = "job-failed"
                    error = (
                        task_info.get("_source", {})
                        .get("event", {})
                        .get("exception", UNDETERMINED_BY_WATCHDOG)
                    )
                    short_error = get_short_error(error)
                    error_traceback = (
                        task_info.get("_source", {})
                        .get("event", {})
                        .get("traceback", UNDETERMINED_BY_WATCHDOG)
                    )

            # update status
            if status != new_status:
                if duration > time_limit and "timedout" not in tags:
                    tags.append("timedout")

                # Update job_status_json in memory with new values
                src["status"] = new_status
                src["tags"] = tags
                if error:
                    src["error"] = error
                    src["short_error"] = short_error
                    src["traceback"] = error_traceback

                # guard immediately before the write (not loop top:
                # job-offline docs must still reach the tag lane below)
                if is_job_finalized(task_id):
                    redis_status = get_job_status(task_id)
                    if redis_status != status:
                        # terminal in redis but the ES doc never converged:
                        # the health signal for a lost terminal write
                        divergence_count += 1
                        logging.warning(
                            f"Job {_id}: DIVERGENCE redis={redis_status} "
                            f"es={status}; declining rewrite."
                        )
                    else:
                        logging.info(
                            f"Job {_id}: worker already finalized; not overwriting."
                        )
                    continue

                # Use log_job_status() to ensure all required fields are populated
                # and the update goes through the proper Redis->Logstash pipeline
                try:
                    log_job_status(src)
                    logging.info(f"Set job {_id} to {new_status} via log_job_status().")
                except Exception as e:
                    logging.error(f"Failed to log job status for {_id}: {e}")
                    logging.error(traceback.format_exc())
                continue

        if "timedout" not in tags:
            if duration > time_limit:
                # this path republishes the whole stale _source, status
                # included; never over a finalized job
                if is_job_finalized(task_id):
                    redis_status = get_job_status(task_id)
                    if redis_status != status:
                        divergence_count += 1
                        logging.warning(
                            f"Job {_id}: DIVERGENCE redis={redis_status} "
                            f"es={status}; declining tag write."
                        )
                    else:
                        logging.info(
                            f"Job {_id}: worker already finalized; not tagging "
                            f"via stale doc."
                        )
                    continue
                tags.append("timedout")
                src["tags"] = tags

                # Use log_job_status() to ensure all required fields are populated
                try:
                    log_job_status(src)
                    logging.info(f"Tagged job {_id} as timedout via log_job_status().")
                except Exception as e:
                    logging.error(f"Failed to log job status for {_id}: {e}")
                    logging.error(traceback.format_exc())

    if divergence_count:
        # per-sweep health metric: alarm on this line to see strands that
        # converge only when the redis key expires
        logging.warning(
            f"divergence_count={divergence_count} docs with a terminal redis "
            f"status but a stale non-terminal ES doc this sweep"
        )


def daemon(interval, url, timeout, grace_secs=300):
    """Watch for jobs that have timed out in job-started or job-offline state."""

    interval_min = interval - int(interval / 4)
    interval_max = int(interval / 4) + interval

    logging.info(f"interval min: {interval_min}")
    logging.info(f"interval max: {interval_max}")
    logging.info(f"url: {url}")
    logging.info(f"timeout threshold: {timeout}")
    logging.info(f"task-failed grace period: {grace_secs}")

    while True:
        try:
            tag_timedout_jobs(url, timeout, grace_secs)
        except Exception as e:
            logging.error(f"Got error: {e}")
            logging.error(traceback.format_exc())
            traceback.format_exc()
        time.sleep(random.randint(interval_min, interval_max))


if __name__ == "__main__":
    desc = "Watchdog jobs stuck in job-offline or job-started."
    host = app.conf.get("JOBS_ES_URL", "https://localhost:9200")
    logging.info("host : {}".format(host))
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
    # a flag (not an env var) so deployments that override -t/-i on the
    # supervisord command line can set it the same way
    parser.add_argument(
        "-g",
        "--grace-secs",
        type=int,
        default=300,
        help="seconds to wait after a task-failed event before rewriting the "
        "job doc, giving the worker's own async job-failed doc time to land "
        "in ES",
    )
    args = parser.parse_args()
    daemon(args.interval, args.url, args.timeout, args.grace_secs)
