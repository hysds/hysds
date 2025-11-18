#!/usr/bin/env python
"""
Sync jobs stuck in job-queued or job-started state that may have been
de-synchronized. It searches through task statuses stored in ES for
final celery task status and resynchronizes job status. If no task
status exists in ES, it queries the celery task metadata store in redis
for task status. If none exists, the job is offlined.
"""

from future import standard_library

standard_library.install_aliases()
import argparse
import json
import logging
import os
import sys

import requests
from kombu.serialization import loads, prepare_accept_content, registry
from redis import ConnectionPool, StrictRedis

from hysds.celery import app
from hysds.log_utils import log_job_status
from hysds.utils import datetime_iso_naive

log_format = "[%(asctime)s: %(levelname)s/offline_orphaned_jobs] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

# redis connection pool
POOL = None


def set_redis_pool():
    """Set redis connection pool for status updates."""

    global POOL
    if POOL is None:
        POOL = ConnectionPool.from_url(app.conf.REDIS_UNIX_DOMAIN_SOCKET)


def offline_orphaned_jobs(es_url, dry_run=False):
    """Set jobs with job-queued or job-started state to job-offline
    if not synced with celery task state."""

    # get redis connection
    set_redis_pool()
    global POOL
    rd = StrictRedis(connection_pool=POOL,
                     ssl_ciphers=app.conf.get("broker_use_ssl", {}).get("ciphers"))

    # get celery task result serializer
    content_type, content_encoding, encoder = registry._encoders[
        app.conf.CELERY_RESULT_SERIALIZER
    ]
    accept = prepare_accept_content(app.conf.CELERY_ACCEPT_CONTENT)
    logging.info(f"content_type: {content_type}")
    logging.info(f"content_encoding: {content_encoding}")
    logging.info(f"encoder: {encoder}")
    logging.info(f"accept: {accept}")

    # query - retrieve full _source to ensure we have all fields for log_job_status()
    query = {
        "query": {
            "bool": {"must": [{"terms": {"status": ["job-started", "job-queued"]}}]}
        },
    }
    url_tmpl = "{}/job_status-current/_search?search_type=scan&scroll=10m&size=100"
    r = requests.post(url_tmpl.format(es_url), data=json.dumps(query))
    if r.status_code != 200:
        logging.error(
            "Failed to query ES. Got status code %d:\n%s"
            % (r.status_code, json.dumps(query, indent=2))
        )
    r.raise_for_status()
    scan_result = r.json()
    count = scan_result["hits"]["total"]
    scroll_id = scan_result["_scroll_id"]

    # get list of results
    results = []
    while True:
        r = requests.post(f"{es_url}/_search/scroll?scroll=10m", data=scroll_id)
        res = r.json()
        scroll_id = res["_scroll_id"]
        if len(res["hits"]["hits"]) == 0:
            break
        for hit in res["hits"]["hits"]:
            results.append(hit)

    # check for celery state
    for res in results:
        id = res["_id"]
        src = res.get("_source", {})
        status = src["status"]
        tags = src.get("tags", [])
        task_id = src["uuid"]

        # check celery task status in ES
        task_query = {"query": {"term": {"_id": task_id}}, "_source": ["status"]}
        r = requests.post(
            f"{es_url}/task_status-current/task/_search", data=json.dumps(task_query)
        )
        if r.status_code != 200:
            logging.error(
                "Failed to query ES. Got status code %d:\n%s"
                % (r.status_code, json.dumps(task_query, indent=2))
            )
            continue
        task_res = r.json()
        if task_res["hits"]["total"] > 0:
            task_info = task_res["hits"]["hits"][0]
            if task_info["_source"]["status"] == "task-failed":
                updated_status = "job-failed"
            elif task_info["_source"]["status"] == "task-succeeded":
                updated_status = "job-completed"
            elif task_info["_source"]["status"] in ("task-sent", "task-started"):
                continue
            else:
                logging.error(
                    f"Cannot handle task status {task_info['_source']['status']} for {task_id}."
                )
                continue
            if dry_run:
                logging.info(
                    f"Would've update job status to {updated_status} for {task_id}."
                )
            else:
                # Update job_status_json in memory and use log_job_status()
                src["status"] = updated_status
                time_end = datetime_iso_naive() + "Z"
                src.setdefault("job", {}).setdefault("job_info", {})["time_end"] = time_end
                try:
                    log_job_status(src)
                    logging.info(f"Set job {id} to {updated_status} via log_job_status().")
                except Exception as e:
                    logging.error(f"Failed to log job status for {id}: {e}")
            continue

        # get celery task metadata in redis
        task_meta = loads(
            rd.get(f"celery-task-meta-{task_id}"),
            content_type=content_type,
            content_encoding=content_encoding,
            accept=accept,
        )
        if task_meta is None:
            updated_status = "job-offline"
            if dry_run:
                logging.info(
                    f"Would've update job status to {updated_status} for {task_id}."
                )
            else:
                # Update job_status_json in memory and use log_job_status()
                src["status"] = updated_status
                time_end = datetime_iso_naive() + "Z"
                src.setdefault("job", {}).setdefault("job_info", {})["time_end"] = time_end
                try:
                    log_job_status(src)
                    logging.info(f"Set job {id} to {updated_status} via log_job_status().")
                except Exception as e:
                    logging.error(f"Failed to log job status for {id}: {e}")
            continue


if __name__ == "__main__":
    host = app.conf.get("JOBS_ES_URL", "https://localhost:9200")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-d", "--dry_run", help="dry run", action="store_true")
    args = parser.parse_args()

    # prompt user system is quiet
    print(
        "\033[1;31;40mThis script should run when no workers are consuming jobs from any queues."
    )
    print("(i.e. RabbitMQ admin shows all 0's in the unacked column).\033[0m")
    try:
        input_str = input("Type YES to continue: ").strip()
    except:
        sys.exit(1)
    if input_str != "YES":
        sys.exit(0)

    offline_orphaned_jobs(host, args.dry_run)
