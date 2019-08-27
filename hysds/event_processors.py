from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import


from future import standard_library

standard_library.install_aliases()
import os
import sys
import json
import requests
import time
import backoff
import socket
import traceback
from datetime import datetime
from redis import ConnectionPool, StrictRedis

from hysds.task_worker import run_task
from hysds.celery import app
from hysds.log_utils import (
    logger,
    log_job_status,
    backoff_max_tries,
    backoff_max_value,
    JOB_STATUS_KEY_TMPL,
    JOB_WORKER_KEY_TMPL,
)


# redis connection pool
POOL = None


def set_redis_pool():
    """Set redis connection pool for status updates."""

    global POOL
    if POOL is None:
        POOL = ConnectionPool.from_url(app.conf.REDIS_UNIX_DOMAIN_SOCKET)


@backoff.on_exception(
    backoff.expo, Exception, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def fail_job(event, uuid, exc, short_error):
    """Set job status to job-failed."""

    query = {"query": {"bool": {"must": [{"term": {"uuid": uuid}}]}}}
    search_url = "%s/job_status-current/job/_search" % (app.conf["JOBS_ES_URL"],)
    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code != 200:
        logger.error("Failed to query for task UUID %s: %s" % (uuid, r.content))
        return
    result = r.json()
    total = result["hits"]["total"]
    if total == 0:
        logger.error("Failed to query for task UUID %s: %s" % (uuid, r.content))
        return
    res = result["hits"]["hits"][0]
    job_status = res["_source"]
    job_status["status"] = "job-failed"
    job_status["error"] = exc
    job_status["short_error"] = short_error
    job_status["traceback"] = event.get("traceback", "")
    time_end = datetime.utcnow().isoformat() + "Z"
    job_status.setdefault("job", {}).setdefault("job_info", {})["time_end"] = time_end
    log_job_status(job_status)


@backoff.on_exception(
    backoff.expo, Exception, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def offline_jobs(event):
    """Set job status to job-offline."""

    set_redis_pool()
    global POOL
    rd = StrictRedis(connection_pool=POOL)
    time_end = datetime.utcnow().isoformat() + "Z"
    query = {
        "query": {
            "filtered": {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"celery_hostname": event["hostname"]}},
                            {"term": {"status": "job-started"}},
                        ]
                    }
                }
            }
        }
    }
    job_status_jsons = []
    es_url = (
        "%s/job_status-current/_search?search_type=scan&scroll=60m&size=100"
        % app.conf["JOBS_ES_URL"]
    )
    uuids = []
    try:
        r = requests.post(es_url, data=json.dumps(query))
        r.raise_for_status()
        scan_result = r.json()
        scroll_id = scan_result["_scroll_id"]
        while True:
            r = requests.post(
                "%s/_search/scroll?scroll=60m" % app.conf["JOBS_ES_URL"], data=scroll_id
            )
            res = r.json()
            scroll_id = res["_scroll_id"]
            if len(res["hits"]["hits"]) == 0:
                break
            for hit in res["hits"]["hits"]:
                job_status_jsons.append(hit["_source"])
        logger.info(
            "Got {} jobs for {}.".format(len(job_status_jsons), event["hostname"])
        )
        for job_status_json in job_status_jsons:
            uuid = job_status_json["uuid"]
            # offline the job only if it hasn't been picked up by another worker
            cur_job_status = rd.get(JOB_STATUS_KEY_TMPL % uuid).decode()
            cur_job_worker = rd.get(JOB_WORKER_KEY_TMPL % uuid).decode()
            logger.info("cur_job_status: {}".format(cur_job_status))
            logger.info("cur_job_worker: {}".format(cur_job_worker))
            if cur_job_status == "job-started" and cur_job_worker == event["hostname"]:
                job_status_json["status"] = "job-offline"
                job_status_json[
                    "error"
                ] = "Received worker-offline event during job execution."
                job_status_json["short_error"] = "worker-offline"
                job_status_json.setdefault("job", {}).setdefault("job_info", {})[
                    "time_end"
                ] = time_end
                log_job_status(job_status_json)
                logger.info("Offlined job with UUID {}.".format(uuid))
                uuids.append(uuid)
            else:
                logger.info(
                    "Not offlining job with UUID {} since real-time job status doesn't match.".format(
                        uuid
                    )
                )
    except Exception as e:
        logger.warn(
            "Got exception trying to update task events for "
            + "offline worker %s: %s\n%s"
            % (event["hostname"], str(e), traceback.format_exc())
        )


@backoff.on_exception(
    backoff.expo, socket.error, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def queue_fail_job(event, uuid, exc, short_error):
    """Queue task to set job status to job-failed."""

    payload = {
        "type": "process_events",
        "function": "hysds.event_processors.fail_job",
        "args": [event, uuid, exc, short_error],
    }
    run_task.apply_async((payload,), queue=app.conf.PROCESS_EVENTS_TASKS_QUEUE)


@backoff.on_exception(
    backoff.expo, socket.error, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def queue_offline_jobs(event):
    """Queue task to set job status to job-offine."""

    payload = {
        "type": "process_events",
        "function": "hysds.event_processors.offline_jobs",
        "args": [event],
    }
    run_task.apply_async((payload,), queue=app.conf.PROCESS_EVENTS_TASKS_QUEUE)
