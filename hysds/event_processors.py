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

from hysds.task_worker import run_task
from hysds.celery import app
from hysds.log_utils import logger, log_job_status, backoff_max_tries, backoff_max_value


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
    backoff.expo, socket.error, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def queue_fail_job(event, uuid, exc, short_error):
    """Queue job id for user_rules_job evaluation."""

    payload = {
        "type": "process_events",
        "function": "hysds.event_processors.fail_job",
        "args": [event, uuid, exc, short_error],
    }
    run_task.apply_async((payload,), queue=app.conf.PROCESS_EVENTS_TASKS_QUEUE)
