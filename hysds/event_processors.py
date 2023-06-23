from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library

standard_library.install_aliases()

import json
import requests
import backoff
import socket
import traceback
from datetime import datetime

from hysds.task_worker import run_task
from hysds.celery import app
from hysds.log_utils import (
    logger,
    log_job_status,
    get_val_via_socket,
    backoff_max_tries,
    backoff_max_value,
    TASK_WORKER_KEY_TMPL,
    JOB_STATUS_KEY_TMPL,
)
from hysds.es_util import get_mozart_es


mozart_es = get_mozart_es()


@backoff.on_exception(
    backoff.expo, Exception, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def fail_job(event, uuid, exc, short_error):
    """Set job status to job-failed."""

    query = {
        "query": {
            "bool": {
                "must": [{"term": {"uuid": uuid}}]
            }
        }
    }

    result = mozart_es.search(index="job_status-current", body=query)
    total = result["hits"]["total"]["value"]
    if total == 0:
        logger.error("Failed to query for task UUID %s" % uuid)
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

    time_end = datetime.utcnow().isoformat() + "Z"
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"celery_hostname": event["hostname"]}},
                    {"term": {"status": "job-started"}},
                ]
            }
        }
    }
    logger.info("offline jobs query: %s" % json.dumps(query))
    uuids = []

    try:
        job_status_jsons = mozart_es.query(index="job_status-current", body=query)
        logger.info(
            "Got {} jobs for {}.".format(len(job_status_jsons), event["hostname"])
        )

        for job_status in job_status_jsons:
            job_status_json = job_status["_source"]
            uuid = job_status_json["uuid"]

            # offline the job only if it hasn't been picked up by another worker
            cur_job_status = get_val_via_socket(JOB_STATUS_KEY_TMPL % uuid)
            cur_job_worker = get_val_via_socket(TASK_WORKER_KEY_TMPL % uuid)
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
                logger.info("Offlined job with UUID %s" % uuid)
                uuids.append(uuid)
            else:
                logger.info(
                    "Not offlining job with UUID %s since real-time job status doesn't match"
                    % uuid
                )
    except Exception as e:
        logger.warn(
            "Got exception trying to update task events for offline worker %s: %s\n%s"
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
