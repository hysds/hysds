from future import standard_library

standard_library.install_aliases()

import json
import socket
import traceback
from datetime import UTC, datetime

import backoff
import requests

from hysds.celery import app
from hysds.es_util import get_mozart_es
from hysds.log_utils import (
    JOB_STATUS_KEY_TMPL,
    TASK_WORKER_KEY_TMPL,
    backoff_max_tries,
    backoff_max_value,
    get_val_via_socket,
    log_job_status,
    logger,
)
from hysds.task_worker import run_task
from hysds.user_rules_job import queue_finished_job

mozart_es = get_mozart_es()


@backoff.on_exception(backoff.expo, Exception, max_tries=5, max_value=10)
def fail_job(event, uuid, exc, short_error):
    """Set job status to job-failed."""

    query = {"query": {"bool": {"must": [{"term": {"uuid": uuid}}]}}}

    result = mozart_es.search(index="job_status-current", body=query)
    # TODO: Remove this after debugging
    logger.info(f"job status from fail_job: {json.dumps(result, indent=2)}")
    total = result["hits"]["total"]["value"]
    logger.info(f"total results back from fail_job function: {total}")
    if total == 0:
        msg = f"Failed to query for task UUID {uuid}"
        logger.error(msg)
        raise RuntimeError(msg)

    res = result["hits"]["hits"][0]
    job_status = res["_source"]
    if job_status["status"] == "job-started" or job_status["status"] == "job-queued":
        job_status["status"] = "job-failed"
        job_status["error"] = exc
        job_status["short_error"] = short_error
        job_status["traceback"] = event.get("traceback", "")

        time_end = datetime.now(UTC).isoformat() + "Z"
        job_status.setdefault("job", {}).setdefault("job_info", {})[
            "time_end"
        ] = time_end
        log_job_status(job_status)

        queue_finished_job(job_status["payload_id"], index=res["_index"])
    else:
        logger.info(
            f"fail_job - {uuid}: Will not re-log and requeue job as job status is already set "
            f"to {job_status['status']}. exc={exc}, short_error={short_error}\n"
            f"traceback={event.get('traceback', '')}"
        )


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
    logger.info(f"offline jobs query: {json.dumps(query)}")
    uuids = []

    try:
        job_status_jsons = mozart_es.query(index="job_status-current", body=query)
        logger.info(f"Got {len(job_status_jsons)} jobs for {event['hostname']}.")

        for job_status in job_status_jsons:
            job_status_json = job_status["_source"]
            uuid = job_status_json["uuid"]

            # offline the job only if it hasn't been picked up by another worker
            cur_job_status = get_val_via_socket(JOB_STATUS_KEY_TMPL % uuid)
            cur_job_worker = get_val_via_socket(TASK_WORKER_KEY_TMPL % uuid)
            logger.info(f"cur_job_status: {cur_job_status}")
            logger.info(f"cur_job_worker: {cur_job_worker}")

            if cur_job_status == "job-started" and cur_job_worker == event["hostname"]:
                job_status_json["status"] = "job-offline"
                job_status_json["error"] = (
                    "Received worker-offline event during job execution."
                )
                job_status_json["short_error"] = "worker-offline"
                job_status_json.setdefault("job", {}).setdefault("job_info", {})[
                    "time_end"
                ] = time_end
                log_job_status(job_status_json)
                logger.info(f"Offlined job with UUID {uuid}")
                uuids.append(uuid)
            else:
                logger.info(
                    f"Not offlining job with UUID {uuid} since real-time job status doesn't match"
                )
    except Exception as e:
        logger.warning(
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
