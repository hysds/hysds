from future import standard_library

standard_library.install_aliases()

import json
import re
import socket
import traceback

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
    ABSENT,
    SUPERSEDED,
    UNKNOWN,
    is_job_finalized,
    job_supersession,
    log_custom_event,
    log_job_status,
    logger,
)
from hysds.utils import datetime_iso_naive
from hysds.task_worker import run_task
from hysds.user_rules_job import queue_finished_job

mozart_es = get_mozart_es()

# task-failed patterns meaning "the worker had no chance to send an update".
# A soft time limit is not one: the worker catches it, triages, and logs its
# own job-failed doc. The lookbehind keeps SoftTimeLimitExceeded from
# matching TimeLimitExceeded.
TASK_FAILED_RE = re.compile(
    r"(?<![A-Za-z])(WorkerLostError|TimeLimitExceeded|ConnectionError)"
)


def fail_job(event, uuid, exc, short_error):
    """Set job status to job-failed."""

    # log_job_status() is async on the ES side, so the ES read below can
    # trail the worker's own terminal write; the redis key is authoritative
    # (same re-check pattern as offline_jobs below). The guard sits OUTSIDE
    # the retried body: _fail_job's own write sets the key to job-failed, so
    # a retried guard would read its own write and drop the requeue.
    if is_job_finalized(uuid):
        logger.info(
            f"fail_job - {uuid}: worker already finalized this job; "
            f"not overwriting. exc={exc}, short_error={short_error}"
        )
        return
    # Owned here, outside the retried body, so a backoff replay of _fail_job
    # can see that the terminal doc was already written and not write it
    # again. Without this, a broker outage after the write replays the whole
    # body up to five times: five job-failed writes for one _id, and five
    # paired logstash deletes. That is the duplicate this branch exists to
    # remove, in the one supervisory writer the reaper was built to repair.
    written = {"done": False}
    _fail_job(event, uuid, exc, short_error, written)


def _fail_job_gave_up(details):
    """Backoff exhausted: say so where it can be seen, not just in a log."""
    args = details.get("args") or ()
    uuid = args[1] if len(args) > 1 else None
    log_custom_event(
        "worker_anomaly",
        "fail_job_gave_up",
        {"uuid": uuid, "tries": details.get("tries"),
         "elapsed": details.get("elapsed")},
    )


@backoff.on_exception(
    backoff.expo, Exception, max_tries=5, max_value=10, on_giveup=_fail_job_gave_up
)
def _fail_job(event, uuid, exc, short_error, written=None):
    """Rewrite the job doc as job-failed and requeue rule evaluation."""
    if written is None:
        written = {"done": False}
    if written["done"]:
        # A backoff replay after the terminal doc was already handed to
        # log_job_status: only the requeue is outstanding. Do not re-read the
        # doc. By now the search can return our own job-failed write, and the
        # "already terminal" branch below would then drop the requeue.
        queue_finished_job(written["payload_id"], index="job_failed", uuid=uuid)
        return

    query = {"query": {"bool": {"must": [{"term": {"uuid": uuid}}]}}}

    result = mozart_es.search(index="job_status-current", body=query)
    total = result["hits"]["total"]["value"]
    if total == 0:
        msg = f"Failed to query for task UUID {uuid}"
        logger.error(msg)
        raise RuntimeError(msg)

    res = result["hits"]["hits"][0]
    job_status = res["_source"]

    if job_status["status"] == "job-started" or job_status["status"] == "job-queued":
        # A retry keeps the payload_id and mints a new uuid, so a live doc
        # owned by someone else means this attempt is history: rewriting it
        # would resurrect a doc the retry deleted. Guarded inside the branch
        # that writes, so an already-terminal doc does not pay for a probe
        # whose answer it would never use.
        state = job_supersession(
            job_status["payload_id"],
            uuid,
            retry_count=(job_status.get("job") or {}).get("retry_count"),
            index=(job_status.get("job") or {}).get("job_info", {}).get("index"),
            es=mozart_es,
        )
        if state == SUPERSEDED:
            logger.info(
                f"fail_job - {uuid}: a later attempt owns payload "
                f"{job_status['payload_id']}; not writing job-failed."
            )
            return
        if state == ABSENT:
            # Gone from every home: a retry deleted it while its
            # replacement is still in the pipeline. Writing now would
            # resurrect the old attempt AND, through logstash's paired
            # delete on job_info.index, remove the retried attempt's
            # fresh doc. Raise so this function's backoff re-reads.
            raise RuntimeError(
                f"payload {job_status['payload_id']} has no live status doc; "
                f"a retry is mid-flight, deferring"
            )
        if state == UNKNOWN:
            # Could not ask -- a probe raised and nothing was found. That
            # is a transport fault, not evidence of deletion; retry the
            # read rather than drop a failure record on it.
            raise RuntimeError(
                f"payload {job_status['payload_id']}: supersession probe "
                f"failed, retrying the read"
            )

        job_status["status"] = "job-failed"
        job_status["error"] = exc
        job_status["short_error"] = short_error
        job_status["traceback"] = event.get("traceback", "")

        time_end = datetime_iso_naive() + "Z"
        job_status.setdefault("job", {}).setdefault("job_info", {})[
            "time_end"
        ] = time_end
        log_job_status(job_status)
        written.update(done=True, payload_id=job_status["payload_id"])

        # Rules must be evaluated against job_failed, not res["_index"].
        # The doc just rewritten as job-failed is moved there by logstash
        # (see configs/logstash/indexer.conf.mozart), so settling on the dated
        # index the search hit either exhausts assert_doc_settled's backoff or
        # passes on the pre-move job-started doc. This is the same index
        # job_worker.py passes for a worker-written failure.
        queue_finished_job(
            job_status["payload_id"], index="job_failed", uuid=job_status.get("uuid")
        )
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

    time_end = datetime_iso_naive() + "Z"
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
                # Same guard as every other supervisory writer. The redis pair
                # above expires at HYSDS_JOB_STATUS_EXPIRES and nothing clears
                # it on revoke, so after a retry the old attempt's keys can
                # still say job-started while a newer attempt owns the payload
                # -- and job-offline would land on that attempt's live doc,
                # which the reaper never sees.
                state = job_supersession(
                    job_status_json.get("payload_id"),
                    uuid,
                    retry_count=(job_status_json.get("job") or {}).get("retry_count"),
                    index=(job_status_json.get("job") or {}).get("job_info", {}).get("index"),
                    es=mozart_es,
                )
                if state in (SUPERSEDED, ABSENT):
                    logger.info(f"Not offlining job with UUID {uuid}: {state}")
                    continue
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
