#!/usr/bin/env python
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from redis import ConnectionPool, StrictRedis
from datetime import datetime
from pprint import pformat
import backoff
import re
import msgpack
import traceback
import logging
import json
from builtins import str
from future import standard_library

import hysds
from hysds.celery import app
from hysds.log_utils import (
    log_job_status,
    backoff_max_tries,
    backoff_max_value,
    WORKER_STATUS_KEY_TMPL,
)
from hysds.event_processors import queue_fail_job, queue_offline_jobs

standard_library.install_aliases()


log_format = "[%(asctime)s: %(levelname)s/process_events] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

# redis connection pool
POOL = None

# regex for orchestrator tasks and workers
ORCH_HOST_RE = re.compile(r"^celery@orchestrator")
ORCH_NAME_RE = re.compile(r"^hysds.orchestrator.submit_job")

# regex for task-failed errors that won't be updated in ES because
# worker had no chance to send update
TASK_FAILED_RE = re.compile(r"^(WorkerLostError|TimeLimitExceeded)")

# regex for extracting type and hostname from worker
TYPE_RE = re.compile(r"'type': '(.+?)',")
HOSTNAME_RE = re.compile(r"^celery@(.+?)\..+$")

DATE_FORMAT = "%Y.%m.%d"


def set_redis_pool():
    """Set redis connection pool for status updates."""

    global POOL
    if POOL is None:
        POOL = ConnectionPool.from_url(app.conf.REDIS_UNIX_DOMAIN_SOCKET)


def parse_job_type(event):
    """Extract resource's job type."""

    # parse job type from worker task events
    hostname = event.get("hostname", "")
    match = HOSTNAME_RE.search(hostname)
    if match:
        return match.group(1)

    # parse job type from orchestrator task events
    args = event.get("args", "")
    match = TYPE_RE.search(args)
    if match:
        job_type = match.group(1)
    else:
        job_type = "unknown"
        logging.error(
            "Got exception trying to parse job type for %s:\n%s\n%s"
            % (hostname, json.dumps(event, indent=2), traceback.format_exc())
        )
    return job_type


def log_task_event(event_type, event, uuid=[]):
    """Print task event."""

    set_redis_pool()
    global POOL
    current_time = datetime.utcnow()
    info = {
        "resource": "task",
        "type": parse_job_type(event),
        "status": event_type,
        "celery_hostname": event.get("hostname", None),
        "uuid": uuid,
        "@version": "1",
        "@timestamp": "%sZ" % current_time.isoformat(),
        "event": event,
        "index": f"task_status-{current_time.strftime(DATE_FORMAT)}"
    }

    # send update to redis
    r = StrictRedis(connection_pool=POOL)
    r.rpush(app.conf.REDIS_JOB_STATUS_KEY, msgpack.dumps(info))

    # print log
    try:
        logging.info("hysds.task_event:%s" % json.dumps(info))
    except Exception as e:
        logging.error("Got exception trying to log task event: %s" % str(e))


def log_worker_event(event_type, event, uuid=[]):
    """Print worker event."""

    set_redis_pool()
    global POOL
    current_time = datetime.utcnow()
    info = {
        "resource": "worker",
        "type": parse_job_type(event),
        "status": event_type,
        "celery_hostname": event["hostname"],
        "uuid": uuid,
        "@version": "1",
        "@timestamp": "%sZ" % current_time.isoformat(),
        "event": event,
        "index": f"worker_status-{current_time.strftime(DATE_FORMAT)}"
    }

    # send update to redis
    r = StrictRedis(connection_pool=POOL)
    r.rpush(app.conf.REDIS_JOB_STATUS_KEY, msgpack.dumps(info))

    # print log
    try:
        logging.info("hysds.worker_event:%s" % json.dumps(info))
    except Exception as e:
        logging.error("Got exception trying to log worker event: %s" % str(e))


def log_worker_status(worker, status):
    """Print worker status."""

    set_redis_pool()
    global POOL

    # send update to redis; set at the heartbeat-interval of celery workers
    r = StrictRedis(connection_pool=POOL)
    r.setex(WORKER_STATUS_KEY_TMPL % worker, 60, status)

    # print log
    try:
        logging.info("hysds.worker_status:%s:%s" % (worker, status))
    except Exception as e:
        logging.error("Got exception trying to log worker status: %s" % str(e))


def event_monitor(app):
    state = app.events.State()

    def task_sent(event):
        state.event(event)
        if ORCH_HOST_RE.search(event["hostname"]) or ORCH_NAME_RE.search(event["name"]):
            return
        log_task_event("task-sent", event, uuid=event["uuid"])

    def task_received(event):
        state.event(event)
        if ORCH_HOST_RE.search(event["hostname"]):
            return
        log_task_event("task-received", event, uuid=event["uuid"])

    def task_started(event):
        state.event(event)
        if ORCH_HOST_RE.search(event["hostname"]):
            return
        log_task_event("task-started", event, uuid=event["uuid"])

    def task_succeeded(event):
        set_redis_pool()
        global POOL
        state.event(event)
        if ORCH_HOST_RE.search(event["hostname"]):
            return
        log_task_event("task-succeeded", event, uuid=event["uuid"])

    def task_failed(event):
        state.event(event)
        uuid = event["uuid"]
        exc = event.get("exception", "")
        if isinstance(exc, str):
            match = TASK_FAILED_RE.search(exc)
            if match:
                short_error = match.group(1)
                queue_fail_job(event, uuid, exc, short_error)
        log_task_event("task-failed", event, uuid=event["uuid"])

    def task_retried(event):
        state.event(event)
        if ORCH_HOST_RE.search(event["hostname"]):
            return
        log_task_event("task-retried", event, uuid=event["uuid"])

    def task_revoked(event):
        state.event(event)
        if ORCH_HOST_RE.search(event["hostname"]):
            return
        log_task_event("task-revoked", event, uuid=event["uuid"])

    def worker_online(event):
        state.event(event)
        if ORCH_HOST_RE.search(event["hostname"]):
            return
        log_worker_status(event["hostname"], event["type"])
        log_worker_event("worker-online", event)

    def worker_offline(event):
        set_redis_pool()
        global POOL
        rd = StrictRedis(connection_pool=POOL)
        state.event(event)
        if ORCH_HOST_RE.search(event["hostname"]):
            return
        rd.delete(WORKER_STATUS_KEY_TMPL % event["hostname"])
        queue_offline_jobs(event)
        log_worker_event("worker-offline", event)

    def worker_heartbeat(event):
        state.event(event)
        if ORCH_HOST_RE.search(event["hostname"]):
            return
        log_worker_status(event["hostname"], event["type"])
        log_worker_event("worker-heartbeat", event)

    def any_event(event):
        state.event(event)
        logging.info("EVENT: %s" % pformat(event))

    with app.connection() as connection:
        recv = app.events.Receiver(
            connection,
            handlers={
                "task-sent": task_sent,
                "task-received": task_received,
                "task-started": task_started,
                "task-succeeded": task_succeeded,
                "task-failed": task_failed,
                "task-retried": task_retried,
                "task-revoked": task_revoked,
                "worker-online": worker_online,
                "worker-offline": worker_offline,
                "worker-heartbeat": worker_heartbeat,
                # '*': any_event,
            },
        )
        recv.capture(limit=None, timeout=None, wakeup=True)


if __name__ == "__main__":
    event_monitor(app)
