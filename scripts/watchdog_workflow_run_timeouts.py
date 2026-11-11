#!/usr/bin/env python
"""Workflow run reaper (contract 12.6): run timeouts, stalled-run event replay, and a sweep of
leftover jobs of cancelled/failed runs. Thin loop around ``hysds.workflow_reaper.sweep_once``.

Env (flags override): WORKFLOW_REAPER_INTERVAL (300 s), WORKFLOW_STALL_SECONDS (600 s),
WORKFLOW_RUN_TIMEOUT_HOURS (48), WORKFLOW_REVOKE_JOB_TYPE (default: look up
job-lw-mozart-revoke:<version> in job_specs), WORKFLOW_STATUS_INDEX / WORKFLOW_SPECS_INDEX.
Runs on the mozart host: direct OpenSearch, jobs submitted via ``app.conf.MOZART_REST_URL``.
"""
import argparse
import functools
import logging
import os
import random
import time

from hysds.workflow_reaper import (
    DEFAULT_RUN_TIMEOUT_HOURS,
    DEFAULT_STALL_SECONDS,
    ReaperConfig,
    sweep_once,
)

log_format = "[%(asctime)s: %(levelname)s/watchdog_workflow_run_timeouts] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

DEFAULT_INTERVAL = 300


def publish_event(payload):
    """Publish to WORKFLOW_QUEUE the way log_utils.publish_workflow_event does."""
    from kombu import Exchange, Queue

    from hysds.celery import app
    from hysds.log_utils import get_workflow_queue_producer

    queue_name = app.conf.WORKFLOW_QUEUE
    exchange = Exchange(queue_name, type="direct")
    get_workflow_queue_producer().publish(
        payload, exchange=exchange, routing_key=queue_name,
        declare=[Queue(queue_name, exchange, routing_key=queue_name)], retry=True)


def daemon(interval, cfg, once=False):
    from hysds.celery import app
    from hysds.es_util import get_mozart_es
    from hysds_commons.workflow_submit import submit_workflow_job

    es = get_mozart_es().es
    submit = functools.partial(submit_workflow_job, mozart_rest_url=app.conf.MOZART_REST_URL)
    state = {}
    while True:
        try:
            s = sweep_once(es, cfg, submit, publish_event, state)
            logging.info("sweep: %d runs; timed out %d, stalled %d (replayed %d), swept %d (revoked %d)",
                         s["runs"], len(s["timeout"]), len(s["stalled"]), s["replayed"],
                         len(s["sweep"]), s["revoked"])
        except Exception:
            logging.exception("sweep failed")
        if once:
            return
        time.sleep(random.randint(int(interval * 0.9), int(interval * 1.1)))


if __name__ == "__main__":
    env = os.environ.get
    p = argparse.ArgumentParser(description="Reap stalled / timed-out workflow runs.")
    p.add_argument("-i", "--interval", type=int, default=int(env("WORKFLOW_REAPER_INTERVAL", DEFAULT_INTERVAL)))
    p.add_argument("-s", "--stall-seconds", type=int,
                   default=int(env("WORKFLOW_STALL_SECONDS", DEFAULT_STALL_SECONDS)))
    p.add_argument("-t", "--timeout-hours", type=float,
                   default=float(env("WORKFLOW_RUN_TIMEOUT_HOURS", DEFAULT_RUN_TIMEOUT_HOURS)))
    p.add_argument("--once", action="store_true", help="run one sweep and exit")
    a = p.parse_args()
    daemon(a.interval, ReaperConfig(
        status_index=env("WORKFLOW_STATUS_INDEX", "workflow_status"),
        specs_index=env("WORKFLOW_SPECS_INDEX", "workflow_specs"),
        stall_seconds=a.stall_seconds, timeout_hours=a.timeout_hours,
        revoke_job_type=env("WORKFLOW_REVOKE_JOB_TYPE") or None), once=a.once)
