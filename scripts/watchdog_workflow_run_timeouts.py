#!/usr/bin/env python
"""Workflow reaper process (v2): run timeouts and stuck-run recovery from Redis. Kept at this path so the
supervisord program is unchanged; all logic lives in ``hysds.workflow_reaper``.

Settings (celeryconfig): WORKFLOW_REAPER_INTERVAL, WORKFLOW_STALL_SECONDS, WORKFLOW_RUN_TIMEOUT_HOURS,
WORKFLOW_REAPER_QUEUED_SECONDS, WORKFLOW_REAPER_VANISHED_SECONDS, WORKFLOW_REAPER_EVENT_GRACE_SECONDS,
WORKFLOW_REAPER_LOCK_TTL.
"""
import sys

from hysds.workflow_reaper import main

if __name__ == "__main__":
    main(once="--once" in sys.argv)
