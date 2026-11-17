"""Push full workflow run snapshots to OpenSearch asynchronously via Redis -> Logstash.

While a run is active, Redis is the source of truth. After every change the engine pushes a
complete run document onto the same ``logstash`` Redis list that ``log_utils.log_job_status``
uses; Logstash (``indexer.conf.mozart``, ``resource == "workflow_status"`` branch) indexes it
with ``document_id = run_id`` and ``version_type = external_gte``, so an OpenSearch outage only
delays the record and out-of-order delivery cannot overwrite a newer snapshot.

The payload format matches ``log_job_status``: a msgpack-encoded dict with ``resource``,
``@version`` ("1", the HySDS payload marker, unrelated to the document ``version``) and
``@timestamp``, RPUSHed onto ``REDIS_JOB_STATUS_KEY``.

``job_definitions`` (the frozen job specs/hysds-io/containers, large, written once by mozart) are in the FIRST
snapshot (version 1, from ``init_run``) and the FINAL one (the run ended) only; every snapshot in between leaves
the field out. Logstash indexes with ``action => index``, which replaces the whole document, so the definitions are
absent from OpenSearch between those two snapshots and the final snapshot restores them. That is acceptable: while a
run is active mozart reads it from Redis (``GET /workflow/runs/<id>``), where the run doc keeps them (the engine
submits jobs from them).
"""
import logging
import re
import time
from datetime import datetime, timezone

import msgpack

logger = logging.getLogger("hysds.workflow_snapshot")

RESOURCE = "workflow_status"
VERSION_KEY_TMPL = "wf:ver:%s"
FINAL_STATUSES = ("completed", "failed", "cancelled")
DEFAULT_LIST_KEY = "logstash"
DEFAULT_MAX_LEN = 100000
ALERT_INTERVAL_SEC = 300

_last_alert = 0.0


def _conf(name, default):
    try:
        from hysds.celery import app

        value = app.conf.get(name, default)
        return value if isinstance(value, type(default)) else default  # (a mocked celery app yields junk)
    except Exception:  # noqa: BLE001  (unit tests / no celeryconfig)
        return default


def daily_index(run_doc, now=None):
    """workflow_status-YYYY.MM.DD from the run's creation date (stable across versions)."""
    for field in ("created_at", "submitted_at"):
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", str(run_doc.get(field) or ""))
        if m:
            return "%s-%s.%s.%s" % ((RESOURCE,) + m.groups())
    now = now or datetime.now(timezone.utc)
    return now.strftime(RESOURCE + "-%Y.%m.%d")


def _check_list_size(redis_client, list_key, max_len):
    """Alert (rate limited) when the list is long; never blocks or drops a push."""
    global _last_alert
    try:
        n = redis_client.llen(list_key)
    except Exception:  # noqa: BLE001
        return
    if n > max_len:
        now = time.monotonic()
        if _last_alert == 0.0 or now - _last_alert >= ALERT_INTERVAL_SEC:
            _last_alert = now
            logger.error(
                "redis list %r has %d entries (> WORKFLOW_LOGSTASH_MAX_LEN=%d): the indexer or "
                "OpenSearch is behind; snapshots are still being queued, none are dropped",
                list_key, n, max_len,
            )


def build_snapshot(run_doc, version=None):
    """msgpack bytes of the snapshot of ``run_doc`` (the exact payload ``push_run_snapshot`` queues).

    ``version`` (default: the doc's own) is stamped on a copy; the caller's dict is not modified.
    Used by the engine to queue the snapshot inside the same Redis transaction as the state change."""
    doc = dict(run_doc)
    if version is not None:
        doc["version"] = version
    if doc.get("version") != 1 and doc.get("status") not in FINAL_STATUSES:
        doc.pop("job_definitions", None)  # first and final snapshots only (see the module docstring)
    doc["resource"] = RESOURCE
    doc["index"] = run_doc.get("index") or daily_index(run_doc)  # mozart stamps it: one daily index per run
    doc["@version"] = "1"
    doc["@timestamp"] = datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z"
    return msgpack.dumps(doc)


def list_key_and_max():
    return _conf("REDIS_JOB_STATUS_KEY", DEFAULT_LIST_KEY), _conf("WORKFLOW_LOGSTASH_MAX_LEN", DEFAULT_MAX_LEN)


def check_list_size(redis_client, list_key=None, max_len=None):
    """Public form of the rate-limited long-list alert (never raises)."""
    k, m = list_key_and_max()
    _check_list_size(redis_client, list_key or k, m if max_len is None else max_len)


def push_run_snapshot(redis_client, run_doc, list_key=None, max_len=None, version=None):
    """Stamp a version on a copy of ``run_doc`` and RPUSH it. Returns the version used.

    Without ``version`` the run's counter is INCRed (``wf:ver:<run_id>``). With ``version`` (the engine,
    which already incremented it atomically with the state change) it is used as is. ``run_doc`` must
    carry ``run_id``. Redis errors propagate so the caller can retry.
    """
    run_id = run_doc["run_id"]
    list_key = list_key or _conf("REDIS_JOB_STATUS_KEY", DEFAULT_LIST_KEY)
    if max_len is None:
        max_len = _conf("WORKFLOW_LOGSTASH_MAX_LEN", DEFAULT_MAX_LEN)
    if version is None:
        version = int(redis_client.incr(VERSION_KEY_TMPL % run_id))
    _check_list_size(redis_client, list_key, max_len)
    redis_client.rpush(list_key, build_snapshot(run_doc, version))
    return version
