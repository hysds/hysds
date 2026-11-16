"""Push full workflow run snapshots to OpenSearch asynchronously via Redis -> Logstash.

While a run is active, Redis is the source of truth. After every change the engine pushes a
complete run document onto the same ``logstash`` Redis list that ``log_utils.log_job_status``
uses; Logstash (``indexer.conf.mozart``, ``resource == "workflow_status"`` branch) indexes it
with ``document_id = run_id`` and ``version_type = external_gte``, so an OpenSearch outage only
delays the record and out-of-order delivery cannot overwrite a newer snapshot.

The payload format matches ``log_job_status``: a msgpack-encoded dict with ``resource``,
``@version`` ("1", the HySDS payload marker, unrelated to the document ``version``) and
``@timestamp``, RPUSHed onto ``REDIS_JOB_STATUS_KEY``.
"""
import logging
import re
import time
from datetime import datetime, timezone

import msgpack

logger = logging.getLogger("hysds.workflow_snapshot")

RESOURCE = "workflow_status"
VERSION_KEY_TMPL = "wf:ver:%s"
DEFAULT_LIST_KEY = "logstash"
DEFAULT_MAX_LEN = 100000
ALERT_INTERVAL_SEC = 300

_last_alert = 0.0


def _conf(name, default):
    try:
        from hysds.celery import app

        return app.conf.get(name, default)
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


def push_run_snapshot(redis_client, run_doc, list_key=None, max_len=None):
    """INCR the run's version counter, stamp it on a copy of ``run_doc`` and RPUSH it.

    Returns the version used. ``run_doc`` must carry ``run_id``. The caller's dict is not
    modified. Redis errors propagate so the caller can retry.
    """
    run_id = run_doc["run_id"]
    list_key = list_key or _conf("REDIS_JOB_STATUS_KEY", DEFAULT_LIST_KEY)
    if max_len is None:
        max_len = _conf("WORKFLOW_LOGSTASH_MAX_LEN", DEFAULT_MAX_LEN)

    version = int(redis_client.incr(VERSION_KEY_TMPL % run_id))
    doc = dict(run_doc)
    doc["version"] = version
    doc["resource"] = RESOURCE
    doc["index"] = daily_index(run_doc)
    doc["@version"] = "1"
    doc["@timestamp"] = datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z"

    _check_list_size(redis_client, list_key, max_len)
    redis_client.rpush(list_key, msgpack.dumps(doc))
    return version
