import logging

import fakeredis
import msgpack
import pytest

from hysds import workflow_snapshot as ws


@pytest.fixture(autouse=True)
def _reset():
    ws._last_alert = 0.0


@pytest.fixture
def r():
    return fakeredis.FakeStrictRedis()


def _pop_all(r, key="logstash"):
    return [msgpack.unpackb(x, raw=False) for x in r.lrange(key, 0, -1)]


def test_push_increments_version_and_format(r):
    doc = {"run_id": "r1", "status": "running", "created_at": "2026-11-10T01:02:03Z"}
    assert ws.push_run_snapshot(r, doc, list_key="logstash") == 1
    assert ws.push_run_snapshot(r, doc, list_key="logstash") == 2
    a, b = _pop_all(r)
    assert (a["version"], b["version"]) == (1, 2)
    assert a["resource"] == "workflow_status" and a["run_id"] == "r1"
    assert a["index"] == "workflow_status-2026.11.10"
    assert a["@version"] == "1" and a["@timestamp"].endswith("Z")
    assert "version" not in doc  # caller's dict untouched
    assert int(r.get("wf:ver:r1")) == 2


def test_versions_independent_per_run(r):
    ws.push_run_snapshot(r, {"run_id": "a"}, list_key="logstash")
    assert ws.push_run_snapshot(r, {"run_id": "b"}, list_key="logstash") == 1


def test_index_falls_back_to_today(r):
    ws.push_run_snapshot(r, {"run_id": "x"}, list_key="logstash")
    assert _pop_all(r)[0]["index"].startswith("workflow_status-")


def test_guard_logs_once_per_interval_but_never_drops(r, caplog):
    for i in range(5):
        r.rpush("logstash", b"x")
    with caplog.at_level(logging.ERROR, logger="hysds.workflow_snapshot"):
        for _ in range(3):
            ws.push_run_snapshot(r, {"run_id": "g"}, list_key="logstash", max_len=3)
    assert len([x for x in caplog.records if x.levelno == logging.ERROR]) == 1
    assert r.llen("logstash") == 5 + 3


def test_guard_not_triggered_below_threshold(r, caplog):
    with caplog.at_level(logging.ERROR, logger="hysds.workflow_snapshot"):
        ws.push_run_snapshot(r, {"run_id": "g"}, list_key="logstash", max_len=10)
    assert not caplog.records


def test_guard_realerts_after_interval(r, caplog, monkeypatch):
    for i in range(5):
        r.rpush("logstash", b"x")
    t = [1000.0]
    monkeypatch.setattr(ws.time, "monotonic", lambda: t[0])
    with caplog.at_level(logging.ERROR, logger="hysds.workflow_snapshot"):
        ws.push_run_snapshot(r, {"run_id": "g"}, list_key="logstash", max_len=1)
        t[0] += ws.ALERT_INTERVAL_SEC + 1
        ws.push_run_snapshot(r, {"run_id": "g"}, list_key="logstash", max_len=1)
    assert len(caplog.records) == 2
