"""workflow_reaper: pure decision/plan tests and sweep_once against a fake OpenSearch."""
import copy
import json
import sys
import unittest.mock as umock
from datetime import datetime, timedelta, timezone

import pytest

# hysds.celery searches for configuration on import (same convention as test_log_utils.py)
sys.modules.setdefault("hysds.celery", umock.MagicMock())

from hysds import workflow_reaper as wr  # noqa: E402

NOW = datetime(2026, 11, 11, 12, 0, 0, tzinfo=timezone.utc)
STALL = 600


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def ago(**kw):
    return iso(NOW - timedelta(**kw))


def run_doc(run_id="r1", status="running", submitted=None, updated=None, nodes=None, spec_id="s:1"):
    return {"run_id": run_id, "spec_id": spec_id, "status": status, "error": None,
            "submitted_at": submitted or ago(hours=1), "updated_at": updated or ago(seconds=5),
            "nodes": nodes or {"A": {"state": "running"}}}


def job(status, step="A", jid="j1", ci=None, n=None, ts="2026-11-11T11:00:00.000Z", **extra):
    d = {"job_id": jid, "uuid": jid, "payload_id": "p-" + jid, "status": status, "@timestamp": ts,
         "workflow": {"run_id": "r1", "step_id": step, "spec_id": "s:1", "child_index": ci, "child_total": n}}
    d.update(extra)
    return d


# ---------------------------------------------------------------- classify_run
@pytest.mark.parametrize("run,last,expect", [
    (run_doc(), None, wr.NONE),  # healthy
    (run_doc(submitted=ago(hours=49)), None, wr.TIMEOUT),
    (run_doc(submitted=ago(hours=47)), None, wr.NONE),
    (run_doc(submitted=ago(hours=49), updated=ago(hours=30)), None, wr.TIMEOUT),  # timeout wins
    (run_doc(updated=ago(seconds=601)), None, wr.STALLED),
    (run_doc(updated=ago(seconds=599)), None, wr.NONE),
    (run_doc(updated=ago(seconds=900)), NOW - timedelta(seconds=100), wr.NONE),  # rate limited
    (run_doc(updated=ago(seconds=900)), NOW - timedelta(seconds=601), wr.STALLED),
    (run_doc(status="cancelled", updated=ago(hours=2)), None, wr.SWEEP),
    (run_doc(status="failed", updated=ago(hours=23)), None, wr.SWEEP),
    (run_doc(status="failed", updated=ago(hours=25)), None, wr.NONE),
    (run_doc(status="completed", updated=ago(hours=1)), None, wr.NONE),
])
def test_classify(run, last, expect):
    assert wr.classify_run(run, NOW, 48, STALL, last) == expect


def test_timeout_hours_from_spec():
    assert wr.run_timeout_hours({"defaults": {"timeout_hours": 2}}, 48) == 2
    assert wr.run_timeout_hours({"defaults": {}}, 48) == 48
    assert wr.run_timeout_hours(None, 48) == 48
    assert wr.run_timeout_hours({"defaults": {"timeout_hours": "x"}}, 48) == 48
    assert wr.run_timeout_hours({"defaults": {"timeout_hours": 0}}, 48) == 48


# ---------------------------------------------------------------- plan_replay
def test_plan_linear_latest_terminal():
    run = run_doc(nodes={"A": {"state": "completed"}, "B": {"state": "running"}})
    jobs = {"A": [job("job-completed", "A")],  # node A already terminal: ignored
            "B": [job("job-failed", "B", "old", ts="2026-11-11T10:00:00.000Z"),
                  job("job-completed", "B", "new", ts="2026-11-11T11:00:00.000Z")]}
    plan = wr.plan_replay(run, jobs)
    assert [j["job_id"] for j in plan] == ["new"]


def test_plan_nothing_when_a_job_non_terminal():
    run = run_doc(nodes={"B": {"state": "running"}})
    assert wr.plan_replay(run, {"B": [job("job-completed", "B", "a"), job("job-started", "B", "b")]}) == []
    assert wr.plan_replay(run, {"B": [job("job-queued", "B")]}) == []
    assert wr.plan_replay(run, {}) == []


def test_plan_scatter_children_one_event_each():
    run = run_doc(nodes={"S": {"state": "completed"}, "C": {"state": "running", "expected": 3}})
    jobs = {"C": [job("job-completed", "C", f"c{i}", ci=i, n=3) for i in (2, 0)]
            + [job("job-failed", "C", "c1", ci=1, n=3)]}
    plan = wr.plan_replay(run, jobs)
    assert [j["workflow"]["child_index"] for j in plan] == [0, 1, 2]


def test_plan_scatter_blocked_by_running_child():
    run = run_doc(nodes={"C": {"state": "running"}})
    jobs = {"C": [job("job-completed", "C", "c0", ci=0, n=2), job("job-started", "C", "c1", ci=1, n=2)]}
    assert wr.plan_replay(run, jobs) == []


def test_replay_event_shape_matches_publish_workflow_event():
    doc = job("job-failed", "C", "c1", ci=1, n=3, short_error="boom", error_class="exit_code:2",
              job={"retry_count": 2, "job_info": {"workflow_outputs": [{"name": "x", "value_d": 1}]}})
    ev = wr.replay_event(doc, NOW)
    assert ev["replayed"] is True and ev["child_index"] == 1 and ev["child_total"] == 3
    assert ev["error_class"] == "exit_code:2" and ev["retry_count"] == 2
    assert ev["job_id"] == "c1" and ev["status"] == "job-failed"
    # keep the replay in lockstep with log_utils.publish_workflow_event
    from hysds import log_utils
    producer = umock.MagicMock()
    with umock.patch.object(log_utils, "get_workflow_queue_producer", return_value=producer), \
            umock.patch.object(log_utils.app.conf, "WORKFLOW_QUEUE", "workflow_queue", create=True):
        # __wrapped__: skip the backoff decorator (its params are mocked out with hysds.celery)
        log_utils.publish_workflow_event.__wrapped__(dict(doc, job=doc["job"]))
    published = producer.publish.call_args.args[0]
    assert set(ev) - {"replayed"} == set(published)
    for k in published:
        if k != "timestamp":
            assert ev[k] == published[k], k


# ---------------------------------------------------------------- sweep_once
class FakeES:
    def __init__(self, runs, jobs=None, specs=None):
        self.docs = {("workflow_status", r["run_id"]): (copy.deepcopy(r), i + 1) for i, r in enumerate(runs)}
        self.jobs = jobs or {}   # (run, step) -> [docs]
        self.nonterm = {}        # run -> [payload ids]
        self.specs = specs or {}
        self.status_searches = 0

    def search(self, index, body):
        if index == "workflow_status":
            self.status_searches += 1
            return {"hits": {"hits": [{"_source": copy.deepcopy(d), "sort": [0, d["run_id"]]}
                                      for (i, _), (d, _) in self.docs.items()
                                      if d["status"] in ("running", "failed", "cancelled")]}}
        if index == "job_status-current":
            f = body["query"]["bool"]["filter"]
            run = f[0]["term"]["workflow.run_id"]
            if len(f) == 2:
                return {"hits": {"hits": [{"_source": d, "sort": [1, "u"]}
                                          for d in self.jobs.get((run, f[1]["term"]["workflow.step_id"]), [])]}}
            return {"hits": {"hits": [{"_id": p, "_source": {}, "sort": [1, p]}
                                      for p in self.nonterm.get(run, [])]}}
        raise AssertionError(index)

    def get(self, index, id, ignore=None):
        if index == "workflow_specs":
            return ({"found": True, "_source": self.specs[id]} if id in self.specs
                    else {"found": False, "status": 404, "error": {}})
        if (index, id) not in self.docs:
            return {"found": False, "status": 404, "error": {}}
        s, q = self.docs[(index, id)]
        return {"found": True, "_source": copy.deepcopy(s), "_seq_no": q, "_primary_term": 1}

    def index(self, index, id, body, if_seq_no=None, if_primary_term=None, ignore=None):
        self.docs[(index, id)] = (copy.deepcopy(body), self.docs[(index, id)][1] + 1)
        return {"result": "updated"}


def cfg(**kw):
    return wr.ReaperConfig(stall_seconds=STALL, timeout_hours=48,
                           revoke_job_type="job-lw-mozart-revoke:test", **kw)


def sweep(es, state=None, now=NOW):
    submit, publish = umock.MagicMock(return_value="x"), umock.MagicMock()
    s = wr.sweep_once(es, cfg(), submit, publish, state, now)
    return s, submit, publish


def test_healthy_run_is_noop():
    es = FakeES([run_doc()])
    s, submit, publish = sweep(es)
    assert s["runs"] == 1 and not s["timeout"] and not s["stalled"] and not s["sweep"]
    submit.assert_not_called(); publish.assert_not_called()
    assert es.docs[("workflow_status", "r1")][0]["status"] == "running"


def test_timeout_fails_run_and_revokes():
    es = FakeES([run_doc(submitted=ago(hours=49), nodes={"A": {"state": "completed"}, "B": {"state": "running"}})])
    es.nonterm["r1"] = ["p1", "p2"]
    s, submit, publish = sweep(es)
    doc = es.docs[("workflow_status", "r1")][0]
    assert doc["status"] == "failed" and doc["error"] == "run timed out after 48h"
    assert doc["nodes"]["A"]["state"] == "completed" and doc["nodes"]["B"]["state"] == "cancelled"
    assert s["timeout"] == ["r1"] and s["revoked"] == 2
    kw = submit.call_args.kwargs
    assert kw["job_type"] == "job-lw-mozart-revoke:test" and kw["queue"] == "system-jobs-queue"
    assert kw["params"]["operation"] == "revoke"
    assert json.loads(kw["params"]["query"])["query"]["ids"]["values"] == ["p1", "p2"]
    publish.assert_not_called()


def test_timeout_uses_spec_hours():
    es = FakeES([run_doc(submitted=ago(hours=3))], specs={"s:1": {"defaults": {"timeout_hours": 2}}})
    s, _, _ = sweep(es)
    assert es.docs[("workflow_status", "r1")][0]["error"] == "run timed out after 2h"


def test_stall_replays_linear_step_and_rate_limits():
    es = FakeES([run_doc(updated=ago(minutes=30), nodes={"A": {"state": "completed"}, "B": {"state": "running"}})],
                jobs={("r1", "B"): [job("job-completed", "B", "jb")]})
    state = {}
    s, submit, publish = sweep(es, state)
    assert s["stalled"] == ["r1"] and s["replayed"] == 1
    ev = publish.call_args.args[0]
    assert ev["replayed"] is True and ev["run_id"] == "r1" and ev["step_id"] == "B" and ev["job_id"] == "jb"
    submit.assert_not_called()
    assert es.docs[("workflow_status", "r1")][0]["nodes"]["B"]["state"] == "running"  # reaper wrote nothing
    # rate limit: same state, 1 minute later -> nothing; after the window -> again
    s2, _, publish2 = sweep(es, state, NOW + timedelta(seconds=60))
    assert s2["replayed"] == 0 and not publish2.called
    s3, _, publish3 = sweep(es, state, NOW + timedelta(seconds=STALL + 1))
    assert s3["replayed"] == 1


def test_stall_replays_each_scatter_child():
    es = FakeES([run_doc(updated=ago(minutes=30), nodes={"C": {"state": "running", "expected": 3}})],
                jobs={("r1", "C"): [job("job-completed", "C", f"c{i}", ci=i, n=3) for i in range(3)]})
    s, _, publish = sweep(es)
    assert s["replayed"] == 3
    assert [c.args[0]["child_index"] for c in publish.call_args_list] == [0, 1, 2]
    assert all(c.args[0]["replayed"] and c.args[0]["child_total"] == 3 for c in publish.call_args_list)


def test_stall_nothing_replayed_with_nonterminal_job():
    es = FakeES([run_doc(updated=ago(minutes=30), nodes={"C": {"state": "running"}})],
                jobs={("r1", "C"): [job("job-completed", "C", "c0", ci=0, n=2),
                                    job("job-started", "C", "c1", ci=1, n=2)]})
    state = {}
    s, _, publish = sweep(es, state)
    assert s["replayed"] == 0 and not publish.called and "r1" not in state["replays"]


@pytest.mark.parametrize("st", ["cancelled", "failed"])
def test_terminal_sweep_revokes_leftovers(st):
    es = FakeES([run_doc(status=st, updated=ago(hours=1))])
    es.nonterm["r1"] = ["late"]
    state = {}
    s, submit, _ = sweep(es, state)
    assert s["sweep"] == ["r1"] and s["revoked"] == 1
    assert json.loads(submit.call_args.kwargs["params"]["query"])["query"]["ids"]["values"] == ["late"]
    assert es.docs[("workflow_status", "r1")][0]["status"] == st  # untouched
    s2, submit2, _ = sweep(es, state, NOW + timedelta(seconds=30))  # swept recently -> skipped
    assert not s2["sweep"] and not submit2.called


def test_terminal_sweep_nothing_leftover_submits_nothing():
    es = FakeES([run_doc(status="cancelled", updated=ago(hours=1))])
    s, submit, _ = sweep(es)
    assert s["revoked"] == 0 and not s["sweep"] and not submit.called


def test_old_terminal_run_ignored():
    es = FakeES([run_doc(status="failed", updated=ago(hours=30))])
    es.nonterm["r1"] = ["late"]
    s, submit, _ = sweep(es)
    assert not submit.called


def test_one_bad_run_does_not_stop_sweep():
    es = FakeES([run_doc("r1", submitted=ago(hours=49)), run_doc("r2", submitted=ago(hours=49))])
    es.nonterm = {"r1": ["a"], "r2": ["b"]}
    submit = umock.MagicMock(side_effect=[RuntimeError("down"), "x"])
    s = wr.sweep_once(es, cfg(), submit, umock.MagicMock(), {}, NOW)
    assert s["runs"] == 2 and submit.call_count == 2
    assert {es.docs[("workflow_status", r)][0]["status"] for r in ("r1", "r2")} == {"failed"}
