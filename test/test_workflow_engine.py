"""workflow v2 engine: decisions and Redis state, against fakeredis (Lua via lupa). No OpenSearch, no broker."""
import json

import fakeredis
import msgpack
import pytest

from hysds import workflow_engine as we
from hysds import workflow_state as ws
from hysds_commons.workflow_run2 import freeze_job_definitions, new_run_doc, validate_run_doc
from hysds_commons.workflow_spec2 import validate_spec2

JT = "job-t:v1"


def _step(deps=(), ins=None, out=("o",), **kw):
    s = {"job_type": JT, "deps": sorted(deps), "in": ins or {}, "out": list(out)}
    s.update(kw)
    return s


def _src(*refs, **kw):
    return dict({"source": list(refs)}, **kw)


def make_spec(steps, outputs=None, inputs=None, **kw):
    spec = {"schema_version": "2.0", "id": "t", "version": "1", "on_error": "kill",
            "inputs": inputs or {"x": {"type": "string", "default": "d"}},
            "steps": steps, "outputs": outputs or {}}
    spec.update(kw)
    assert validate_spec2(spec) == [], validate_spec2(spec)
    return spec


FROZEN = {"job_spec": {"params": []}, "hysds_io": {"params": []}, "container": {"id": "c"}, "queue": "q1"}


class Harness:
    """Engine + recorded submissions + an in-memory event queue."""

    def __init__(self, spec, inputs=None, fail_submit=0):
        self.r = fakeredis.FakeStrictRedis()
        self.jobs, self.queue, self.revoked = [], [], []
        self.fail_submit = fail_submit
        self.n = 0

        def new_id():
            self.n += 1
            return "p%d" % self.n

        def build(definition, jt, queue, params, **kw):
            return {"jt": jt, "queue": queue, "params": {k: v for k, v in params.items() if v is not None}, **kw}

        def submit(job, pid):
            if self.fail_submit:
                self.fail_submit -= 1
                raise RuntimeError("broker down")
            self.jobs.append((pid, job))
            return pid

        self.eng = Engine = we.Engine(self.r, submit, publish_fn=self.queue.append,
                                      revoke_fn=lambda run, ids, u: self.revoked.append((run, list(ids), u)),
                                      new_id=new_id, build_job_fn=build, list_key="logstash")
        self.spec = spec
        defs = freeze_job_definitions(spec, lambda jt: FROZEN)
        resolved = {k: (v.get("default")) for k, v in spec["inputs"].items()}
        resolved.update(inputs or {})
        self.run = new_run_doc(spec, "r1", resolved, defs)
        ws.init_run(self.r, spec, self.run, list_key="logstash")

    def start(self):
        self.send({"kind": "run_started", "run_id": "r1"})

    def send(self, ev):
        return self.eng.handle_event(ev)

    def pump(self):
        while self.queue:
            self.send(self.queue.pop(0))

    def doc(self):
        return ws.get_run(self.r, "r1")

    def job_of(self, step):
        return [(pid, j) for pid, j in self.jobs if j["workflow"]["step_id"] == step][-1]

    def finish(self, step, outputs=None, status="job-completed", **kw):
        pid, _ = self.job_of(step)
        ev = {"run_id": "r1", "step_id": step, "status": status, "job_id": pid, "payload_id": pid,
              "workflow_outputs": [{"name": k, "value_s": v} if isinstance(v, str) else {"name": k, "value_b": v}
                                   for k, v in (outputs or {}).items()], "error_class": None}
        ev.update(kw)
        self.send(ev)
        self.pump()
        return ev

    def snapshots(self):
        return [msgpack.unpackb(x, raw=False) for x in self.r.lrange("logstash", 0, -1)]


LINEAR = {
    "a": _step(ins={"v": _src("$.inputs.x")}, out=("o",)),
    "b": _step(deps=["a"], ins={"v": _src("$.a.o")}),
    "c": _step(deps=["b"], ins={"v": _src("$.b.o")}),
}


def test_linear_chain_completes_with_outputs():
    spec = make_spec(LINEAR, outputs={"res": {"type": "string", "source": ["$.c.o"]}})
    h = Harness(spec)
    h.start()
    assert [j["workflow"]["step_id"] for _, j in h.jobs] == ["a"]
    assert h.jobs[0][1]["params"] == {"v": "d"} and h.jobs[0][1]["queue"] == "q1"
    assert h.doc()["steps"]["a"]["state"] == "queued"
    h.finish("a", {"o": "A"})
    assert h.job_of("b")[1]["params"] == {"v": "A"}
    h.finish("b", {"o": "B"})
    h.finish("c", {"o": "C"})
    d = h.doc()
    assert d["status"] == "completed" and d["outputs"] == {"res": "C"}
    assert validate_run_doc(d) == []
    assert h.r.zrange(ws.ACTIVE_KEY, 0, -1) == []  # left the registry
    assert h.r.ttl(ws.k_run("r1")) > 0 and h.r.ttl(ws.k_out("r1")) > 0


def test_snapshot_after_every_change_with_monotonic_version_and_index():
    h = Harness(make_spec(LINEAR))
    h.run["index"] = "workflow_status-2026.01.02"
    h.start()
    h.finish("a", {"o": "A"})
    snaps = h.snapshots()
    vs = [s["version"] for s in snaps]
    assert vs == sorted(vs) and len(set(vs)) == len(vs) and len(vs) >= 3
    assert all(s["resource"] == "workflow_status" and s["run_id"] == "r1" for s in snaps)
    assert int(h.r.get("wf:ver:r1")) == vs[-1] == h.doc()["version"]


def test_started_event_marks_running():
    h = Harness(make_spec(LINEAR))
    h.start()
    pid, _ = h.job_of("a")
    h.send({"run_id": "r1", "step_id": "a", "status": "job-started", "job_id": pid, "payload_id": pid})
    assert h.doc()["steps"]["a"]["state"] == "running"


PARALLEL = {
    "a": _step(ins={"v": _src("$.inputs.x")}),
    "p": _step(deps=["a"], ins={"v": _src("$.a.o")}),
    "q": _step(deps=["a"], ins={"v": _src("$.a.o")}),
    "j": _step(deps=["p", "q"], ins={"l": _src("$.p.o", "$.q.o", link_merge="merge_flattened")}),
    "r0": _step(ins={"v": _src("$.inputs.x")}),  # second root: starts in parallel with a
}


def test_parallel_branches_and_join_releases_once():
    h = Harness(make_spec(PARALLEL))
    h.start()
    assert sorted(j["workflow"]["step_id"] for _, j in h.jobs) == ["a", "r0"]
    h.finish("a", {"o": "A"})
    assert sorted(j["workflow"]["step_id"] for _, j in h.jobs) == ["a", "p", "q", "r0"]
    h.finish("p", {"o": "P"})
    assert "j" not in [j["workflow"]["step_id"] for _, j in h.jobs]  # q not settled yet
    h.finish("q", {"o": "Q"})
    assert [j["workflow"]["step_id"] for _, j in h.jobs].count("j") == 1
    assert h.job_of("j")[1]["params"] == {"l": ["P", "Q"]}
    h.finish("j", {"o": "J"})
    assert h.doc()["status"] == "running"  # r0 still unsettled
    h.finish("r0", {"o": "R"})
    assert h.doc()["status"] == "completed"


def test_join_lua_exactly_once_and_replay():
    spec = make_spec(PARALLEL)
    r = fakeredis.FakeStrictRedis()
    assert ws.join(r, "r1", spec, "p", ["j"]) == []
    assert ws.join(r, "r1", spec, "p", ["j"]) == []  # duplicate: still one of two deps
    assert ws.join(r, "r1", spec, "q", ["j"]) == ["j"]
    assert ws.join(r, "r1", spec, "q", ["j"]) == []  # already released
    assert ws.join(r, "r1", spec, "q", ["j"], replay=True) == ["j"]  # replay heals a lost release


SKIP = {
    "a": _step(out=("flag", "o")),
    "m": _step(deps=["a"], ins={"v": _src("$.a.o")}, when="$.a.flag"),
    "n": _step(deps=["a"], ins={"v": _src("$.a.o")}),
    "k": _step(deps=["m", "n"], ins={"v": _src("$.m.o", "$.n.o", pick="first_non_null")}),
}


def test_when_false_skips_and_rejoin_with_first_non_null():
    h = Harness(make_spec(SKIP, outputs={"res": {"type": "string", "source": ["$.k.o"]}}))
    h.start()
    h.finish("a", {"o": "A", "flag": False})
    d = h.doc()
    assert d["steps"]["m"]["state"] == "skipped" and d["steps"]["m"]["skipped_by"] == {"flag": "$.a.flag", "value": False}
    assert "m" not in [j["workflow"]["step_id"] for _, j in h.jobs]
    h.finish("n", {"o": "N"})
    assert h.job_of("k")[1]["params"] == {"v": "N"}  # no cascade: k still ran
    h.finish("k", {"o": "K"})
    assert h.doc()["status"] == "completed" and h.doc()["outputs"] == {"res": "K"}


def test_when_true_runs():
    h = Harness(make_spec(SKIP))
    h.start()
    h.finish("a", {"o": "A", "flag": True})
    assert "m" in [j["workflow"]["step_id"] for _, j in h.jobs]


def test_input_resolution_error_fails_run_with_class():
    # b pulls $.a.missing, which the job never wrote
    spec = make_spec({"a": _step(), "b": _step(deps=["a"], ins={"v": _src("$.a.o")})})
    h = Harness(spec)
    h.start()
    h.finish("a", {"other": "x"})
    d = h.doc()
    assert d["status"] == "failed" and d["error_class"] == "input_resolution" and d["failed_step"] == "b"
    assert d["steps"]["b"]["state"] == "failed" and validate_run_doc(d) == []


def test_when_not_boolean_is_input_resolution():
    h = Harness(make_spec(SKIP))
    h.start()
    h.finish("a", {"o": "A", "flag": "yes"})
    assert h.doc()["error_class"] == "input_resolution" and h.doc()["steps"]["m"]["state"] == "failed"


def test_null_input_all_null_first_non_null_fails():
    spec = make_spec(SKIP)
    h = Harness(spec)
    h.start()
    h.finish("a", {"o": "A", "flag": False})
    # n completes without output "o" => k's first_non_null sees a missing output
    h.finish("n", {"zzz": "1"})
    assert h.doc()["error_class"] == "input_resolution" and h.doc()["failed_step"] == "k"


def test_failure_kills_run_and_revokes_other_jobs():
    h = Harness(make_spec(PARALLEL))
    h.start()
    h.finish("a", {"o": "A"})
    h.finish("p", status="job-failed", error_class="worker", short_error="boom")
    d = h.doc()
    assert d["status"] == "failed" and d["error_class"] == "worker" and d["failed_step"] == "p"
    assert d["steps"]["p"]["state"] == "failed"
    assert d["steps"]["q"]["state"] == "cancelled" and d["steps"]["q"]["cancelled_by"]["kind"] == "failure"
    assert d["steps"]["j"]["state"] == "cancelled" and d["steps"]["r0"]["state"] == "cancelled"
    assert sorted(h.revoked[0][1]) == sorted([h.job_of("q")[0], h.job_of("r0")[0]])
    assert validate_run_doc(d) == []
    assert ws.n_done(h.r, "r1") == 5
    # late events of the dead run are ignored
    h.finish("q", {"o": "Q"})
    assert h.doc()["status"] == "failed" and "j" not in [j["workflow"]["step_id"] for _, j in h.jobs]


def test_retry_then_success_and_then_exhausted():
    spec = make_spec({"a": _step(max_retries=1, retry_on=["worker"])})
    h = Harness(spec)
    h.start()
    h.finish("a", status="job-failed", error_class="worker", short_error="x")
    d = h.doc()
    assert d["status"] == "running" and len(d["steps"]["a"]["attempts"]) == 2
    assert d["steps"]["a"]["attempts"][0]["status"] == "failed" and len(h.jobs) == 2
    assert "retry_count" not in h.jobs[0][1] and h.jobs[1][1]["retry_count"] == 1
    h.finish("a", status="job-failed", error_class="worker", short_error="x")  # retries used up
    assert h.doc()["status"] == "failed" and len(h.jobs) == 2

    h = Harness(spec)
    h.start()
    h.finish("a", status="job-failed", error_class="worker")
    h.finish("a", {"o": "ok"})
    assert h.doc()["status"] == "completed" and len(h.doc()["steps"]["a"]["attempts"]) == 2


def test_non_matching_error_class_is_not_retried():
    h = Harness(make_spec({"a": _step(max_retries=3, retry_on=["exit_code:75"])}))
    h.start()
    h.finish("a", status="job-failed", error_class="worker")
    assert h.doc()["status"] == "failed" and len(h.jobs) == 1


def test_idempotent_replay_of_completion_and_failure():
    h = Harness(make_spec(LINEAR))
    h.start()
    ev = h.finish("a", {"o": "A"})
    v = h.doc()["version"]
    n = len(h.jobs)
    for _ in range(3):
        h.send(dict(ev))
        h.pump()
    assert len(h.jobs) == n == 2  # b submitted once
    assert h.doc()["version"] == v  # no state change at all
    # duplicate start event is harmless as well
    h.send({"kind": "run_started", "run_id": "r1"})
    assert len(h.jobs) == 2


def test_replayed_settle_heals_lost_release():
    """crash after the settle committed but before b was queued: the replayed event releases b."""
    h = Harness(make_spec(LINEAR), fail_submit=0)
    h.start()
    pid, _ = h.job_of("a")
    h.eng.submit_fn, real = (lambda job, pid: (_ for _ in ()).throw(RuntimeError("down"))), h.eng.submit_fn
    ev = {"run_id": "r1", "step_id": "a", "status": "job-completed", "job_id": pid, "payload_id": pid,
          "workflow_outputs": [{"name": "o", "value_s": "A"}]}
    with pytest.raises(RuntimeError):
        h.send(dict(ev))
    d = h.doc()
    assert d["steps"]["a"]["state"] == "completed" and d["steps"]["b"]["state"] == "waiting"  # attempt undone
    h.eng.submit_fn = real
    h.send(dict(ev))  # the broker redelivers
    assert h.doc()["steps"]["b"]["state"] == "queued" and len(h.jobs) == 2


def test_concurrent_duplicate_completion_submits_once():
    """two consumers handle the same event: the CAS on the run doc lets only one release b."""
    h = Harness(make_spec(LINEAR))
    h.start()
    pid, _ = h.job_of("a")
    ev = {"run_id": "r1", "step_id": "a", "status": "job-completed", "job_id": pid, "payload_id": pid,
          "workflow_outputs": [{"name": "o", "value_s": "A"}]}
    orig = h.eng._after_settle
    calls = []

    def racing(run_id, sid, replay=False):
        calls.append(replay)
        if len(calls) == 1:  # while consumer 1 is between settle and release, consumer 2 runs fully
            h.eng._after_settle = orig
            h.send(dict(ev))
        return orig(run_id, sid, replay)
    h.eng._after_settle = racing
    h.send(dict(ev))
    assert [j["workflow"]["step_id"] for _, j in h.jobs].count("b") == 1


def test_stale_and_foreign_events_ignored():
    h = Harness(make_spec(LINEAR))
    h.start()
    note = h.send({"run_id": "r1", "step_id": "a", "status": "job-completed", "job_id": "nope", "payload_id": "nope"})
    assert "matches no attempt" in note and h.doc()["steps"]["a"]["state"] == "queued"
    with pytest.raises(we.EventRejected):
        h.send({"run_id": "r1", "step_id": "zzz", "status": "job-completed", "job_id": "x"})
    with pytest.raises(we.EventRejected):
        h.send({"run_id": "nope", "step_id": "a", "status": "job-completed", "job_id": "x"})


def test_bad_workflow_outputs_fail_step():
    h = Harness(make_spec(LINEAR))
    h.start()
    h.finish("a", workflow_outputs=[{"name": "o"}])
    assert h.doc()["status"] == "failed" and h.doc()["error_class"] == "outputs"


def test_submit_failure_requeues_without_losing_the_step():
    h = Harness(make_spec(LINEAR), fail_submit=1)
    with pytest.raises(RuntimeError):
        h.start()
    assert h.doc()["steps"]["a"]["state"] == "waiting" and h.doc()["steps"]["a"]["attempts"] == []
    h.start()  # redelivery
    assert h.doc()["steps"]["a"]["state"] == "queued" and len(h.jobs) == 1


def test_cancel_kills_and_marks_cancelled_idempotently():
    h = Harness(make_spec(PARALLEL))
    h.start()
    h.send({"kind": "cancel_requested", "run_id": "r1", "username": "alice"})
    d = h.doc()
    assert d["status"] == "cancelled" and d["cancelled_by"] == "alice" and validate_run_doc(d) == []
    assert all(st["state"] == "cancelled" and st["cancelled_by"]["kind"] == "user" for st in d["steps"].values())
    assert sorted(h.revoked[0][1]) == sorted(p for p, _ in h.jobs) and h.revoked[0][2] == "alice"
    v = d["version"]
    h.send({"kind": "cancel_requested", "run_id": "r1", "username": "alice"})
    assert h.doc()["version"] == v and len(h.revoked) == 1
    h.finish("a", {"o": "A"})  # late event
    assert h.doc()["status"] == "cancelled"


def test_scatter_with_a_non_list_input_fails_as_input_resolution():
    steps = {"a": _step(ins={"v": _src("$.inputs.x")}, scatter={"input": "v"})}
    h = Harness(make_spec(steps))
    h.start()
    d = h.doc()
    assert d["status"] == "failed" and d["error_class"] == "input_resolution"
    assert "must be a list" in d["steps"]["a"]["error"]


def test_halt_failed_fires_and_fails_run():
    steps = {"a": _step(out=("bad",)),
             "h": {"halt": {"status": "failed", "reason": "qa"}, "deps": ["a"], "when": "$.a.bad"}}
    h = Harness(make_spec(steps))
    h.start()
    h.finish("a", {"bad": True})
    d = h.doc()
    assert d["status"] == "failed" and d["error_class"] == "halted" and d["reason"] == "qa"
    assert d["steps"]["h"]["halt"]["fired"] is True and validate_run_doc(d) == []


def test_finalize_output_type_check():
    spec = make_spec(LINEAR, outputs={"res": {"type": "string", "source": ["$.c.o"]}})
    # c writes no 'o' -> pulling the output fails => output_resolution
    h = Harness(spec)
    h.start()
    h.finish("a", {"o": "A"})
    h.finish("b", {"o": "B"})
    h.finish("c", {"zzz": "x"})
    d = h.doc()
    assert d["status"] == "failed" and d["error_class"] == "output_resolution"


def test_finalize_null_in_required_output():
    steps = {"a": _step(out=("flag", "o")), "m": _step(deps=["a"], when="$.a.flag")}
    spec = make_spec(steps, outputs={"res": {"type": "string?", "source": ["$.m.o"]}})
    h = Harness(spec)
    h.start()
    h.finish("a", {"flag": False})
    assert h.doc()["status"] == "completed" and h.doc()["outputs"] == {"res": None}


def test_on_error_modes_other_than_kill_fall_back_to_kill():
    spec = make_spec({"a": _step()}, on_error="drain")
    h = Harness(spec)
    h.start()
    h.finish("a", status="job-failed", error_class="worker")
    assert h.doc()["status"] == "failed"


def test_registry_and_keys():
    h = Harness(make_spec(LINEAR))
    assert ws.list_active_runs(h.r) == ["r1"] and "r1" in ws.active_heartbeats(h.r)
    assert ws.get_run(h.r, "r1")["run_id"] == "r1" and ws.get_run(h.r, "other") is None
    with pytest.raises(ws.RunExists):
        ws.init_run(h.r, h.spec, h.run, list_key="logstash")
    for k in ws.run_keys("r1", h.spec):
        assert "r1" in k
    assert h.snapshots()[0]["version"] == 1  # init_run pushed the first snapshot


def test_publish_run_event_shapes():
    got = []
    ws.publish_run_event("r9", "cancel_requested", {"username": "u"}, publish_fn=got.append)
    assert got[0]["kind"] == "cancel_requested" and got[0]["run_id"] == "r9" and got[0]["username"] == "u"
    with pytest.raises(ValueError):
        ws.publish_run_event("r9", "bogus", publish_fn=got.append)


def test_two_runs_do_not_interfere():
    h = Harness(make_spec(LINEAR))
    spec = h.spec
    run2 = new_run_doc(spec, "r2", {"x": "e"}, freeze_job_definitions(spec, lambda jt: FROZEN))
    ws.init_run(h.r, spec, run2, list_key="logstash")
    h.start()
    h.send({"kind": "run_started", "run_id": "r2"})
    assert len(h.jobs) == 2 and {j["workflow"]["run_id"] for _, j in h.jobs} == {"r1", "r2"}
    h.send({"kind": "cancel_requested", "run_id": "r2", "username": "u"})
    assert h.doc()["status"] == "running" and ws.get_run(h.r, "r2")["status"] == "cancelled"


# ---- consumer shell
class _Msg:
    def __init__(self, redelivered=False):
        self.delivery_info = {"redelivered": redelivered}
        self.acked = self.requeued = False

    def ack(self):
        self.acked = True

    def requeue(self):
        self.requeued = True


def _shell(handler):
    from hysds.workflow_rules import WorkflowRulesConsumer

    class E:
        handle_event = staticmethod(handler)
    dl = []
    return WorkflowRulesConsumer(None, "q", E, dlq_publish_fn=dl.append, sleep_fn=lambda s: None), dl


def test_shell_validate_event():
    from hysds.workflow_rules import WorkflowRulesConsumer as C
    assert C.validate_event({"kind": "run_started", "run_id": "r"}) is None
    assert C.validate_event({"run_id": "r", "step_id": "a", "status": "job-completed"}) is None
    assert C.validate_event({"run_id": "r"}) and C.validate_event([]) and C.validate_event({"kind": "x"})


def test_shell_requeue_once_then_dead_letter_and_reject():
    def boom(ev):
        raise RuntimeError("x")
    c, dl = _shell(boom)
    ev = {"kind": "run_started", "run_id": "r"}
    m = _Msg()
    c.on_message(ev, m)
    assert m.requeued and not m.acked and not dl
    m = _Msg(redelivered=True)
    c.on_message(ev, m)
    assert m.acked and "poison" in dl[0]["reason"]

    def rej(ev):
        raise we.EventRejected("nope")
    c, dl = _shell(rej)
    m = _Msg()
    c.on_message(ev, m)
    assert m.acked and "rejected" in dl[0]["reason"]
    c, dl = _shell(lambda e: "ok")
    m = _Msg()
    c.on_message({"bad": 1}, m)
    assert m.acked and "malformed" in dl[0]["reason"]


def test_run_keys_expire_after_end_with_the_configured_ttl(monkeypatch):
    from hysds import workflow_snapshot
    h = Harness(make_spec(LINEAR))
    h.start()
    assert all(h.r.ttl(k) > 24 * 3600 for k in ws.run_keys("r1", h.spec) if h.r.exists(k))  # active: safety TTL
    h.finish("a", {"o": "A"})
    h.finish("b", {"o": "B"})
    h.finish("c")
    assert h.doc()["status"] == "completed"
    assert all(0 < h.r.ttl(k) <= 24 * 3600 for k in ws.run_keys("r1", h.spec) if h.r.exists(k))  # default: 1 day
    # the setting overrides it
    real = workflow_snapshot._conf
    monkeypatch.setattr(workflow_snapshot, "_conf", lambda n, d: 600 if n == "WORKFLOW_RUN_TTL_AFTER_END" else real(n, d))
    h2 = Harness(make_spec(LINEAR))
    h2.start()
    for s in "abc":
        h2.finish(s, {"o": s})
    assert all(0 < h2.r.ttl(k) <= 600 for k in ws.run_keys("r1", h2.spec) if h2.r.exists(k))
