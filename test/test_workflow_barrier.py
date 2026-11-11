"""Redis scatter/gather barrier: Lua semantics, concurrency, rebuild."""
import os
import random
import threading
import unittest.mock as umock

import fakeredis
import pytest

from hysds import workflow_barrier as wb

RUN, C = "run-1", "P"
OK, FAIL, OFF = "job-completed", "job-failed", "job-offline"


@pytest.fixture
def r():
    return fakeredis.FakeStrictRedis()


def test_n_completes_exactly_one_winner(r):
    res = [wb.record_child(r, RUN, C, f"j{i}", OK, 5) for i in range(5)]
    assert [x.winner for x in res] == [False] * 4 + [True]
    assert (res[-1].completed, res[-1].failed, res[-1].expected) == (5, 0, 5)
    assert wb.get_barrier(r, RUN, C)["released"] == 1


def test_redelivery_no_double_count(r):
    for _ in range(3):
        x = wb.record_child(r, RUN, C, "j0", OK, 2)
    assert (x.completed, x.failed, x.winner) == (1, 0, False)
    assert wb.record_child(r, RUN, C, "j1", OK, 2).winner
    # redelivery after release is never a second winner
    assert not wb.record_child(r, RUN, C, "j1", OK, 2).winner
    assert not wb.record_child(r, RUN, C, "j0", OK, 2).winner


def test_failed_then_completed_upgrade(r):
    x = wb.record_child(r, RUN, C, "j0", FAIL, 2)
    assert (x.completed, x.failed) == (0, 1)
    x = wb.record_child(r, RUN, C, "j0", OK, 2)
    assert (x.completed, x.failed, x.winner) == (1, 0, False)
    x = wb.record_child(r, RUN, C, "j1", OK, 2)
    assert (x.completed, x.failed, x.winner) == (2, 0, True)


def test_upgrade_after_release_never_rereleases(r):
    wb.record_child(r, RUN, C, "j0", OK, 2)
    assert wb.record_child(r, RUN, C, "j1", FAIL, 2).winner
    x = wb.record_child(r, RUN, C, "j1", OK, 2)  # late upgrade: counts move, no second winner
    assert (x.completed, x.failed, x.winner) == (2, 0, False)


def test_completed_sticky_failed_ignored(r):
    wb.record_child(r, RUN, C, "j0", OK, 3)
    x = wb.record_child(r, RUN, C, "j0", FAIL, 3)
    assert (x.completed, x.failed) == (1, 0)
    x = wb.record_child(r, RUN, C, "j0", OFF, 3)
    assert (x.completed, x.failed) == (1, 0)


def test_failed_vs_offline_same_class_noop(r):
    wb.record_child(r, RUN, C, "j0", FAIL, 3)
    x = wb.record_child(r, RUN, C, "j0", OFF, 3)
    assert (x.completed, x.failed) == (0, 1)


def test_mixed_winner_counts(r):
    sts = [OK, FAIL, OK, OFF, OK]
    res = [wb.record_child(r, RUN, C, f"j{i}", s, 5) for i, s in enumerate(sts)]
    assert [x.winner for x in res] == [False] * 4 + [True]
    assert (res[-1].completed, res[-1].failed, res[-1].expected) == (3, 2, 5)


def test_expected_fixed_by_first_event_and_ttl(r):
    wb.record_child(r, RUN, C, "j0", OK, 4)
    wb.record_child(r, RUN, C, "j1", OK, 9)  # later child_total ignored
    assert wb.get_barrier(r, RUN, C)["expected"] == 4
    bkey, mkey = wb.barrier_keys(RUN, C)
    assert bkey == "wf-barrier:run-1:P" and mkey == "wf-barrier-meta:run-1:P"
    for k in (bkey, mkey):
        assert 0 < r.ttl(k) <= 30 * 86400


def test_exists_and_get(r):
    assert not wb.barrier_exists(r, RUN, C)
    assert wb.get_barrier(r, RUN, C) is None
    wb.record_child(r, RUN, C, "j0", OK, 2)
    assert wb.barrier_exists(r, RUN, C)


def test_bad_status_rejected(r):
    with pytest.raises(ValueError):
        wb.record_child(r, RUN, C, "j0", "job-started", 2)


def test_seed_then_event(r):
    wb.seed_barrier(r, RUN, C, 5, {"j0": OK, "j1": FAIL, "j2": OK})
    assert wb.get_barrier(r, RUN, C) == {"expected": 5, "completed": 2, "failed": 1, "released": 0}
    assert not wb.record_child(r, RUN, C, "j3", OK, 5).winner
    x = wb.record_child(r, RUN, C, "j4", OK, 5)
    assert (x.completed, x.failed, x.winner) == (4, 1, True)


def test_seed_complete_does_not_steal_win(r):
    wb.seed_barrier(r, RUN, C, 3, {"j0": OK, "j1": OK, "j2": OK})
    assert wb.get_barrier(r, RUN, C)["released"] == 0
    x = wb.record_child(r, RUN, C, "j2", OK, 3)  # the current event is already in the seed
    assert x.winner and (x.completed, x.failed) == (3, 0)
    assert not wb.record_child(r, RUN, C, "j2", OK, 3).winner


def test_seed_large_pipelined(r):
    n = 1700
    wb.seed_barrier(r, RUN, C, n, {f"j{i}": OK for i in range(n)})
    assert wb.get_barrier(r, RUN, C)["completed"] == n


# ------------------------------------------------------------------ concurrency
def _hammer(make_client, run, n=60, threads=16, seed=7):
    rnd = random.Random(seed)
    events = [(f"j{i}", FAIL if rnd.random() < 0.3 else OK) for i in range(n)]
    # every child delivered 1-4 times, some failed ones later redelivered as completed
    deliveries = []
    for jid, st in events:
        for _ in range(rnd.randint(1, 4)):
            deliveries.append((jid, st))
    rnd.shuffle(deliveries)
    chunks = [deliveries[i::threads] for i in range(threads)]
    winners, errors = [], []

    def work(chunk):
        client = make_client()
        try:
            for jid, st in chunk:
                x = wb.record_child(client, run, C, jid, st, n)
                if x.winner:
                    winners.append(x)
        except Exception as e:  # pragma: no cover
            errors.append(e)

    ts = [threading.Thread(target=work, args=(c,)) for c in chunks]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert not errors, errors
    assert len(winners) == 1
    k = sum(1 for _, s in events if s == OK)
    assert (winners[0].completed, winners[0].failed) == (k, n - k)


def test_concurrent_exactly_one_winner_fakeredis():
    server = fakeredis.FakeServer()
    for run in range(5):
        _hammer(lambda: fakeredis.FakeStrictRedis(server=server), f"run-{run}", seed=run)


@pytest.mark.skipif(os.environ.get("HYSDS_LIVE_REDIS") != "1", reason="set HYSDS_LIVE_REDIS=1")
def test_concurrent_exactly_one_winner_live_redis():
    import uuid

    from redis import StrictRedis
    url = os.environ.get("HYSDS_LIVE_REDIS_URL") or (
        "rediss://:" + os.environ.get("REDIS_PASSWORD", "")
        + "@localhost:6379/?ssl_cert_reqs=none")
    mk = lambda: StrictRedis.from_url(url)  # noqa: E731
    runs = [f"test-barrier-{uuid.uuid4().hex}" for _ in range(3)]
    try:
        for i, run in enumerate(runs):
            _hammer(mk, run, n=200, threads=16, seed=i)
    finally:
        c = mk()
        for run in runs:
            c.delete(*wb.barrier_keys(run, C))


# ------------------------------------------------------------------ rebuild
def _hit(job_id, status, ts):
    return {"_id": job_id + ts, "_source": {"job_id": job_id, "status": status, "@timestamp": ts},
            "sort": [ts, job_id]}


def test_rebuild_pages_and_latest_wins():
    pages = [
        {"hits": {"hits": [_hit("a", FAIL, "t1"), _hit("b", OK, "t2")]}},
        {"hits": {"hits": [_hit("a", OK, "t3"), _hit("c", OFF, "t4")]}},
        {"hits": {"hits": [_hit("c", OFF, "t5")]}},
    ]
    es = umock.MagicMock()
    es.search.side_effect = pages
    out = wb.rebuild_from_job_status(es, RUN, C, page_size=2)
    assert out == {"a": OK, "b": OK, "c": OFF}
    assert es.search.call_count == 3
    first = es.search.call_args_list[0][1]
    assert first["index"] == "job_status-current"
    filt = first["body"]["query"]["bool"]["filter"]
    assert {"term": {"workflow.run_id": RUN}} in filt and {"term": {"workflow.step_id": C}} in filt
    assert "search_after" in es.search.call_args_list[1][1]["body"]
    assert "search_after" in es.search.call_args_list[2][1]["body"]


def test_rebuild_empty():
    es = umock.MagicMock()
    es.search.return_value = {"hits": {"hits": []}}
    assert wb.rebuild_from_job_status(es, RUN, C) == {}


def test_rebuild_seed_then_event_counts(r):
    es = umock.MagicMock()
    es.search.return_value = {"hits": {"hits": [_hit("j0", OK, "t1"), _hit("j1", FAIL, "t2")]}}
    seed = wb.rebuild_from_job_status(es, RUN, C)
    wb.seed_barrier(r, RUN, C, 3, seed)
    x = wb.record_child(r, RUN, C, "j2", OK, 3)
    assert (x.completed, x.failed, x.expected, x.winner) == (1 + 1, 1, 3, True)


def test_expire_run_barriers_sets_ttl_on_both_keys_and_ignores_missing():
    import fakeredis
    r = fakeredis.FakeStrictRedis()
    wb.record_child(r, "R", "C1", "j1", "job-completed", 2)
    n = wb.expire_run_barriers(r, "R", ["C1", "C2-missing"], 100)
    assert n == 2
    for k in wb.barrier_keys("R", "C1"):
        assert r.ttl(k) == 100
    assert not r.exists(*wb.barrier_keys("R", "C2-missing"))
