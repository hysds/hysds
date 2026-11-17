"""Live state of a workflow run in Redis (PLAN 6.10: while a run is active, Redis is the source of truth).

Nothing here reads or writes OpenSearch. The durable record is the snapshot of the run document that
every state change queues on the ``logstash`` Redis list (see :mod:`hysds.workflow_snapshot`).

Public API (the contract with mozart): :func:`init_run`, :func:`publish_run_event`, :func:`get_run`,
:func:`list_active_runs`.

Redis key layout (``R`` = run id, ``D`` = step id; every per-run key is scoped by ``R``)::

    wf:run:<R>        string  JSON run document (schema 2.0, incl. frozen job_definitions): the live doc
    wf:spec:<R>       string  JSON compiled spec 2.0 (a frozen copy: the consumer never reads specs elsewhere)
    wf:out:<R>        hash    step id -> JSON {"state": "completed"|"skipped", "outputs": {name: value}}
                              (the recorded step results input resolution and finalize pull from)
    wf:join:<R>:<D>   set     settled dependencies of D (PLAN 6.2; a set, so duplicate events are harmless)
    wf:rel:<R>:<D>    string  release claim of D (epoch seconds); D is released once (Lua)
    wf:done:<R>       set     settled step ids (completed, skipped, failed, cancelled): finalize when
                              SCARD == number of steps
    wf:sci:<R>:<D>    string  scattered step D: JSON list of the items being fanned out (children re-read it on retry)
    wf:sc:<R>:<D>     hash    scattered step D, one field per child index -> "<attempt>, <state>, <job id>" joined by TAB
                              (state: queued|running|retry_wait|completed|failed); the per-child state
    wf:sco:<R>:<D>    hash    child index -> JSON of the child's recorded outputs (completed) or of
                              {"error", "error_class"} (failed): gather reads these, never job_status
    wf:scm:<R>:<D>    hash    scatter counters n (children), completed, failed, retries; updated atomically
                              with the per-child state by one Lua script (the barrier, PLAN 6.5)
    wf:ver:<R>        string  last run-doc version (the monotonic external version of the snapshots)
    wf:active         zset    registry of active runs: member run id, score = heartbeat (epoch seconds,
                              refreshed by every state change); removed when the run ends

Every state change is ONE Redis transaction (WATCH on ``wf:run:<R>`` + MULTI): it writes the new run doc,
sets ``wf:ver:<R>`` to the doc's new (greater) version, refreshes the heartbeat, records step results/done
set when the change settles a step, and RPUSHes the snapshot. A concurrent change makes the transaction
retry from a fresh read, so readiness and state updates are atomic and replays are no-ops.

When a run ends (completed, failed, cancelled) every key above except ``wf:active`` gets a TTL
(``WORKFLOW_RUN_TTL``, default 7 days) and the run leaves the registry. Until then each key carries a safety
TTL (``WORKFLOW_RUN_MAX_TTL``, default 30 days) set at ``init_run``.

Events (kombu queue ``WORKFLOW_QUEUE``): job status events come from log_utils (no ``kind``); engine events
carry ``kind``: ``run_started``, ``cancel_requested`` (+ ``username``), ``step_skipped`` (a skip is a settle
event, PLAN 6.3). Redis must run with AOF and ``maxmemory-policy noeviction`` (PLAN 6.10).
"""

import json
import logging
import time
from datetime import datetime, timezone

from redis.exceptions import WatchError

from hysds import workflow_snapshot

log = logging.getLogger("hysds.workflow_state")

ACTIVE_KEY = "wf:active"
DEFAULT_RUN_TTL = 7 * 24 * 3600
DEFAULT_RUN_MAX_TTL = 30 * 24 * 3600
MAX_CAS_ATTEMPTS = 50
TERMINAL_RUN_STATUSES = frozenset({"completed", "failed", "cancelled"})
EVENT_KINDS = ("run_started", "cancel_requested", "step_skipped")


class WorkflowStateError(Exception):
    pass


class RunExists(WorkflowStateError):
    pass


class RunNotFound(WorkflowStateError):
    pass


def k_run(run_id):
    return "wf:run:%s" % run_id


def k_spec(run_id):
    return "wf:spec:%s" % run_id


def k_out(run_id):
    return "wf:out:%s" % run_id


def k_join(run_id, step_id):
    return "wf:join:%s:%s" % (run_id, step_id)


def k_rel(run_id, step_id):
    return "wf:rel:%s:%s" % (run_id, step_id)


def k_done(run_id):
    return "wf:done:%s" % run_id


def k_sc_items(run_id, step_id):
    return "wf:sci:%s:%s" % (run_id, step_id)


def k_sc(run_id, step_id):
    return "wf:sc:%s:%s" % (run_id, step_id)


def k_sc_out(run_id, step_id):
    return "wf:sco:%s:%s" % (run_id, step_id)


def k_sc_meta(run_id, step_id):
    return "wf:scm:%s:%s" % (run_id, step_id)


def k_ver(run_id):
    return "wf:ver:%s" % run_id


def _ttl(name, default):
    return int(workflow_snapshot._conf(name, default))


def _now():
    return time.time()


def run_keys(run_id, spec):
    """Every per-run key (for TTLs/cleanup); ``spec`` supplies the step ids."""
    keys = [k_run(run_id), k_spec(run_id), k_out(run_id), k_done(run_id), k_ver(run_id)]
    for sid in (spec or {}).get("steps") or {}:
        keys += [k_join(run_id, sid), k_rel(run_id, sid)]
    for sid, step in ((spec or {}).get("steps") or {}).items():
        if "scatter" in step:
            keys += [k_sc_items(run_id, sid), k_sc(run_id, sid), k_sc_out(run_id, sid), k_sc_meta(run_id, sid)]
    return keys


# --------------------------------------------------------------------------- reads


def get_run(r, run_id):
    """The live run document, or None."""
    raw = r.get(k_run(run_id))
    return json.loads(raw) if raw is not None else None


def get_spec(r, run_id):
    raw = r.get(k_spec(run_id))
    return json.loads(raw) if raw is not None else None


def settled_map(r, run_id):
    """``{step: {"state", "outputs"}}`` of the recorded results (the shape ``workflow_refs2`` resolves from)."""
    return {sid: json.loads(v) for sid, v in _decode_hash(r.hgetall(k_out(run_id))).items()}


def _decode_hash(h):
    return {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
            for k, v in h.items()}


def n_done(r, run_id):
    return int(r.scard(k_done(run_id)))


def list_active_runs(r):
    """Ids of the runs in the active registry, oldest heartbeat first."""
    return [m.decode() if isinstance(m, bytes) else m for m in r.zrange(ACTIVE_KEY, 0, -1)]


def active_heartbeats(r):
    """``{run_id: heartbeat_epoch_seconds}`` for the reaper (S8)."""
    return {(m.decode() if isinstance(m, bytes) else m): s for m, s in r.zrange(ACTIVE_KEY, 0, -1, withscores=True)}


# --------------------------------------------------------------------------- create


def init_run(r, spec, run_doc, list_key=None):
    """Store the compiled ``spec`` and ``run_doc`` (with its frozen job definitions), register the run as
    active with a heartbeat and queue the first snapshot, all in one transaction. Raises :class:`RunExists`.
    Returns ``run_doc``. Mozart then sends the ``run_started`` event."""
    run_id = run_doc["run_id"]
    list_key = list_key or workflow_snapshot.list_key_and_max()[0]
    max_ttl = _ttl("WORKFLOW_RUN_MAX_TTL", DEFAULT_RUN_MAX_TTL)
    with r.pipeline() as pipe:
        pipe.watch(k_run(run_id))
        if pipe.exists(k_run(run_id)):
            pipe.unwatch()
            raise RunExists("run %s already exists" % run_id)
        pipe.multi()
        pipe.set(k_run(run_id), json.dumps(run_doc), ex=max_ttl)
        pipe.set(k_spec(run_id), json.dumps(spec), ex=max_ttl)
        pipe.set(k_ver(run_id), run_doc["version"], ex=max_ttl)
        pipe.zadd(ACTIVE_KEY, {run_id: _now()})
        pipe.rpush(list_key, workflow_snapshot.build_snapshot(run_doc, run_doc["version"]))
        pipe.execute()
    workflow_snapshot.check_list_size(r, list_key)
    return run_doc


# --------------------------------------------------------------------------- atomic update


def update_run(r, run_id, fn, list_key=None):
    """Atomic read-modify-write of the run doc (the only way the engine changes state).

    ``fn(doc)`` returns ``None`` (nothing to do), or ``(new_doc, result)`` or
    ``(new_doc, result, extra)`` where ``extra(pipe)`` queues more commands in the same transaction
    (step results, done set). ``new_doc["version"]`` must exceed ``doc["version"]`` (the builders of
    ``workflow_run2`` guarantee it). Returns ``(new_doc, result)`` or ``None``. Raises :class:`RunNotFound`.
    """
    list_key = list_key or workflow_snapshot.list_key_and_max()[0]
    for _ in range(MAX_CAS_ATTEMPTS):
        with r.pipeline() as pipe:
            try:
                pipe.watch(k_run(run_id))
                raw = pipe.get(k_run(run_id))
                if raw is None:
                    pipe.unwatch()
                    raise RunNotFound("run %s not found" % run_id)
                doc = json.loads(raw)
                spec_raw = pipe.get(k_spec(run_id))
                out = fn(doc)
                if out is None:
                    pipe.unwatch()
                    return None
                new_doc, result = out[0], out[1]
                extra = out[2] if len(out) > 2 else None
                if new_doc["version"] <= doc["version"]:
                    raise WorkflowStateError("version must increase: %r -> %r" % (doc["version"], new_doc["version"]))
                ended = new_doc["status"] in TERMINAL_RUN_STATUSES
                pipe.multi()
                pipe.set(k_run(run_id), json.dumps(new_doc), keepttl=True)
                pipe.set(k_ver(run_id), new_doc["version"], keepttl=True)
                if extra is not None:
                    extra(pipe)
                pipe.rpush(list_key, workflow_snapshot.build_snapshot(new_doc, new_doc["version"]))
                if ended:
                    ttl = _ttl("WORKFLOW_RUN_TTL", DEFAULT_RUN_TTL)
                    pipe.zrem(ACTIVE_KEY, run_id)
                    spec = json.loads(spec_raw) if spec_raw else {"steps": new_doc["steps"]}
                    for key in run_keys(run_id, spec):
                        pipe.expire(key, ttl)
                else:
                    pipe.zadd(ACTIVE_KEY, {run_id: _now()})
                pipe.execute()
            except WatchError:
                continue
        workflow_snapshot.check_list_size(r, list_key)
        return new_doc, result
    raise WorkflowStateError("run %s: too much contention, gave up after %d attempts" % (run_id, MAX_CAS_ATTEMPTS))


def record_settle(pipe, run_id, step_id, state, outputs=None):
    """Queue (in an ``update_run`` transaction) the recorded result of a step that settles ``completed`` or
    ``skipped`` (pulled from by dependents and finalize) and its entry in the done set."""
    pipe.hset(k_out(run_id), step_id, json.dumps({"state": state, "outputs": outputs or {}}))
    pipe.sadd(k_done(run_id), step_id)


def record_done(pipe, run_id, step_id):
    """Queue the done-set entry of a step that settles ``failed`` or ``cancelled`` (no outputs)."""
    pipe.sadd(k_done(run_id), step_id)


# --------------------------------------------------------------------------- join (PLAN 6.2, B2)

JOIN_LUA = """
-- KEYS: join set and release claim of each dependent D, interleaved.  ARGV: settled step, now, replay,
-- ttl, then |deps(D)| of each dependent.  Returns the (1-based) positions of the dependents to release.
local step, now, replay, ttl = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4])
local out = {}
for i = 1, #KEYS, 2 do
  local jk, rk = KEYS[i], KEYS[i + 1]
  local n = (i + 1) / 2
  local ndeps = tonumber(ARGV[4 + n])
  redis.call('SADD', jk, step)
  redis.call('EXPIRE', jk, ttl)
  if redis.call('SCARD', jk) >= ndeps then
    local fresh = redis.call('SET', rk, now, 'NX', 'EX', ttl)
    if fresh or replay == '1' then out[#out + 1] = n end
  end
end
return out
"""


def join(r, run_id, spec, step_id, dependents, replay=False):
    """A step settled (completed or skipped): add it to the join set of every dependent. Atomically
    returns the dependents whose deps are now all settled and that were not released before (exactly
    once). ``replay=True`` (a duplicate settle event) also returns already-released ones, so a crash
    between release and submit is healed; the engine's run-doc transition keeps submission exactly-once."""
    if not dependents:
        return []
    ttl = _ttl("WORKFLOW_RUN_MAX_TTL", DEFAULT_RUN_MAX_TTL)
    keys, ndeps = [], []
    for d in dependents:
        keys += [k_join(run_id, d), k_rel(run_id, d)]
        ndeps.append(len(spec["steps"][d].get("deps") or []))
    idx = r.eval(JOIN_LUA, len(keys), *keys, step_id, repr(_now()), "1" if replay else "0", ttl, *ndeps)
    return [dependents[int(i) - 1] for i in idx]


def claim_release(r, run_id, step_id):
    """Release claim of a step with no deps (run start): True for the first caller."""
    ttl = _ttl("WORKFLOW_RUN_MAX_TTL", DEFAULT_RUN_MAX_TTL)
    return bool(r.set(k_rel(run_id, step_id), repr(_now()), nx=True, ex=ttl))


def drop_release_claim(r, run_id, step_id):
    r.delete(k_rel(run_id, step_id))


# --------------------------------------------------------------------------- scatter (PLAN 6.5, B14)

SCATTER_LUA = """
-- KEYS: per-child state hash (sc), child results hash (sco), counters hash (scm).
-- ARGV: op, idx, then per op. A child record is attempt, state, job id joined by TAB.
-- Returns {ok, completed, failed, n}; ok = 1 when the op changed something (0: stale, duplicate or unknown).
local sc, sco, scm = KEYS[1], KEYS[2], KEYS[3]
local op, idx = ARGV[1], ARGV[2]
local function parse(rec)
  if not rec then return nil end
  local a, st, job = string.match(rec, "^(%d+)\t([%a_]+)\t(.*)$")
  return {tonumber(a), st, job}
end
local function put(a, st, job) redis.call('HSET', sc, idx, a .. "\t" .. st .. "\t" .. job) end
local function counts(ok)
  local m = redis.call('HMGET', scm, 'completed', 'failed', 'n')
  return {ok, tonumber(m[1]) or 0, tonumber(m[2]) or 0, tonumber(m[3]) or 0}
end
local rec = parse(redis.call('HGET', sc, idx))
local ok = 0
if op == 'claim' then          -- ARGV: job, ttl
  if not rec then
    put(1, 'queued', ARGV[3]); redis.call('EXPIRE', sc, tonumber(ARGV[4])); ok = 1
  end
elseif op == 'unclaim' then    -- ARGV: job   (the first submit failed)
  if rec and rec[1] == 1 and rec[2] == 'queued' and rec[3] == ARGV[3] then redis.call('HDEL', sc, idx); ok = 1 end
elseif op == 'started' then    -- ARGV: job
  if rec and rec[3] == ARGV[3] and rec[2] == 'queued' then put(rec[1], 'running', rec[3]); ok = 1 end
elseif op == 'settle' then     -- ARGV: job, final state (completed|failed), result json, ttl
  if rec and rec[3] == ARGV[3] and (rec[2] == 'queued' or rec[2] == 'running' or rec[2] == 'retry_wait') then
    put(rec[1], ARGV[4], rec[3])
    redis.call('HSET', sco, idx, ARGV[5]); redis.call('EXPIRE', sco, tonumber(ARGV[6]))
    redis.call('HINCRBY', scm, ARGV[4], 1)
    ok = 1
  end
elseif op == 'retry_mark' then -- ARGV: job   (an attempt failed and will be retried; replays are fine)
  if rec and rec[3] == ARGV[3] then
    if rec[2] == 'queued' or rec[2] == 'running' then put(rec[1], 'retry_wait', rec[3]); ok = 1
    elseif rec[2] == 'retry_wait' then ok = 1 end
  end
elseif op == 'requeue' then    -- ARGV: old job, new job
  if rec and rec[3] == ARGV[3] and rec[2] == 'retry_wait' then
    put(rec[1] + 1, 'queued', ARGV[4]); redis.call('HINCRBY', scm, 'retries', 1); ok = 1
  end
elseif op == 'revert' then     -- ARGV: new job, old job   (the retry could not be submitted)
  if rec and rec[3] == ARGV[3] and rec[2] == 'queued' and rec[1] > 1 then
    put(rec[1] - 1, 'retry_wait', ARGV[4]); redis.call('HINCRBY', scm, 'retries', -1); ok = 1
  end
end
return counts(ok)
"""

CHILD_ACTIVE_STATES = ("queued", "running", "retry_wait")


def parse_child(raw):
    """``"<attempt>\\t<state>\\t<job id>"`` -> ``{"attempt", "state", "job"}`` (None for no record)."""
    if raw is None:
        return None
    raw = raw.decode() if isinstance(raw, bytes) else raw
    a, st, job = raw.split("\t", 2)
    return {"attempt": int(a), "state": st, "job": job}


def scatter_begin(pipe, run_id, step_id, items):
    """Queue (in the ``update_run`` transaction that moves the step into its fan-out) the item list and the
    zeroed counters. Replaces any earlier state of the step (a step fans out once)."""
    ttl = _ttl("WORKFLOW_RUN_MAX_TTL", DEFAULT_RUN_MAX_TTL)
    for k in (k_sc(run_id, step_id), k_sc_out(run_id, step_id), k_sc_meta(run_id, step_id)):
        pipe.delete(k)
    pipe.set(k_sc_items(run_id, step_id), json.dumps(items), ex=ttl)
    pipe.hset(k_sc_meta(run_id, step_id), mapping={"n": len(items), "completed": 0, "failed": 0, "retries": 0})
    pipe.expire(k_sc_meta(run_id, step_id), ttl)


def scatter_items(r, run_id, step_id):
    raw = r.get(k_sc_items(run_id, step_id))
    return json.loads(raw) if raw is not None else None


def scatter_op(r, run_id, step_id, op, idx, *args):
    """Run one atomic per-child transition (see ``SCATTER_LUA``). Returns ``(ok, n_completed, n_failed, n)``."""
    ttl = _ttl("WORKFLOW_RUN_MAX_TTL", DEFAULT_RUN_MAX_TTL)
    if op == "claim":
        args = args + (ttl,)
    elif op == "settle":
        args = args + (ttl,)
    res = r.eval(SCATTER_LUA, 3, k_sc(run_id, step_id), k_sc_out(run_id, step_id), k_sc_meta(run_id, step_id),
                 op, idx, *args)
    return bool(int(res[0])), int(res[1]), int(res[2]), int(res[3])


def scatter_child(r, run_id, step_id, idx):
    return parse_child(r.hget(k_sc(run_id, step_id), str(idx)))


def scatter_counts(r, run_id, step_id):
    """``{"n", "completed", "failed", "retries"}`` (all 0 before the fan-out starts)."""
    m = _decode_hash(r.hgetall(k_sc_meta(run_id, step_id)))
    return {k: int(m.get(k, 0)) for k in ("n", "completed", "failed", "retries")}


def scatter_children(r, run_id, step_id):
    """``{index: {"attempt", "state", "job"}}`` of every claimed child."""
    return {int(k): parse_child(v) for k, v in _decode_hash(r.hgetall(k_sc(run_id, step_id))).items()}


def scatter_results(r, run_id, step_id):
    """``{index: result}`` of the settled children (outputs dict for completed, ``{"error", "error_class"}``
    for failed)."""
    return {int(k): json.loads(v) for k, v in _decode_hash(r.hgetall(k_sc_out(run_id, step_id))).items()}


# --------------------------------------------------------------------------- events


def _kombu_publish(event):
    from kombu import Exchange, Queue

    from hysds.celery import app
    from hysds.log_utils import get_workflow_queue_producer

    queue_name = app.conf.WORKFLOW_QUEUE
    exchange = Exchange(queue_name, type="direct")
    queue = Queue(queue_name, exchange, routing_key=queue_name)
    get_workflow_queue_producer().publish(event, exchange=exchange, routing_key=queue_name,
                                          declare=[queue], retry=True)


def publish_run_event(run_id, kind, payload=None, publish_fn=None):
    """Put an engine event on the queue the consumer reads. ``kind``: ``run_started`` (mozart, after
    ``init_run``), ``cancel_requested`` (payload ``{"username": ...}``), ``step_skipped`` (internal).
    ``publish_fn`` is for tests."""
    if kind not in EVENT_KINDS:
        raise ValueError("unknown run event kind %r (one of %s)" % (kind, EVENT_KINDS))
    event = dict(payload or {})
    event.update(kind=kind, run_id=run_id,
                 timestamp=datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z")
    (publish_fn or _kombu_publish)(event)
    return event
