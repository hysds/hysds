"""Redis scatter/gather barrier (workflow-orchestration v1 contract section 10, "Barrier").

Keys (per run ``R`` and scatter child step ``C``)::

    wf-barrier:<R>:<C>       hash  {job_id: terminal_status}
    wf-barrier-meta:<R>:<C>  hash  {expected: N, completed: k, failed: f, released: 0|1}

One atomic Lua script handles every child event:

* ``expected`` is initialised from ``child_total`` when the meta hash does not exist yet
  (both keys get ``EXPIRE 30d`` on creation).
* Upgrade semantics: a new job_id is counted; the same status again is a no-op;
  ``job-failed``/``job-offline`` -> ``job-completed`` moves the counts; completed is sticky
  (completed -> failed is ignored).
* When ``completed + failed == expected`` and ``released == 0`` the script sets ``released = 1``
  and reports ``winner = 1`` -- exactly once per barrier, whichever event gets there first.
  Seeding (Redis-loss rebuild) uses the same script with ``release = 0`` so a seed can never
  steal the win from the event being processed.

The script returns ``{completed, failed, expected, winner}``.
"""

import logging
from dataclasses import dataclass

log = logging.getLogger("hysds.workflow_barrier")

BARRIER_TTL_SECONDS = 30 * 24 * 3600
COMPLETED_STATUS = "job-completed"
TERMINAL_CHILD_STATUSES = ("job-completed", "job-failed", "job-offline")
SEED_PIPELINE_CHUNK = 500

BARRIER_LUA = """
local bkey = KEYS[1]
local mkey = KEYS[2]
local job_id = ARGV[1]
local status = ARGV[2]
local child_total = tonumber(ARGV[3])
local release = ARGV[4]
local ttl = tonumber(ARGV[5])

if redis.call('EXISTS', mkey) == 0 then
  redis.call('HSET', mkey, 'expected', child_total, 'completed', 0, 'failed', 0, 'released', 0)
  redis.call('EXPIRE', mkey, ttl)
  redis.call('EXPIRE', bkey, ttl)
end

local is_completed = (status == 'job-completed')
local prev = redis.call('HGET', bkey, job_id)
if not prev then
  redis.call('HSET', bkey, job_id, status)
  if is_completed then
    redis.call('HINCRBY', mkey, 'completed', 1)
  else
    redis.call('HINCRBY', mkey, 'failed', 1)
  end
  if redis.call('TTL', bkey) < 0 then redis.call('EXPIRE', bkey, ttl) end
elseif prev ~= 'job-completed' and is_completed then
  redis.call('HSET', bkey, job_id, status)
  redis.call('HINCRBY', mkey, 'failed', -1)
  redis.call('HINCRBY', mkey, 'completed', 1)
end

local c = tonumber(redis.call('HGET', mkey, 'completed'))
local f = tonumber(redis.call('HGET', mkey, 'failed'))
local expected = tonumber(redis.call('HGET', mkey, 'expected'))
local winner = 0
if release == '1' and c + f == expected and redis.call('HGET', mkey, 'released') == '0' then
  redis.call('HSET', mkey, 'released', 1)
  winner = 1
end
return {c, f, expected, winner}
"""


@dataclass
class BarrierResult:
    completed: int
    failed: int
    expected: int
    winner: bool
    rebuilt: bool = False

    def summary(self):
        return (f"barrier completed={self.completed} failed={self.failed} expected={self.expected} "
                f"winner={self.winner}" + (" rebuilt=True" if self.rebuilt else ""))


def barrier_keys(run_id, step_id):
    return f"wf-barrier:{run_id}:{step_id}", f"wf-barrier-meta:{run_id}:{step_id}"


_scripts = {}


def _script(redis):
    # register_script binds to a client; cache one Script per client object
    key = id(redis)
    entry = _scripts.get(key)
    if entry is None or entry[0] is not redis:
        entry = (redis, redis.register_script(BARRIER_LUA))
        _scripts[key] = entry
    return entry[1]


def _args(job_id, status, child_total, release):
    return [job_id, status, int(child_total), "1" if release else "0", BARRIER_TTL_SECONDS]


def _result(raw, rebuilt=False):
    c, f, n, w = (int(x) for x in raw)
    return BarrierResult(c, f, n, bool(w), rebuilt)


def record_child(redis, run_id, step_id, job_id, status, child_total):
    """Apply one child terminal event atomically. Returns a :class:`BarrierResult`."""
    if status not in TERMINAL_CHILD_STATUSES:
        raise ValueError(f"not a terminal child status: {status!r}")
    keys = list(barrier_keys(run_id, step_id))
    return _result(_script(redis)(keys=keys, args=_args(job_id, status, child_total, True)))


def barrier_exists(redis, run_id, step_id):
    """True if either barrier key exists (the rebuild runs only when both are missing)."""
    bkey, mkey = barrier_keys(run_id, step_id)
    return bool(redis.exists(bkey, mkey))


def seed_barrier(redis, run_id, step_id, expected, statuses):
    """Rebuild seed: apply ``{job_id: terminal_status}`` with the same upgrade semantics, but
    never release (the event being processed decides the winner). Returns the counts."""
    keys = list(barrier_keys(run_id, step_id))
    script = _script(redis)
    items = list(statuses.items())
    for i in range(0, len(items), SEED_PIPELINE_CHUNK):
        pipe = redis.pipeline(transaction=False)
        for job_id, status in items[i:i + SEED_PIPELINE_CHUNK]:
            if status in TERMINAL_CHILD_STATUSES:
                script(keys=keys, args=_args(job_id, status, expected, False), client=pipe)
        pipe.execute()
    return get_barrier(redis, run_id, step_id)


def get_barrier(redis, run_id, step_id):
    """Meta counts as ints, or None if there is no barrier."""
    _, mkey = barrier_keys(run_id, step_id)
    raw = redis.hgetall(mkey)
    if not raw:
        return None
    meta = {(k.decode() if isinstance(k, bytes) else k): int(v) for k, v in raw.items()}
    return meta


# ----------------------------------------------------------------------------- rebuild
def rebuild_from_job_status(es, run_id, step_id, index="job_status-current", page_size=1000):
    """``{job_id: latest terminal status}`` for the scatter child ``step_id`` of ``run_id``.

    Plain filtered search on the raw client, ascending by ``(@timestamp, job_id)`` and paged with
    ``search_after`` (N can be 10k), so a later doc for the same job_id overwrites an earlier one.
    Reads a lagging index: it can undercount (the Phase 4 reaper reconciles).
    """
    body = {
        "size": page_size,
        "_source": ["job_id", "status", "@timestamp"],
        "query": {"bool": {"filter": [
            {"term": {"workflow.run_id": run_id}},
            {"term": {"workflow.step_id": step_id}},
            {"terms": {"status": list(TERMINAL_CHILD_STATUSES)}},
        ]}},
        "sort": [{"@timestamp": "asc"}, {"job_id": "asc"}],
    }
    latest = {}
    while True:
        r = es.search(index=index, body=body)
        hits = (r.get("hits") or {}).get("hits") or []
        for h in hits:
            src = h.get("_source") or {}
            job_id = src.get("job_id") or h.get("_id")
            if job_id and src.get("status") in TERMINAL_CHILD_STATUSES:
                latest[job_id] = src["status"]
        if len(hits) < page_size:
            break
        body["search_after"] = hits[-1]["sort"]
    return latest
