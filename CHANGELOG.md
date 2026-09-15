# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Fixed
- HC-651: `scripts/reap_orphaned_job_failed.py` gains a second scan, for the
  pair celery redelivery leaves behind. A worker shut down mid-job writes
  `job-failed` and is killed before it acks, so the broker delivers the same
  task -- same uuid -- to another worker, which runs it to completion; the
  stale `job_failed` doc then sits beside the `job-completed` doc under one
  payload_id, and the retried-attempt scan excludes it at four gates (no
  `retry_count` moved, same uuid, equal counts, and a shared redis key that
  now reads `job-completed`). The new scan starts from executions carrying
  `job.delivery_info.redelivered: true` that reached `job-completed`, requires
  the same uuid, and judges the pair by each execution's own
  `job_info.time_start` / `time_end`: the re-run must have started after the
  failed execution ended. It reports `mechanism: redelivered_after_terminal`
  with basis `time_start`, records both executions' hosts in the event, and
  accepts `job-completed` for the shared key. Nothing else changes: the same
  flags, the same opt-in delete, and the same `--since` for a one-time sweep
  of pairs that already exist. New counters: `redelivered_scanned`,
  `redelivered_skipped_different_uuid`, `redelivered_skipped_not_later`,
  `redelivered_skipped_unclassified`, `redelivered_skipped_redis`.
- HC-651: with `ENABLE_JOB_LOCKING`, a redelivered task whose uuid already
  reads `job-completed` or `job-deduped` in redis is now deduped before any
  lock is taken, as the non-locking branch already did. The locking branch
  never made that check: a finished execution has released its lock, so the
  redelivery ran again, `publish_datasets` found its own dataset already
  published (same payload_id and task id), the job failed, and logstash's
  paired delete then removed the good `job-completed` doc. The dedup returns
  without writing a status, since a `job-deduped` write would land under the
  shared `_id` and clobber the completed doc it defers to. A redelivery after
  `job-failed` still re-runs on purpose.

## [3.3.3] - 2026-09-02

### Added
- `scripts/reap_orphaned_job_failed.py`, a mozart daemon that deletes
  `job_failed` documents a later attempt has superseded. It **deletes
  production failure records**, so the supervisord block ships with
  `--dry-run` and `--lookback-days 1`: reconcile a dry-run sweep against your
  own audit before letting it delete. **Deleting is opt-in**: without
  `--delete-orphans` the daemon only reports what it would delete, so a block
  copied into a PCM override that loses a flag can only get quieter, never
  destructive. `--lookback-days` defaults to one day, matching the redis
  job-status TTL; a longer window is refused without `--dry-run` or
  `--allow-expired-redis`, because past the TTL the redis cross-check is inert.
  Reporting sweeps still emit `job_failed_orphan_reaped` events, flagged
  `dry_run: true`, so the audit has something to read. Every event carries a
  `mechanism` (which side of the retry's delete the orphan was indexed on,
  from `_seq_no` against the mark lightweight-jobs v2.1.2 records on the
  resubmitted job) and a `mechanism_basis`, also as indexed tags. Every PCM
  overrides `supervisord.conf.mozart`, so the block has to be added to each
  override at adoption.
- `log_utils.job_supersession()`, one realtime `mget` over every index a job
  doc can live in, wired into every supervisory status writer (the watchdog,
  `event_processors._fail_job` and `offline_jobs`, `task_revoked_handler`,
  the job-lock contention path, `offline_orphaned_jobs`) so none of them
  overwrites a payload a newer attempt owns. It reports `ABSENT` (a retry
  just deleted the doc) and `UNKNOWN` (the probe could not ask) separately;
  the verdi sites degrade to their previous unconditional write on `UNKNOWN`
  rather than dropping a terminal record when mozart's OpenSearch is
  unreachable from a worker.

  Three limits are worth knowing before planning around this guard.

  **Two of the six sites are inert wherever a worker cannot reach mozart's
  OpenSearch.** On a venue whose verdi client verifies certificates, both
  `get_mozart_es().es.info()` and the guard's `mget` fail in about 0.2 s
  (`CERTIFICATE_VERIFY_FAILED` was measured on one live venue). The revoked-task
  handler and the job-lock path then answer `UNKNOWN` every time and keep their
  previous unconditional write. Failing open is deliberate -- losing the guard
  beats losing the record -- but do not count on live coverage at those two
  sites until a worker on that venue can make the call.

  **It does not separate two first attempts.** `job.retry_count` is written
  only by `retry.py`, so a first attempt carries no key and the comparison
  coerces both sides to `0`. With the strict "theirs greater than mine", two
  different first attempts under one payload -- a celery redelivery, or lock
  contention -- neither supersedes the other. That is correct for what this
  guard is for, since a retry always arrives at N+1, but it means the
  revoked-task handler is not a defence against redelivery contention.

  **`_fail_job` can now give up without writing a record.** It raises on both
  `ABSENT` and `UNKNOWN` so its own backoff re-reads, and if the retry's
  replacement doc is not visible within five tries the backoff exhausts and no
  terminal record is written. Given that writing during `ABSENT` would destroy
  the retried attempt's own doc through logstash's paired delete, losing the
  record is the better trade -- but it is a new way to lose one. The
  `fail_job_gave_up` event means either that a record was written and its rules
  never queued, or that no record was written at all; treat any occurrence as
  worth investigating rather than as routine.

### Changed
- **Upgrade order: factotum first.** `queue_finished_job` now sends `index`
  and `uuid` kwargs on every finished job, and a pre-3.3.3 `user_rules_job`
  worker raises TypeError on `uuid`, which stops all rule evaluation until it
  is updated, with no error that points at the upgrade. That worker runs on
  the factotum (`supervisord.conf.factotum`); the producers are on mozart
  (`event_processors`, `orchestrator`) and verdi (`job_worker`). Update the
  factotum before mozart and verdi.
- **Requires lightweight-jobs v2.1.2** for the reaper's exact classification:
  older retry jobs write no delete mark, and every classification falls back
  to `mechanism_basis: timestamp`. **sdscli 2.1.2**, or the same additions to
  the PCM's `job_status` template override, declares `job.retry_count` and
  `job.job_info.retry_delete`.
- **Behaviour change.** User rules now evaluate the failures `process_events`
  routes through `fail_job` -- WorkerLostError, TimeLimitExceeded and
  ConnectionError. Those evaluations previously settled against the dated
  index a job-failed doc had already been moved out of, exhausted their
  backoff and never ran. Rule sets that match on connection-error text will
  begin firing where they did not before; a celery-level ConnectionError is
  not by itself evidence that the job's work failed. Check your venue's live
  `user_rules-mozart` index before upgrading.
- `run_job` and `event_processors._fail_job` each write a failed job's
  terminal status document once. `run_job` wrote it twice; `_fail_job`
  re-wrote it on every backoff replay when the rule requeue hit an
  unreachable broker. Either duplicate could land after a retry had deleted
  the document and re-create it as an orphan.
- Rule evaluation's settle probe is pinned to the attempt that queued it, so
  an unreaped orphan under the same `_id` cannot satisfy it on the wrong doc.

## [3.3.2] - 2026-08-03

### Fixed
- Properly expand environment variables in `runtime_options` for podman
  (HC-641, #222).

## [3.3.1] - 2026-07-23

### Added
- Python 3.12 compatibility updates
- CHANGELOG.md file to track changes

### Changed
- Updated `datetime.utcnow()` to `datetime.now(timezone.utc)` throughout the codebase
- Replaced deprecated `logger.warn()` calls with `logger.warning()`
- Refactored `get_disk_usage` to use Python's native `os.lstat` and `os.walk`
- Updated timezone handling to be explicit with `UTC` timezone
- Modified `parse_iso8601` to return timezone-aware datetime objects
- Updated test cases to work with Python 3.12

### Fixed
- Failed job documents no longer lose their triage link (`products_staged`),
  exit code, and duration to a stale overwrite by `event_processors.fail_job`
  or the timeout watchdog (HC-639). Supervisory writers now consult the
  synchronous redis status key (`is_job_finalized()`) immediately before
  writing; the watchdog additionally waits a `-g/--grace-secs` grace period
  (default 300s) measured from the task-failed event; `TASK_FAILED_RE` no
  longer false-matches `SoftTimeLimitExceeded` as `TimeLimitExceeded`; and the
  redis status-key TTL is derived from the job's own `time_limit`. The
  watchdog also emits a per-sweep `divergence_count` WARNING (terminal redis
  status with a stale non-terminal ES doc) as the health signal for lost
  terminal writes. Behavior notes: `job-offline` counts as terminal for the
  guard (timedout tagging declines on offline jobs whose redis key
  survives), and a `ConnectionError` task-failed event for a worker that
  already finalized no longer rewrites the job document.
- `WorkerExecutionError`/`OrchestratorExecutionError` are picklable again, so
  celery no longer reports `UnpickleableExceptionWrapper` in the result
  backend and stored tracebacks (HC-638). MUST ship in the same release as
  the HC-639 fix above: this change removes the traceback string operators
  use to find documents damaged by HC-639.
- Fixed `test_import` in `test_version.py` to use proper assertions
- Resolved timezone-related `TypeError` exceptions in datetime operations

### Removed
- Removed dependency on system `du` command in `get_disk_usage`

## [Previous Versions]

*Note: Previous changes not documented in this changelog.*
