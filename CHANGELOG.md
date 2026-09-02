# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [3.3.3]

### Added
- `scripts/reap_orphaned_job_failed.py`, a mozart daemon that deletes
  `job_failed` documents a later attempt has superseded. It **deletes
  production failure records**, so the supervisord block ships with
  `--dry-run`: reconcile a sweep against your own audit before dropping the
  flag. Every PCM overrides `supervisord.conf.mozart`, so the block has to be
  added to each override at adoption.
- `log_utils.job_supersession()`, wired into every supervisory status writer,
  so none of them overwrites a payload a newer attempt owns.

### Changed
- **Behaviour change.** User rules now evaluate the failures `process_events`
  routes through `fail_job` -- WorkerLostError, TimeLimitExceeded and
  ConnectionError. Those evaluations previously settled against the dated
  index a job-failed doc had already been moved out of, exhausted their
  backoff and never ran. Rule sets that match on connection-error text will
  begin firing where they did not before; a celery-level ConnectionError is
  not by itself evidence that the job's work failed. Check your venue's live
  `user_rules-mozart` index before upgrading.
- `run_job` writes a failed job's terminal status document once instead of
  twice. The duplicate could land after a retry had deleted the document and
  re-create it as an orphan.

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
