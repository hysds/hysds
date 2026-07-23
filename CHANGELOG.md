# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

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
