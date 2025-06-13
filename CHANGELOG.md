# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
- Python 3.12 compatibility updates
- CHANGELOG.md file to track changes

### Changed
- Updated `datetime.utcnow()` to `datetime.now(UTC)` throughout the codebase
- Replaced deprecated `logger.warn()` calls with `logger.warning()`
- Refactored `get_disk_usage` to use Python's native `os.lstat` and `os.walk`
- Updated timezone handling to be explicit with `UTC` timezone
- Modified `parse_iso8601` to return timezone-aware datetime objects
- Updated test cases to work with Python 3.12

### Fixed
- Fixed `test_import` in `test_version.py` to use proper assertions
- Resolved timezone-related `TypeError` exceptions in datetime operations

### Removed
- Removed dependency on system `du` command in `get_disk_usage`

## [Previous Versions]

*Note: Previous changes not documented in this changelog.*
