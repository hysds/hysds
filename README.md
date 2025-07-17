# Background

For more information, check out:
- Wiki: https://hysds-core.atlassian.net/wiki/spaces/HYS/overview
- Issue Tracker: https://hysds-core.atlassian.net/projects/HC/issues
- Repos: https://github.com/hysds/

# HySDS

[![CircleCI](https://circleci.com/gh/hysds/hysds.svg?style=svg)](https://circleci.com/gh/hysds/hysds)

Core component for the Hybrid Science Data System


## Prerequisites

- Python 3.12+ (required)
- pip 23.0+
- setuptools 68.0.0+
- virtualenv 20.0.0+
- prov-es 0.2.0+
- osaka 0.2.0+
- hysds-commons 0.2.0+

## Python 3.12 Compatibility

This version of HySDS is fully compatible with Python 3.12. The following changes were made to ensure compatibility:

- Updated `datetime.utcnow()` to `datetime.now(timezone.utc)` throughout the codebase
- Replaced deprecated `logger.warn()` calls with `logger.warning()`
- Refactored `get_disk_usage` to use Python's native `os.lstat` and `os.walk`
- Updated timezone handling to be explicit with `UTC` timezone

### Known Issues

- Some modules have low test coverage (see test coverage report)
- Some third-party dependencies may have their own Python 3.12 compatibility considerations

## Dependency Management

This project uses `pip-tools` for dependency management. The main dependency files are:

- `requirements.in` - Main production dependencies
- `dev-requirements.in` - Development dependencies
- `requirements.txt` - Compiled production dependencies (generated)
- `dev-requirements.txt` - Compiled development dependencies (generated)

### Updating Dependencies

1. Install pip-tools:
   ```bash
   pip install pip-tools
   ```

2. Update requirements files:
   ```bash
   pip-compile --upgrade --output-file=requirements.txt requirements.in
   pip-compile --upgrade --output-file=dev-requirements.txt dev-requirements.in
   ```

3. To add a new dependency, edit the appropriate `.in` file and recompile.


## Installation

1. Create virtual environment and activate:
  ```
  virtualenv env
  source env/bin/activate
  ```

2. Update pip and setuptools:
  ```
  pip install -U pip
  pip install -U setuptools
  ```

3. Install prov-es:
  ```
  git clone https://github.com/pymonger/prov_es.git
  cd prov_es
  pip install .
  cd ..
  ```

4. Install hysds:
  ```
  pip install -r requirements.txt
  git clone https://github.com/hysds/hysds.git
  cd hysds
  pip install .
  ```
