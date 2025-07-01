# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Testing
```bash
# Run all tests
pytest

# Run tests with coverage
pytest -v --cov=hysds --cov-report=term-missing --cov-report=xml

# Run specific test file
pytest test/test_job_worker.py
```

### Building and Installation
```bash
# Install in development mode (recommended)
pip install -e .

# Build distribution packages
python setup.py sdist bdist_wheel

# Copy Celery config template (required for testing)
cp configs/celery/celeryconfig.py.tmpl celeryconfig.py
```

### Code Quality
```bash
# Run all pre-commit hooks
pre-commit run --all-files

# Individual linting tools
flake8 .
black .
isort .
mypy hysds
```

### Dependency Management
```bash
# Update requirements files
./update_requirements.sh

# Or manually with pip-tools
pip-compile --upgrade --output-file=requirements.txt requirements.in
pip-compile --upgrade --output-file=dev-requirements.txt dev-requirements.in
```

## Architecture Overview

HySDS (Hybrid Cloud Science Data System) is a distributed job execution framework built on Celery. It processes both lightweight tasks and complex containerized scientific workflows.

### Core Components

**Orchestrator** (`hysds/orchestrator.py`):
- Central job coordination and routing
- Handles job submission, validation, and deduplication
- Routes jobs to appropriate queues based on job type
- Manages job creator functions that transform payloads into executable jobs

**Job Worker** (`hysds/job_worker.py`):
- Executes complex, long-running containerized jobs
- Creates isolated work directories with timestamps
- Manages full job lifecycle including pre/post-processors
- Handles resource cleanup and disk space management

**Task Worker** (`hysds/task_worker.py`):
- Executes lightweight Python functions
- Direct function invocation without containers
- Used for quick computational tasks

### Container Architecture

HySDS uses a pluggable container system (`hysds/containers/`):
- **Factory pattern** for container engine selection (Docker, Podman, Singularity)
- **Abstract base class** defines common container interface
- **Engine-specific implementations** handle Docker vs Podman differences
- **Security features** include mount validation and user mapping

### Job Execution Flow

1. Job submitted to orchestrator via `submit_job()`
2. Orchestrator validates and transforms payload using job creator functions
3. Job queued to appropriate worker queue
4. Job worker creates isolated environment and executes container
5. Pre/post-processors handle data localization and publishing
6. Status tracked in Elasticsearch for monitoring

### Key Configuration Files

- `celeryconfig.py` - Celery broker and worker configuration
- `configs/orchestrator/orchestrator_jobs.json` - Maps job types to creator functions
- `configs/worker/job_worker.json.*` - Worker execution specifications
- `configs/datasets/datasets.json` - Dataset ingestion rules

## Python 3.12 Requirements

This codebase requires Python 3.12+ with specific modernization patterns:

### Datetime Handling
```python
# Use timezone-aware datetime
from datetime import datetime, UTC
created_time = datetime.now(UTC)  # Not datetime.utcnow()
```

### Logging
```python
# Use warning() instead of deprecated warn()
logger.warning("message")  # Not logger.warn()
```

### Dependencies
All dependencies must be Python 3.12 compatible. Update `requirements.in` and run `pip-compile` to generate updated `requirements.txt`.

## Testing Notes

- Tests use pytest with configurations in `setup.cfg`
- Some tests require Docker/container engine running
- Coverage reporting generates XML output for CI
- Test data located in `test/examples/`

## Key Patterns

### Job Specs
Jobs are defined with:
- `job_type`: Maps to orchestrator configuration
- `payload`: Input data and parameters
- `container_image_name`: Container for execution
- `priority`, `time_limit`: Resource constraints

### Error Handling
- Use `backoff` decorators for retry logic
- Log errors with `get_short_error()` for concise messages
- Handle `SoftTimeLimitExceeded` for job timeouts

### Disk Space Management
Jobs automatically clean old data when disk usage exceeds thresholds using `get_disk_usage()` and related utilities.