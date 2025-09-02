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

HySDS (Hybrid Cloud Science Data System) is a sophisticated distributed job execution framework built on Celery that processes both lightweight tasks and complex containerized scientific workflows. The project is currently on version 1.3.8 and has been modernized for Python 3.12 compatibility.

### Project Structure
```
hysds/
├── hysds/                      # Main package
│   ├── containers/             # Container abstraction layer
│   ├── pymonitoredrunner/      # Process monitoring system
│   ├── orchestrator.py         # Central job coordination
│   ├── job_worker.py           # Containerized job execution
│   ├── task_worker.py          # Lightweight task execution
│   ├── utils.py                # Core utilities
│   └── log_utils.py            # Logging infrastructure
├── configs/                    # Configuration templates
│   ├── celery/                 # Celery configuration
│   ├── orchestrator/           # Job type mappings
│   ├── worker/                 # Worker specifications
│   └── datasets/               # Data ingestion rules
├── scripts/                    # Operational scripts
└── test/                       # Test suite with examples
```

### Core Components

**Orchestrator** (`hysds/orchestrator.py`):
- Central job coordination and routing hub
- Job submission validation and transformation using job creator functions
- Job deduplication using payload hashing for efficiency
- Queue routing based on job type with priority and time limit management
- Dynamic function loading with LRU cache for performance optimization
- Comprehensive error tracking with job status logging

**Job Worker** (`hysds/job_worker.py`):
- Executes complex, long-running containerized jobs in isolated environments
- Creates timestamped work directories with automatic cleanup
- Manages full job lifecycle including pre/post-processor pipelines
- Built-in processors: URL localization, dataset marking, bulk publishing
- Resource cleanup and disk space management with automatic threshold-based cleanup
- Signal handling for graceful shutdown and container lifecycle management

**Task Worker** (`hysds/task_worker.py`):
- Executes lightweight Python functions directly without containers
- Creates temporary work directories for function execution
- Collects worker instance facts and integrates with AWS metadata
- Used for quick computational tasks and system operations

### Container Architecture

HySDS uses a pluggable container system (`hysds/containers/`) with factory pattern:
- **Factory pattern** (`factory.py`) for container engine selection (Docker, Podman, Singularity)
- **Abstract base class** (`base.py`) defines common container interface
- **Engine-specific implementations** (`docker.py`, `podman.py`) handle engine differences
- **Security features** include mount validation, user mapping, and security controls
- **Process monitoring** (`pymonitoredrunner/`) provides stream monitoring and observer patterns

### Job Execution Flow

1. Job submitted to orchestrator via `submit_job()` with validation
2. Orchestrator validates and transforms payload using job creator functions
3. Job queued to appropriate worker queue based on job type and priority
4. Job worker creates isolated environment and executes container
5. Pre/post-processors handle data localization, validation, and publishing
6. Status tracked in Elasticsearch/Redis for monitoring and debugging
7. Automatic resource cleanup based on disk usage thresholds

### Key Configuration Files

- `celeryconfig.py` - Celery broker and worker configuration
- `configs/orchestrator/orchestrator_jobs.json` - Maps job types to creator functions
- `configs/worker/job_worker.json.*` - Worker execution specifications and constraints
- `configs/datasets/datasets.json` - Dataset ingestion rules and metadata
- `setup.cfg` - pytest configuration with coverage settings

## Python 3.12 Requirements

This codebase requires Python 3.12+ with specific modernization patterns. Recent updates include string formatting modernization (f-strings) and core dependency upgrades.

### Modernization Status
**Completed Updates:**
- ✅ Setup requirements explicitly require Python 3.12
- ✅ All major dependencies updated for Python 3.12 compatibility
- ✅ String formatting modernized from %-style to f-strings (commit fd03c43)
- ✅ Core datetime usage updated to `datetime.now(UTC)` in main modules

**Remaining Legacy Patterns:**
- ⚠️ 14 files still contain `datetime.utcnow()` usage (needs migration)
- ⚠️ 13 files still use deprecated `logger.warn()` (needs migration)
- ⚠️ `from __future__ import` statements throughout codebase (evaluate necessity)

### Datetime Handling
```python
# Use timezone-aware datetime
from datetime import datetime, timezone
created_time = datetime.now(timezone.utc)  # Not datetime.utcnow()
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

## Key Patterns and Conventions

### Job Specifications
Jobs are defined with standardized JSON format:
- `job_type`: Maps to orchestrator configuration and job creator functions
- `payload`: Input data and parameters for job execution
- `container_image_name`: Container for execution environment
- `priority`, `time_limit`: Resource constraints and scheduling hints

### Architectural Patterns
- **Factory Pattern**: Container engine selection (`hysds/containers/factory.py`)
- **Observer Pattern**: Stream monitoring in `pymonitoredrunner`
- **Template Pattern**: Configuration files use templating for flexibility
- **Singleton Pattern**: Global facts collection and Redis connection pools

### Error Handling
- Use `backoff` decorators for retry logic with exponential backoff
- Log errors with `get_short_error()` for concise, actionable messages
- Handle `SoftTimeLimitExceeded` for job timeouts and graceful shutdown
- Comprehensive status tracking through Redis/Elasticsearch

### Disk Space Management
Jobs automatically clean old data when disk usage exceeds thresholds using `get_disk_usage()` and related utilities. Cleanup is triggered during job execution to prevent disk space issues.

### Resource Management
- Isolated work directories with timestamp-based naming
- Automatic cleanup of temporary files and containers
- Memory and CPU constraints enforced through container limits
- Process monitoring for resource usage tracking

### Configuration Management
- Job creator functions dynamically loaded with LRU caching
- Template-based configuration files for different environments
- Centralized orchestrator job mapping for extensibility
- Worker specification files define execution parameters