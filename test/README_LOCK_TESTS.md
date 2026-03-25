# JobLock Unit Tests

Comprehensive unit tests for the distributed job locking mechanism.

## Test Coverage

The test suite covers:

- ✅ **Basic Operations** (8 tests) - acquire, release, extend, expiration
- ✅ **Heartbeat Functionality** (4 tests) - start, stop, extend, failures  
- ✅ **Stale Lock Detection** (5 tests) - various redelivery scenarios
- ✅ **Wait for Renewal** (3 tests) - worker alive/dead detection
- ✅ **Concurrency** (2 tests) - race conditions, parallel jobs
- ✅ **Configuration** (3 tests) - custom settings, metadata format, new configs

**Total: 25 tests**

## Requirements

Install dev dependencies:

```bash
pip install -r dev-requirements.in
```

This installs:
- `pytest` - Test framework
- `fakeredis` - In-memory Redis for fast tests
- `freezegun` - Time mocking for time-dependent tests
- `pytest-xdist` - Parallel test execution

## Running Tests

### Run all lock tests:

```bash
# From hysds root directory
pytest test/test_lock.py -v

# With coverage
pytest test/test_lock.py --cov=hysds.lock --cov-report=html

# Run in parallel (faster)
pytest test/test_lock.py -n auto
```

### Run specific test class:

```bash
pytest test/test_lock.py::TestJobLockBasicOperations -v
pytest test/test_lock.py::TestJobLockHeartbeat -v
pytest test/test_lock.py::TestJobLockStaleLockDetection -v
```

### Run specific test:

```bash
pytest test/test_lock.py::TestJobLockBasicOperations::test_acquire_lock_success -v
```

## Expected Runtime

With optimizations (mocked time and Redis):

- **All 25 tests: ~10-15 seconds total**
- Basic operations: ~2 seconds
- Heartbeat tests: ~5 seconds (some use real time.sleep for thread testing)
- Stale lock tests: ~1 second
- Wait for renewal: ~5 seconds (real thread timing)
- Concurrency tests: ~2 seconds
- Configuration tests: ~3 seconds

## Test Optimizations

These tests are optimized for speed:

1. **fakeredis** - In-memory Redis (no network overhead)
2. **freezegun** - Mock time.time() to avoid real waiting
3. **Short intervals** - Heartbeat intervals of 1-2s for testing (vs 30s production)
4. **Mocked sleeps** - Thread.Event.wait() returns immediately where possible

### Production vs Test Configuration

Tests use production default values except where speed is critical:

**Production defaults (in celeryconfig.py.tmpl):**
- `JOB_LOCK_HEARTBEAT_INTERVAL = 30` (30 seconds)
- `JOB_LOCK_MAX_EXTENSIONS = 4320` (1.5 days)
- `JOB_LOCK_HEARTBEAT_MAX_FAILURES = 3`
- `JOB_LOCK_STALE_CHECK_RETRIES = 3`

**Test values (where overridden for speed):**
- Heartbeat interval: 1-2 seconds (for thread timing tests)
- All other values match production defaults

## Key Test Scenarios

### 1. Basic Lock Lifecycle

```python
def test_acquire_lock_success(self):
    """Acquire → verify Redis keys → check TTL → validate metadata"""
```

### 2. Worker Crash Detection

```python
def test_stale_lock_same_task_different_worker_old_renewal(self):
    """Worker A crashes → Worker B detects stale lock → breaks it"""
```

### 3. Concurrent Access

```python
def test_concurrent_acquire_same_lock(self):
    """5 workers try to acquire same lock → only 1 succeeds"""
```

### 4. Heartbeat Extension

```python
def test_heartbeat_starts_and_extends_lock(self):
    """Start heartbeat → wait → verify lock extended"""
```

## Debugging Failed Tests

### View detailed output:

```bash
pytest test/test_lock.py -v -s  # -s shows print statements
```

### Run with pdb debugger:

```bash
pytest test/test_lock.py --pdb  # Drop into debugger on failure
```

### Check specific assertion:

```bash
pytest test/test_lock.py::test_name -vv  # Very verbose
```

## Continuous Integration

Add to your CI pipeline:

```yaml
# .github/workflows/test.yml
- name: Run JobLock Tests
  run: |
    pip install -r dev-requirements.in
    pytest test/test_lock.py --cov=hysds.lock --cov-report=xml
    
- name: Upload Coverage
  uses: codecov/codecov-action@v3
  with:
    file: ./coverage.xml
```

## Adding New Tests

Follow the existing pattern:

1. **Choose the right test class** based on what you're testing
2. **Use setUp/tearDown** for fixture management
3. **Mock time** with `@freeze_time` for time-dependent tests
4. **Use fakeredis** for Redis operations
5. **Keep tests fast** - avoid real sleep() when possible

Example:

```python
def test_new_feature(self):
    """Test description."""
    # Arrange
    lock = JobLock("payload", "task", "worker")
    
    # Act
    result = lock.some_method()
    
    # Assert
    self.assertTrue(result)
    self.assertIsNotNone(lock.get_lock_metadata())
```

## Known Limitations

1. **Thread timing** - A few tests use real `time.sleep()` for thread synchronization (heartbeat tests). These add ~5 seconds to runtime but ensure proper thread behavior.

2. **Pottery internals** - Tests rely on pottery's behavior. If pottery changes its internal implementation, some tests may need updates.

3. **Redis atomicity** - fakeredis approximates Redis behavior but may not perfectly match all edge cases. Integration tests with real Redis are recommended for production validation.

## Next Steps

Consider adding:

- **Integration tests** with real Redis instance
- **Load tests** with hundreds of concurrent workers
- **Chaos engineering** - random failures, network partitions
- **Performance benchmarks** - measure lock acquisition latency



