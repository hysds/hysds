# Job Lock Configuration Changes

## Summary

Updated the distributed locking mechanism to be more robust and faster at detecting worker failures while remaining conservative to avoid false positives.

## Key Changes

### 1. Faster Heartbeat Interval
- **Before:** 120 seconds
- **After:** 30 seconds
- **Benefit:** 4x faster worker failure detection while maintaining negligible Redis load

### 2. Multiple Heartbeat Retries for Stale Detection
- **Before:** Wait for 1 missed heartbeat before declaring stale (different worker) or use snapshot check (same worker)
- **After:** Wait for 3 missed heartbeats (configurable) for BOTH different worker AND same worker scenarios
- **Benefit:** More conservative and consistent, handles transient issues (GC pauses, network hiccups, CPU spikes)
- **Consistency:** Same wait-and-observe logic applied to both redelivery scenarios

### 3. Configurable Heartbeat Failure Threshold
- **Before:** Hardcoded to 3 failures
- **After:** Configurable via `JOB_LOCK_HEARTBEAT_MAX_FAILURES` (default: 3)
- **Benefit:** Can be tuned based on environment reliability

### 4. Updated Max Extensions
- **Before:** 1,080 extensions (for 120s interval = 1 day)
- **After:** 4,320 extensions (for 30s interval = 1.5 days)
- **Benefit:** Maintains same max job duration support

## Configuration Values

```python
# In celeryconfig.py.tmpl

# Lock expiration time (safety buffer if heartbeat fails)
JOB_LOCK_EXPIRE_TIME = 600  # 10 minutes

# How often to renew the lock
JOB_LOCK_HEARTBEAT_INTERVAL = 30  # 30 seconds

# Max extensions before lock auto-expires
JOB_LOCK_MAX_EXTENSIONS = 4320  # 4,320 × 30s = 36 hours (1.5 days)

# Max consecutive heartbeat failures before stopping
JOB_LOCK_HEARTBEAT_MAX_FAILURES = 3  # 3 × 30s = 90s tolerance

# Number of missed heartbeats before declaring lock stale
JOB_LOCK_STALE_CHECK_RETRIES = 3  # 3 × 30s = 90s detection time
```

## Timing Breakdown

### Worker Crash Detection

**Before (120s heartbeat, 1x wait):**
- Best case: ~10s (just missed heartbeat)
- Average case: ~130s
- Worst case: ~130s

**After (30s heartbeat, 3x wait):**
- Best case: ~10s (already missed 3 heartbeats)
- Average case: ~90s (wait for 3 cycles)
- Worst case: ~100s

**Result:** Slightly faster average detection with much better reliability!

### Heartbeat Overhead

**Redis Operations:**
- Before: 1000 concurrent jobs × 0.025 ops/sec = **25 ops/sec**
- After: 1000 concurrent jobs × 0.1 ops/sec = **100 ops/sec**

**Impact:** Still negligible! Redis can handle 10,000+ ops/sec. This is only **1% utilization**.

### Failure Tolerance

**Scenario 1: Transient GC Pause (5 seconds)**
- Before: Lock broken ❌ (only checks once)
- After: Lock preserved ✅ (waits for 3 missed heartbeats)

**Scenario 2: Worker Process Killed**
- Before: Detected in ~130s
- After: Detected in ~90s ✅ Faster!

**Scenario 3: Network Partition (15 seconds)**
- Before: Lock broken ❌ (only checks once)
- After: Lock preserved ✅ (waits for 3 missed heartbeats = 90s)

## Algorithm Changes

### Stale Lock Detection Logic

**Before (Three Different Approaches):**
```
Different Worker Redelivery (same task_id, diff hostname):
  if lock_age < 120s:
      wait 120s for one heartbeat
      if not renewed → declare stale
  else:
      declare stale immediately

Same Worker Redelivery (same task_id, same hostname):
  if lock_age < 240s (2 × 120s):
      return False (assume still active)  ← SNAPSHOT CHECK
  else:
      declare stale immediately

Different Task (different task_id):
  Check Celery task state:  ← TASK STATE CHECK
    if SUCCESS/FAILURE → declare stale
    if STARTED → keep lock
    if PENDING → use heuristics
```

**After (Unified Heartbeat Observation):**
```
ALL Scenarios (same task_id OR different task_id):
  Calculate stale_threshold = 3 × 30s = 90s
  
  if lock_age >= 90s:
      declare stale immediately (already missed 3+ heartbeats)
  elif lock_age < 30s:
      wait for 3 heartbeat cycles (~90s total)
      if renewed → keep lock (alive)
      if not renewed → declare stale (dead)
  else:  # 30s ≤ lock_age < 90s
      wait for remaining cycles to reach 3 total
      if renewed → keep lock (alive)
      if not renewed → declare stale (dead)
```

**Key Improvements:**
- ✅ **Unified algorithm** - Same logic for all three scenarios
- ✅ **Heartbeat as ground truth** - More reliable than task state
- ✅ **Detects hung workers** - Task state can't detect this
- ✅ **Consistent behavior** - Easier to understand and maintain

### Heartbeat Loop

**Before:**
```python
max_failures = 3  # Hardcoded
```

**After:**
```python
max_failures = app.conf.get("JOB_LOCK_HEARTBEAT_MAX_FAILURES", 3)  # Configurable
```

## Consistency Fix: Unified Heartbeat Observation for All Scenarios

### Previous Inconsistencies

The old code had **three different approaches** for stale lock detection:

1. **Different worker redelivery:** Wait and observe lock renewal ✅
2. **Same worker redelivery:** Single snapshot check (is lock age < threshold?) ❌
3. **Different task_id:** Check Celery task state in result backend ❌

This was **problematic** because:
- Same worker has equal/higher risk of transient failures but used snapshot
- Task state check unreliable for hung workers (shows STARTED but heartbeat dead)
- Result backend lag causes false negatives
- Different code paths = harder to test and maintain

### New Unified Approach

**All three scenarios now use the same wait-and-observe heartbeat logic:**

1. **Same task_id, different worker** → Heartbeat observation
2. **Same task_id, same worker** → Heartbeat observation
3. **Different task_id** → Heartbeat observation (NEW!)

**How it works:**
- Calculate stale threshold: `3 × heartbeat_interval` (90s with 30s heartbeat)
- If `lock_age >= 90s`: Break immediately (already missed 3+ heartbeats)
- If `lock_age < 90s`: Wait and observe for remaining cycles
- If renewed during wait: Lock holder alive → keep lock
- If not renewed: Lock holder dead → break lock

**Benefits:**
- ✅ **Consistent behavior** - Single algorithm for all cases
- ✅ **More reliable** - Heartbeat is ground truth for worker health
- ✅ **Detects hung workers** - Task shows STARTED but no heartbeat
- ✅ **Handles transient issues** - GC pauses, network hiccups tolerated
- ✅ **Simpler code** - One logic path instead of three
- ✅ **Easier to test** - Uniform behavior across all scenarios

## Design Decision: Why Heartbeat Observation for All Cases?

### Alternative Considered: Task State Check

We could have kept using Celery task state for the different task_id case:
- ✅ **Faster** for obviously finished tasks (50ms vs 90s)
- ✅ **Definitive** for terminal states (SUCCESS/FAILURE)
- ❌ **Unreliable** for running tasks (shows STARTED but worker might be hung)
- ❌ **Result backend lag** can cause false negatives
- ❌ **Can't detect** hung workers (task shows STARTED, heartbeat stopped)

### Why We Chose Heartbeat for Everything

**Heartbeat is the ground truth for worker health:**

1. **Detects Hung Workers**
   ```
   Worker stuck in infinite loop:
   - Task state: STARTED (technically correct)
   - Heartbeat: STOPPED (no renewal for 10 minutes)
   - Task state approach: Keeps lock ❌
   - Heartbeat approach: Breaks lock after 90s ✅
   ```

2. **Handles Result Backend Lag**
   ```
   Worker crashes:
   - t=0: Worker dies
   - t=10: Task state still shows STARTED (lag)
   - t=10: Heartbeat shows no renewal for 10s
   - Heartbeat gives more current view
   ```

3. **Consistency and Simplicity**
   - One algorithm for all cases
   - Easier to test, maintain, reason about
   - Predictable behavior

4. **Conservative Trade-off**
   - Slower for finished tasks: ~90s instead of ~50ms
   - But prevents false positives from hung workers
   - Better to wait 90s than break an active lock

### Trade-off Accepted

**Cost:** Slower recovery from obviously orphaned locks (90s vs 50ms)  
**Benefit:** Reliably detects hung/stuck workers, no false positives  
**Verdict:** Reliability > Speed for stale lock detection

## Best Practices Alignment

This change aligns with industry-standard distributed systems:

- **Raft consensus:** Uses 10x heartbeat for election timeout
- **etcd lease:** Uses 3-5x heartbeat for timeout
- **Consul health checks:** Requires 3+ consecutive failures
- **Kubernetes liveness probes:** Default 3 consecutive failures

## Migration Notes

### For Existing Deployments

1. **Update celeryconfig.py** with new values (or use defaults)
2. **No code changes needed** - all thresholds are configurable
3. **Backward compatible** - old locks will expire naturally
4. **No Redis migration needed** - just config changes

### Testing Recommendations

1. **Test transient failures:** Pause worker for 45s, verify lock not broken
2. **Test real crashes:** Kill worker process, verify lock broken in ~90s
3. **Monitor Redis load:** Should remain negligible (<1% utilization)
4. **Check detection times:** Measure actual detection times in your environment

## Rollback Plan

If issues arise, rollback is simple:

```python
# Revert to old values in celeryconfig.py
JOB_LOCK_HEARTBEAT_INTERVAL = 120
JOB_LOCK_MAX_EXTENSIONS = 1080
JOB_LOCK_STALE_CHECK_RETRIES = 1  # Effectively disables multi-retry
JOB_LOCK_HEARTBEAT_MAX_FAILURES = 3  # Same default
```

## Performance Impact

✅ **Minimal CPU overhead** - Threads sleep most of the time  
✅ **Minimal memory overhead** - No additional allocations  
✅ **Minimal network overhead** - 4x more Redis ops but still negligible  
✅ **Better reliability** - Handles transient failures gracefully  
✅ **Faster detection** - 30% faster average detection time  

## Questions?

For questions or issues, refer to:
- Code: `hysds/lock.py`
- Tests: `test/test_lock.py`
- Config: `configs/celery/celeryconfig.py.tmpl`


