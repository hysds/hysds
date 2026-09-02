# HC-598: Remove duplicate context field from job_status documents

## Summary

This PR removes the duplicate top-level `context` field from job_status documents indexed to OpenSearch. Previously, job_status documents contained both a top-level `context` field and `job.context` field with identical content. This duplication contributed to OpenSearch heap pressure and increased storage requirements.

By removing the duplicate field, we achieve an **average 29.5% reduction in document sizes** across all job types.

## Related Tickets

- **[HC-598](https://hysds-core.atlassian.net/browse/HC-598)** - HySDS: Remove duplicate context field from job_status documents
- **[HC-599](https://hysds-core.atlassian.net/browse/HC-599)** - Chimera: Update context field path to use job.context (companion PR)

## Changes

### File: `hysds/log_utils.py`

```diff
+    # Remove duplicate top-level 'context' (identical to 'job.context') before indexing.
+    # Original job dict is preserved for Redis key/status operations.
+    job_for_indexing = copy.deepcopy(job)
+    if "context" in job_for_indexing:
+        del job_for_indexing["context"]
+
-    job["resource"] = "job"
-    job["type"] = job.get("job", {}).get("type", "unknown")
+    job_for_indexing["resource"] = "job"
+    job_for_indexing["type"] = job_for_indexing.get("job", {}).get("type", "unknown")
     ...
-    r.rpush(app.conf.REDIS_JOB_STATUS_KEY, msgpack.dumps(job))  # for ES
+    r.rpush(app.conf.REDIS_JOB_STATUS_KEY, msgpack.dumps(job_for_indexing))  # for ES
```

**Key changes:**
- Create deep copy of job dict for indexing operations
- Remove top-level `context` from the copy (duplicate of `job.context`)
- Use original job dict for Redis key/status operations
- Use optimized copy for OpenSearch indexing

## Backward Compatibility

This change modifies the structure of **newly indexed** job_status documents:

| Field | Old Documents | New Documents |
|-------|---------------|---------------|
| `context` | Present | **Removed** |
| `job.context` | Present | Present |

**Important:** The companion PR [HC-599](https://hysds-core.atlassian.net/browse/HC-599) updates chimera to read from `job.context` instead of the top-level `context` field. This chimera update is backward compatible and should be deployed **before or simultaneously** with this HySDS change.

## Test Artifacts

### Test Environment Setup

To validate this change, two identical NISAR SDS clusters were provisioned using the `dev-e2e-pge-golden_dataset-post-RSO` cluster provisioner from nisar-pcm. This provisioner runs a comprehensive end-to-end test that processes a golden dataset through all NISAR science pipelines (L0A, L0B, RSLC, GCOV, GSLC, INSAR, L3_SM).

| Cluster | HySDS Version | Chimera Version | Description |
|---------|---------------|-----------------|-------------|
| **New Cluster** | HC-598 branch | HC-599 branch | With duplicate context removal |
| **Old Cluster** | v1.4.0 | v2.3.0 | Baseline (no changes) |

Both clusters:
- Used the same terraform provisioner and configuration
- Processed the same golden dataset inputs
- Ran the same set of pipelines and jobs
- Allow for apples-to-apples comparison of job_status and GRQ documents

### 1. Document Structure Verification

Verified that new job_status documents no longer contain the top-level `context` field:

**New cluster (with HC-598):**
```
Has top-level context: False
Has job.context: True
```

**Old cluster (without HC-598):**
```
Has top-level context: True
Has job.context: True
```

### 2. Document Size Reduction

Jobs were matched between clusters by normalizing job names (removing branch tags like `NSDS-4646` vs `NSDS-4677` and timestamps). This ensures we compare the exact same jobs processing the same data.

Comparison of job_status document sizes based on **3,019 matched jobs**:

| Job Type | Matches | Avg Old (bytes) | Avg New (bytes) | Reduction |
|----------|--------:|----------------:|----------------:|----------:|
| L3_SM_Evaluator | 12 | 285,541 | 192,369 | 32.6% |
| GCOV_GSLC_Evaluator | 31 | 269,840 | 181,991 | 32.5% |
| Network_Pair_Evaluator | 30 | 267,883 | 180,823 | 32.5% |
| Datatake_Accountability | 1 | 539,550 | 366,248 | 32.1% |
| SCIFLO_GCOV | 32 | 328,473 | 222,960 | 32.1% |
| Track_Frame_Accountability | 9 | 249,994 | 170,013 | 32.0% |
| SCIFLO_GSLC | 28 | 314,605 | 214,443 | 31.8% |
| send_notify_msg | 177 | 365,954 | 248,748 | 31.3% |
| DC_Radar_Evaluator | 12 | 243,318 | 168,571 | 30.8% |
| SCIFLO_INSAR | 5 | 379,624 | 269,671 | 29.0% |
| SCIFLO_L0B | 16 | 115,909 | 82,133 | 28.8% |
| RRST_Accountability | 854 | 38,539 | 27,654 | 28.2% |
| SCIFLO_L0A_TE | 3 | 670,946 | 484,856 | 27.7% |
| SCIFLO_L3_SM | 14 | 69,945 | 51,145 | 26.9% |
| INGEST_STAGED | 880 | 37,828 | 27,882 | 26.2% |
| SCIFLO_RSLC | 40 | 75,068 | 59,137 | 21.2% |
| timer_handler | 13 | 11,223 | 9,010 | 19.7% |
| purge_ISL | 862 | 11,394 | 9,199 | 19.2% |
| **OVERALL** | **3,019** | **64,772** | **45,664** | **29.5%** |

### 3. GRQ (Dataset) Documents Unaffected

This change only affects job_status documents in Mozart. GRQ product documents (L0A, L0B, RSLC, GCOV, GSLC, etc.) are not impacted as they do not contain a duplicate context field. Comparison of matching GRQ documents between clusters showed **no size difference**, confirming the change is isolated to job_status indexing.

### 4. Job Execution Verification

<!-- SCREENSHOT REQUEST: Provide a screenshot of Figaro showing successful job completions after this change -->
**Screenshot: Figaro job list showing successful job completions**

### 5. Pipeline Execution

<!-- SCREENSHOT REQUEST: Provide a screenshot showing a complete pipeline run (e.g., L0A or L0B pipeline) -->
**Screenshot: Successful pipeline execution (SCIFLO workflow)**

### 6. OpenSearch Document Structure

<!-- SCREENSHOT REQUEST: Provide a screenshot of OpenSearch Dev Tools showing a job_status document WITHOUT the top-level context field -->
**Screenshot: OpenSearch Dev Tools showing new job_status document structure**

## Deployment Notes

### Deployment Order

1. **Deploy chimera (HC-599) first** - Updates chimera to read from `job.context`
2. **Deploy HySDS (HC-598)** - Removes the duplicate `context` field

This order ensures no disruption because:
- The chimera update is backward compatible (reads from `job.context` which exists in both old and new documents)
- Once chimera is updated, it no longer depends on the top-level `context` field
- Then HySDS can safely stop including the duplicate field

### Rollback

If rollback is needed:
- Revert HySDS change first (to restore duplicate context field)
- Chimera change can remain (it reads from `job.context` which is always present)

## Impact

- **Storage reduction**: ~30% smaller job_status documents
- **OpenSearch heap reduction**: Less memory required for field data cache
- **Query performance**: Potentially faster queries with smaller documents
- **No functional changes**: All job data remains available via `job.context`

## Checklist

- [x] Code changes are minimal and focused
- [x] Companion PR (HC-599) created for chimera
- [x] Backward compatible deployment path documented
- [x] No security implications
- [x] Performance improvement verified (29.5% document size reduction)
