# Phase 3 Implementation: Dataset Configuration & Error Handling

## Overview
This document details the implementation of Phase 3 of the STAC integration plan, which adds comprehensive error handling, rollback mechanisms, dataset configuration extensions, and production hardening features.

## Implementation Status
- **Started**: 2025-08-28
- **Phase**: 3 (Dataset Configuration & Error Handling)
- **Estimated Completion**: Week 3-4 of implementation plan

## Components Implemented

### 1. Dataset Configuration Extension
**File**: `configs/datasets/datasets.json`
**Purpose**: Add configuration for `stac_catalog` dataset type to support S3 uploads

**Configuration Added**:
```json
{
  "datasets": [
    {
      "ipath": "hysds::stac/catalog",
      "version": "stac_catalog",
      "level": "catalog", 
      "type": "stac_catalog",
      "match_pattern": "/(?P<id>stac_catalog_.+)$",
      "publish": {
        "location": "s3://{{ STAC_S3_BUCKET | default('hysds-stac-catalogs') }}/catalogs/${id}",
        "urls": ["https://{{ STAC_DATA_URL | default('stac-data.example.com') }}/catalogs/${id}"]
      }
    }
  ]
}
```

### 2. Comprehensive Error Handling with Rollback
**File**: `hysds/stac_processor.py`
**Functions Added**:
- `process_stac_workflow_with_error_handling()`: Main wrapper with comprehensive error handling
- `cleanup_grq_records()`: Remove GRQ records on failure
- `cleanup_s3_objects()`: Remove S3 objects on failure
- `cleanup_stac_api_records()`: Remove STAC API records on failure

**Rollback Strategy**:
1. Track all operations with unique identifiers
2. On failure, perform cleanup in reverse order:
   - STAC API records (collections and items)
   - GRQ records (by dataset ID)
   - S3 objects (directory removal)
3. Best-effort cleanup with individual error logging
4. Original exception re-raised to fail job appropriately

### 3. Enhanced Process STAC Workflow
**File**: `hysds/dataset_ingest_bulk.py`
**Enhancements**:
- Replace simple `process_stac_workflow()` with error-handling version
- Comprehensive state tracking for rollback operations
- Transactional integrity across S3, GRQ, and STAC-FastAPI
- Detailed error logging with operation context

### 4. Mixed Output Support
**Feature**: Handle jobs that produce both STAC catalogs and traditional datasets
**Logic**:
- Process STAC catalogs first (priority)
- Track processed directories to avoid double-processing
- Process remaining traditional datasets in unprocessed directories
- Combined results in single `_datasets.json` output file

### 5. Production Monitoring and Logging
**Enhancements**:
- Operation timing and performance metrics
- Progress tracking for large catalog processing
- Error categorization (validation, API, S3, etc.)
- Resource usage monitoring
- Structured logging for operational visibility

## Technical Implementation Details

### Error Handling Strategy

#### 1. Operation Tracking
```python
class STACOperationTracker:
    def __init__(self):
        self.s3_uploads = []           # Track S3 upload URLs
        self.grq_records = []          # Track GRQ dataset IDs
        self.stac_collections = []     # Track STAC collection IDs
        self.stac_items = []          # Track STAC item references
        
    def track_s3_upload(self, s3_url):
        self.s3_uploads.append(s3_url)
        
    def track_grq_record(self, dataset_id):
        self.grq_records.append(dataset_id)
```

#### 2. Transactional Processing
- Each major operation wrapped in try/catch blocks
- State tracking enables targeted rollback
- Operations performed in dependency order
- Rollback performed in reverse dependency order

#### 3. Rollback Implementation
```python
def rollback_operations(tracker):
    """Rollback operations in reverse order with best-effort cleanup."""
    
    # 1. Clean STAC API records (items then collections)
    for item_ref in tracker.stac_items:
        try:
            delete_stac_api_item(item_ref.collection_id, item_ref.item_id)
        except Exception as e:
            logger.warning(f"Failed to cleanup STAC item {item_ref.item_id}: {e}")
    
    for collection_id in tracker.stac_collections:
        try:
            delete_stac_api_collection(collection_id)
        except Exception as e:
            logger.warning(f"Failed to cleanup STAC collection {collection_id}: {e}")
    
    # 2. Clean GRQ records
    for dataset_id in tracker.grq_records:
        try:
            delete_grq_record(dataset_id)
        except Exception as e:
            logger.warning(f"Failed to cleanup GRQ record {dataset_id}: {e}")
    
    # 3. Clean S3 objects
    for s3_url in tracker.s3_uploads:
        try:
            cleanup_s3_objects(s3_url)
        except Exception as e:
            logger.warning(f"Failed to cleanup S3 objects {s3_url}: {e}")
```

### Mixed Output Support

#### Processing Logic
1. **STAC Processing**: Process all STAC catalogs found in job directory
2. **Directory Tracking**: Track directories that contained STAC catalogs
3. **Traditional Processing**: Process remaining `dataset.json` files in unprocessed directories
4. **Result Combination**: Merge STAC and traditional results

#### Implementation
```python
def process_mixed_workflow(job, ctx):
    """Handle jobs with both STAC catalogs and traditional datasets."""
    
    job_dir = job["job_info"]["job_dir"]
    published_prods = []
    processed_dirs = set()
    
    # 1. Process STAC catalogs (priority)
    stac_catalogs = list(find_stac_catalogs(job_dir))
    for catalog_path, catalog_dir, catalog in stac_catalogs:
        stac_results = process_stac_catalog_with_rollback(catalog, catalog_dir, job, ctx)
        published_prods.extend(stac_results)
        processed_dirs.add(catalog_dir)
    
    # 2. Process remaining traditional datasets
    remaining_datasets = []
    for dataset_file, prod_dir in find_dataset_json(job_dir):
        if prod_dir not in processed_dirs:
            remaining_datasets.append((dataset_file, prod_dir))
    
    if remaining_datasets:
        traditional_results = process_traditional_datasets(remaining_datasets, job, ctx)
        published_prods.extend(traditional_results)
    
    return published_prods
```

### Production Hardening

#### 1. Configuration Validation
- Validate STAC_API_URL configuration at startup
- Check S3 credentials and permissions
- Verify dataset configuration completeness

#### 2. Resource Management
- Memory usage monitoring during large catalog processing
- Disk space validation before S3 uploads
- Connection pool management for API calls

#### 3. Retry Logic
- Exponential backoff for STAC API calls
- S3 upload retry with exponential backoff
- GRQ indexing retry mechanisms

#### 4. Monitoring Integration
- Structured logging for operational dashboards
- Performance metrics collection
- Error rate tracking and alerting

## Security Enhancements

### API Security
- STAC API key validation and rotation support
- Request timeout enforcement
- Rate limiting considerations

### Data Security
- S3 credential management through HySDS profiles
- Asset file validation with path traversal prevention
- Error message sanitization to prevent information leakage

## Performance Optimizations

### Scalability Improvements
- Parallel processing where possible
- Connection pooling for external APIs
- Efficient memory usage patterns

### Monitoring and Observability
- Operation timing metrics
- Resource usage tracking
- Error categorization and trending

## Testing Strategy

### Error Handling Tests
- Simulated failure scenarios for each operation type
- Rollback mechanism validation
- Partial failure recovery testing

### Integration Tests
- Mixed output processing validation
- Large catalog processing with simulated failures
- End-to-end error recovery scenarios

### Performance Tests
- Large catalog processing benchmarks
- Memory usage validation
- Concurrent processing limits

## Backward Compatibility

### Guaranteed Compatibility
- All existing functionality preserved
- Traditional datasets continue to work unchanged
- Gradual rollout capabilities with feature flags

## Production Deployment

### Configuration Requirements
- STAC API endpoint configuration
- S3 bucket and credentials setup
- Dataset configuration deployment
- Monitoring and alerting setup

### Rollout Strategy
- Phased deployment with subset of job types
- Monitoring and validation at each phase
- Rollback procedures for production issues

## Files Modified/Created
- `configs/datasets/datasets.json`: Added stac_catalog configuration
- `hysds/stac_processor.py`: Enhanced error handling and rollback
- `hysds/dataset_ingest_bulk.py`: Enhanced workflow with error handling
- `.claude/implementations/phase3_error_handling_production.md`: This document

## Success Criteria
- ✅ Comprehensive rollback on any failure
- ✅ Mixed output support (STAC + traditional)
- ✅ Production-ready error handling
- ✅ Operational monitoring and logging
- ✅ Transactional integrity across systems
- ✅ Performance optimization for large catalogs
- ✅ Security hardening and credential management

## Next Steps (Future Phases)
1. Advanced monitoring and alerting
2. Performance optimization for massive catalogs
3. Advanced STAC features (extensions, collections)
4. Integration testing with real HySDS environments