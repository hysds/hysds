# Phase 2 Implementation: STAC Processing with Synthetic GRQ Records

## Overview
This document details the implementation of Phase 2 of the STAC integration plan, which includes STAC catalog processing, synthetic GRQ record creation, S3 upload integration, and STAC-FastAPI indexing.

## Implementation Status
- **Started**: 2025-08-28
- **Phase**: 2 (STAC Processing with Synthetic GRQ Records)
- **Estimated Completion**: Week 2-3 of implementation plan

## Components Implemented

### 1. Modified Dataset Publishing Logic
**File**: `hysds/dataset_ingest_bulk.py`
**Changes**:
- Modified `publish_datasets()` to check for STAC processing flag
- Added `process_stac_workflow()` for STAC catalog handling
- Added `process_traditional_workflow()` to maintain existing functionality
- Implemented conditional processing based on `stac_output` job-spec flag

### 2. STAC Processor Module
**File**: `hysds/stac_processor.py` (new)
**Functions**:
- `process_stac_catalog()`: Main STAC catalog processing with per-item GRQ records
- `create_grq_dataset_from_stac_item()`: Convert STAC Items to GRQ-compatible records
- `resolve_asset_urls()`: Convert relative asset hrefs to absolute S3 URLs
- `upload_catalog_to_s3()`: Upload catalog directory using existing HySDS infrastructure
- `index_collections_to_stac_api()`: Index STAC collections to STAC-FastAPI
- `index_items_to_stac_api()`: Index STAC items to STAC-FastAPI
- `update_job_metrics_for_stac()`: Update job metrics with STAC API URLs

### 3. GRQ Dataset Creation Strategy
**Approach**: Per-Item Granularity
- Each STAC Item creates individual GRQ dataset record
- Collection ID becomes GRQ `dataset_type` for proper categorization
- STAC properties copied as metadata fields
- Asset URLs point to S3 locations (not STAC API URLs)
- Standard HySDS fields maintained for ecosystem compatibility

### 4. S3 Upload Integration  
**Integration**: Uses existing HySDS Osaka infrastructure
- Synthetic recognizer for `stac_catalog` dataset type
- Leverages existing S3 credentials and profiles
- Directory-level upload for entire catalog structure
- Asset URL resolution to absolute S3 paths after upload

### 5. STAC-FastAPI Indexing
**Method**: Standard STAC API endpoints
- Uses PUT requests for idempotent upserts
- Collections: `PUT /collections/{collection_id}`
- Items: `PUT /collections/{collection_id}/items/{item_id}`
- Optional API key authentication via X-Api-Key header
- Configurable timeouts and batch processing

## Technical Details

### STAC Workflow Processing Logic
1. Job has `stac_output: true` in job-spec → STAC workflow triggered
2. Search job directory for `catalog.json` files
3. Validate each catalog using strict PySTAC validation
4. Verify all STAC item assets exist as files
5. Upload entire catalog directory to S3
6. Process STAC items in configurable batches (default: 1000)
7. Create GRQ dataset record for each STAC item
8. Index collections and items to STAC-FastAPI
9. Update job metrics with STAC API URLs

### Memory Efficiency Features
- Iterator-based item processing (`catalog.get_items(recursive=True)`)
- Configurable batch sizes for large catalogs
- Progressive processing to handle massive catalogs
- Clear batch tracking with logging

### GRQ Dataset Record Format
```python
{
    "id": "{stac_item.id}",
    "dataset": "stac_item",
    "dataset_type": "{stac_item.collection_id}",  # Collection → dataset_type
    "dataset_level": "item",
    "version": "v1.0", 
    "label": "STAC Item: {stac_item.id}",
    
    # Spatial/temporal from STAC
    "location": "{stac_item.geometry}",
    "starttime": "{stac_item.datetime}",
    "endtime": "{stac_item.datetime}",
    
    # Asset URLs (S3 locations)
    "urls": ["{asset.href}"],  # Absolute S3 URLs
    
    # STAC-specific metadata
    "stac_version": "1.0.0",
    "stac_item_id": "{stac_item.id}", 
    "stac_collection": "{stac_item.collection_id}",
    
    # All STAC properties as metadata
    **stac_item.properties,
    
    # Standard HySDS fields
    "creation_timestamp": "{utc_now}",
    "system_version": "hysds-1.3.8"
}
```

### Job Metrics Updates
- `products_staged` contains STAC API URLs (as requested)
- Format: `{STAC_API_URL}/collections/{collection_id}/items/{item_id}`
- Standard HySDS product tracking maintained
- Dataset type and path information preserved

### Error Handling Strategy
- Fail-fast on STAC validation errors
- Asset file existence verification
- STAC API connectivity validation  
- S3 upload error handling
- Comprehensive error logging with context

## Integration Points

### HySDS Ecosystem Compatibility
- **Tosca/Figaro**: Can search STAC-derived GRQ records using standard faceted search
- **Mozart Triggers**: Dataset rules fire for each STAC item based on dataset_type  
- **Job Metrics**: Standard product tracking via `products_staged`
- **Osaka Integration**: Uses existing S3 upload infrastructure

### STAC Standards Compliance
- Full STAC 1.0+ specification compliance
- Standard STAC API endpoints (no custom bulk operations)
- Proper collection and item relationships
- Asset URL resolution and validation

## Security Considerations

### API Security
- Optional STAC API key authentication
- Configurable API timeouts
- Header-based authentication (X-Api-Key)

### Data Security
- S3 credentials managed through existing HySDS profiles
- Asset file validation prevents path traversal
- Error messages sanitized to avoid information leakage

## Performance Optimizations

### Scalability Features
- Batch processing for large catalogs (configurable batch size)
- Iterator-based processing for memory efficiency
- Parallel S3 uploads using existing infrastructure
- Progressive indexing with status logging

### Monitoring
- Batch completion logging
- Processing time tracking
- Error rate monitoring
- Resource usage visibility

## Backward Compatibility

### Guaranteed Compatibility
- Traditional `dataset.json` workflow unchanged
- Jobs without `stac_output` flag use existing logic
- All existing HySDS tools work with STAC-derived data
- Standard GRQ dataset format maintained

## Testing Strategy

### Unit Tests
- STAC item to GRQ dataset conversion
- Asset URL resolution logic
- S3 upload integration
- STAC API indexing

### Integration Tests  
- End-to-end STAC catalog processing
- Mixed output scenarios (STAC + traditional)
- Error handling and recovery
- Large catalog processing

## Next Steps (Phase 3)
1. Comprehensive error handling and rollback mechanisms
2. Dataset configuration extensions
3. Mixed output support refinements
4. Production hardening and monitoring

## Files Created/Modified
- `hysds/stac_processor.py`: New STAC processing module
- `hysds/dataset_ingest_bulk.py`: Modified for conditional STAC processing
- `.claude/implementations/phase2_stac_processing_grq_records.md`: This document

## Commit Strategy
Phase 2 will be committed as logically grouped changes:
1. Dataset publishing modifications (dataset_ingest_bulk.py)
2. STAC processor module (stac_processor.py core functions)
3. S3 and STAC-FastAPI integration (indexing functions)
4. Documentation (this implementation document)