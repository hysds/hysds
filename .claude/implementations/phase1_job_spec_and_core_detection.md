# Phase 1 Implementation: Job-Spec Configuration & Core Detection

## Overview
This document details the implementation of Phase 1 of the STAC integration plan, which includes job-spec configuration extensions, global STAC configuration setup, and core STAC detection functions.

## Implementation Status
- **Started**: 2025-08-28
- **Phase**: 1 (Job-Spec Configuration & Core Detection)
- **Estimated Completion**: Week 1 of implementation plan

## Components Implemented

### 1. Job-Spec Schema Extension
**Rationale**: Since HySDS doesn't appear to have a centralized JSON schema file, we'll document the expected job-spec format and validate it programmatically in the code.

**Changes**:
- Document `stac_output` boolean field in job specifications
- Add validation logic in utility functions
- Default value: `false` (maintains backward compatibility)

### 2. Global STAC Configuration
**File**: `configs/celery/celeryconfig.py.tmpl`
**Changes Added**:
```python
# STAC API Configuration
STAC_API_URL = "{{ STAC_API_URL | default('') }}"
STAC_API_KEY = "{{ STAC_API_KEY | default('') }}"
STAC_API_TIMEOUT = {{ STAC_API_TIMEOUT | default(30) }}
STAC_VALIDATION_STRICT = {{ STAC_VALIDATION_STRICT | default(True) }}
STAC_BATCH_SIZE = {{ STAC_BATCH_SIZE | default(1000) }}
```

### 3. Core STAC Detection Functions
**File**: `hysds/utils.py`
**Functions Added**:
- `get_job_stac_enabled(job)`: Check if job has STAC output enabled
- `find_stac_catalogs(work_dir)`: Search for and validate STAC catalog.json files
- `validate_stac_assets_exist(catalog, catalog_dir)`: Validate STAC item assets exist

### 4. STAC Exception Classes
**File**: `hysds/utils.py`
**Classes Added**:
- `STACValidationError`: STAC validation failures
- `STACAPIError`: STAC API communication failures  
- `AssetMissingError`: STAC asset file missing errors

### 5. Dependencies
**File**: `requirements.in`
**Added**: `pystac>=1.13.0` for STAC processing capabilities

## Technical Details

### Job-Spec Format Extension
Jobs that want STAC processing must include:
```json
{
  "job_type": "my_stac_processor",
  "stac_output": true,
  "payload": {...}
}
```

### STAC Detection Logic
1. Check job-spec for `stac_output: true` flag
2. If enabled, scan job output directory for `catalog.json` files
3. Validate each catalog using PySTAC strict validation
4. Verify all referenced asset files exist
5. Fail fast on any validation errors

### Error Handling Strategy
- **Fail-fast**: Any STAC validation error fails the entire job
- **Strict validation**: Full STAC spec compliance required
- **Asset verification**: All item assets must exist as files
- **Clear error messages**: Specific error details for debugging

## Testing Strategy

### Unit Tests
- Test job STAC enablement detection
- Test STAC catalog discovery and validation
- Test asset existence validation
- Test exception handling

### Integration Points
- Validate with existing HySDS job processing pipeline
- Ensure backward compatibility with traditional dataset.json workflow
- Test configuration template parsing

## Backward Compatibility

### Guaranteed Compatibility
- Jobs without `stac_output` field continue using traditional workflow
- No changes to existing dataset.json processing
- All existing HySDS functionality preserved
- Optional configuration with sensible defaults

### Migration Path
- No migration required for existing jobs
- New jobs can opt-in to STAC processing via job-spec flag
- Gradual adoption possible

## Security Considerations

### Configuration Security
- STAC API keys handled as sensitive configuration
- Template-based configuration prevents hardcoded secrets
- Optional authentication for STAC API endpoints

### Validation Security
- Strict STAC validation prevents malformed data injection
- File existence checks prevent path traversal attacks
- Error messages don't leak sensitive file system information

## Next Steps (Phase 2)
1. Implement STAC catalog processing with GRQ record creation
2. Add S3 upload integration for STAC catalogs
3. Create STAC-FastAPI indexing functionality
4. Add comprehensive error handling and rollback

## Files Modified
- `configs/celery/celeryconfig.py.tmpl`: Added STAC configuration section
- `hysds/utils.py`: Added STAC detection functions and exception classes
- `requirements.in`: Added pystac dependency

## Commit Strategy
Phase 1 will be committed as logically grouped changes:
1. Configuration and dependencies (celeryconfig.py.tmpl + requirements.in)
2. Core STAC utilities (utils.py functions and exceptions)
3. Documentation (this implementation document)