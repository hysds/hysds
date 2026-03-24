# HySDS Core Packaging Migration Summary

## Migration Completed: March 24, 2026

This document summarizes the migration of the `hysds` repository from legacy `setup.py` to modern `pyproject.toml` packaging.

---

## Changes Made

### ✅ Files Created

1. **`pyproject.toml`** - Modern packaging configuration
   - Package name: `hysds-core` (PyPI) / `hysds` (import)
   - Version: Dynamic from git tags via `hatch-vcs`
   - Dependencies: 31 third-party packages + 3 HySDS siblings
   - Tool configs migrated from `setup.cfg`

2. **`.github/workflows/publish.yml`** - PyPI publishing automation
   - Triggered on git tags (`v*`)
   - Uses PyPI Trusted Publishers (OIDC)
   - Builds both sdist and wheel

3. **`test/test_packaging.py`** - Packaging validation tests
   - Verifies version starts with 7.x
   - Checks HySDS sibling dependencies declared
   - Validates no `future` dependency
   - Tests redis pin relaxed
   - Tests prompt-toolkit upgraded to 3.x

### ✅ Files Modified

1. **`hysds/__init__.py`**
   - Changed from hardcoded version to `importlib.metadata.version("hysds-core")`
   - Version now dynamically read from package metadata

### ✅ Files Removed

1. `setup.py` - Legacy packaging
2. `setup.cfg` - Tool configs (migrated to pyproject.toml)
3. `requirements.txt` - Legacy requirements
4. `requirements-py312.txt` - Legacy requirements
5. `requirements.in` - Legacy requirements
6. `dev-requirements.in` - Legacy dev requirements

---

## Key Dependency Changes

### Fixed Issues

| Issue | Before | After |
|-------|--------|-------|
| Redis pin too strict | `redis==5.2.1` | `redis~=5.2` |
| prompt-toolkit outdated | `prompt-toolkit==1.0.18` | `prompt-toolkit>=3.0,<4.0` |
| Sibling deps unversioned | `osaka>=0.0.1` | `hysds-osaka~=7.0` |
| Sibling deps unversioned | `prov_es>=0.2.0` | `hysds-prov-es~=7.0` |
| Sibling deps unversioned | `hysds_commons>=0.1` | `hysds-commons~=7.0` |

### Dependencies Summary

- **Total dependencies**: 34 (31 third-party + 3 HySDS siblings)
- **Test dependencies**: 6 packages in `[test]` extra
- **Dev dependencies**: 11 packages in `[dev]` extra

---

## Build Verification

```bash
$ python -m build
Successfully built hysds_core-3.2.0.post1.dev0+g62c6cb9e1.d20260324.tar.gz
Successfully built hysds_core-3.2.0.post1.dev0+g62c6cb9e1.d20260324-py3-none-any.whl

$ twine check dist/*
Checking dist/hysds_core-3.2.0.post1.dev0+g62c6cb9e1.d20260324-py3-none-any.whl: PASSED
Checking dist/hysds_core-3.2.0.post1.dev0+g62c6cb9e1.d20260324.tar.gz: PASSED
```

---

## Next Steps

### Before Publishing to PyPI

1. **Tag version 7.0.0**
   ```bash
   git tag -a v7.0.0 -m "Release 7.0.0 - Modern packaging migration"
   git push origin v7.0.0
   ```

2. **Configure PyPI Trusted Publisher**
   - Go to https://pypi.org/manage/account/publishing/
   - Add GitHub Actions publisher for `hysds/hysds` repo
   - Workflow: `publish.yml`
   - Environment: `pypi`

3. **Verify sibling packages published first**
   - ✅ `hysds-osaka~=7.0` must be on PyPI
   - ✅ `hysds-prov-es~=7.0` must be on PyPI
   - ✅ `hysds-commons~=7.0` must be on PyPI

### Installation Methods

#### Development (Local)
```bash
# Editable install (changes reflected immediately)
pip install -e .

# With test dependencies
pip install -e ".[test]"

# With dev dependencies
pip install -e ".[dev]"
```

#### Development (From Git Branch)
```bash
# Install from feature branch
pip install "git+https://github.com/hysds/hysds.git@feature-branch"
```

#### Production (After PyPI Publishing)
```bash
# Install from PyPI
pip install hysds-core

# Or as part of meta-package
pip install "hysds[mozart]"  # Includes hysds-core
```

---

## Testing

### Run Packaging Tests
```bash
cd /Users/mcayanan/git/hysds
pip install -e ".[test]"
pytest test/test_packaging.py -v
```

### Run Full Test Suite
```bash
pytest test/ -v
```

---

## Backward Compatibility

### Import Names (Unchanged)
```python
# All existing imports continue to work
from hysds.celery import app
from hysds.orchestrator import submit_job
from hysds.job_worker import run_job
import hysds
```

### Package Name Change
- **PyPI package**: `hysds` → `hysds-core`
- **Import name**: `hysds` (unchanged)
- **Reason**: Avoid confusion with meta-package

---

## Migration Checklist

- [x] Create `pyproject.toml` with all dependencies
- [x] Update `__init__.py` to use `importlib.metadata`
- [x] Add GitHub Actions workflow for PyPI publishing
- [x] Add packaging validation tests
- [x] Remove legacy `setup.py`, `setup.cfg`, `requirements*.txt`
- [x] Verify `python -m build` succeeds
- [x] Verify `twine check` passes
- [ ] Tag v7.0.0 release
- [ ] Configure PyPI Trusted Publisher
- [ ] Publish to PyPI
- [ ] Update documentation
- [ ] Notify dependent projects

---

## Troubleshooting

### Version shows as dev version
This is expected before tagging. Once you tag `v7.0.0`, the version will be `7.0.0`.

### ImportError: No module named 'hysds-core'
The package name on PyPI is `hysds-core`, but you import it as `hysds`:
```python
import hysds  # Correct
import hysds_core  # Wrong
```

### Dependencies not found during build
Ensure sibling packages are published to PyPI first:
1. `hysds-osaka`
2. `hysds-prov-es`
3. `hysds-commons`

For local development, install siblings in editable mode:
```bash
pip install -e /path/to/osaka
pip install -e /path/to/prov_es
pip install -e /path/to/hysds_commons
pip install -e /path/to/hysds
```

---

## Contact

For questions about this migration, contact the HySDS team at hysds-help@jpl.nasa.gov
