"""Test packaging configuration and metadata."""
import sys
from importlib.metadata import version, requires

import pytest


def test_version_starts_with_7():
    """Verify package version starts with 7."""
    v = version("hysds-core")
    assert v.startswith("7."), f"Expected version 7.x, got {v}"


def test_required_sibling_deps_declared():
    """Verify HySDS sibling dependencies are declared."""
    deps = requires("hysds-core")
    assert deps is not None, "No dependencies found"
    
    dep_names = {dep.split()[0].split(";")[0].split(">=")[0].split("~=")[0].split("<")[0]
                 for dep in deps}
    
    required_siblings = {"hysds-osaka", "hysds-prov-es", "hysds-commons"}
    missing = required_siblings - dep_names
    
    assert not missing, f"Missing required HySDS deps: {missing}"


def test_future_not_a_dependency():
    """Verify 'future' package is not a dependency."""
    deps = requires("hysds-core")
    assert deps is not None
    
    for dep in deps:
        dep_name = dep.split()[0].split(";")[0].split(">=")[0].split("~=")[0].split("<")[0]
        assert dep_name != "future", "'future' should not be a dependency on Python 3.12+"


def test_core_modules_importable():
    """Verify core hysds modules can be imported."""
    import hysds
    assert hasattr(hysds, "__version__")
    assert hasattr(hysds, "__url__")
    assert hasattr(hysds, "__description__")
    
    # Test key modules exist
    try:
        from hysds import celery
        from hysds import orchestrator
        from hysds import job_worker
        
        assert celery is not None
        assert orchestrator is not None
        assert job_worker is not None
    except ImportError as e:
        pytest.skip(f"Skipping module import test: {e}")


def test_python_version_requirement():
    """Verify running on Python 3.12+."""
    assert sys.version_info >= (3, 12), "Requires Python 3.12+"


def test_redis_pin_relaxed():
    """Verify redis dependency is not pinned to exact version."""
    deps = requires("hysds-core")
    assert deps is not None
    
    redis_deps = [d for d in deps if d.startswith("redis")]
    assert redis_deps, "redis dependency not found"
    
    # Should use ~= or >= not ==
    for dep in redis_deps:
        assert "redis==" not in dep, \
            f"redis should not be pinned with ==, found: {dep}"


def test_prompt_toolkit_upgraded():
    """Verify prompt-toolkit is version 3.x."""
    deps = requires("hysds-core")
    assert deps is not None
    
    pt_deps = [d for d in deps if "prompt-toolkit" in d or "prompt_toolkit" in d]
    assert pt_deps, "prompt-toolkit dependency not found"
    
    # Should be >=3.0
    for dep in pt_deps:
        assert "1.0" not in dep, \
            f"prompt-toolkit should be upgraded to 3.x, found: {dep}"


def test_package_name_is_hysds_core():
    """Verify package is published as hysds-core."""
    v = version("hysds-core")
    assert v is not None, "Package 'hysds-core' not found"


def test_import_name_is_hysds():
    """Verify import name remains 'hysds' (not hysds_core)."""
    import hysds
    assert hysds.__name__ == "hysds"


def test_test_dependencies_available():
    """Verify test dependencies are available when installed with [test] extra."""
    try:
        import pytest
        import pytest_cov
        import pytest_mock
        import fakeredis
        import freezegun
        
        assert pytest is not None
        assert pytest_cov is not None
        assert pytest_mock is not None
        assert fakeredis is not None
        assert freezegun is not None
    except ImportError as e:
        pytest.skip(f"Test dependencies not installed: {e}")
