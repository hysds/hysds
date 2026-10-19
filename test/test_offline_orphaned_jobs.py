import importlib.util
import pathlib
import sys
import unittest.mock as umock

# hysds.celery searches for configuration on import, so mock it before any
# hysds import; setdefault to avoid cross-test pollution.
sys.modules.setdefault("hysds.celery", umock.MagicMock())
sys.modules.setdefault("future", umock.MagicMock())

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "scripts"
_spec = importlib.util.spec_from_file_location(
    "offline_orphaned_jobs", SCRIPTS / "offline_orphaned_jobs.py"
)
ooj = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ooj)


def test_guard_reads_redis_not_es():
    """Pin the guard's import provenance to hysds.log_utils (redis); the
    identically-named hysds.utils.get_job_status reads lagging ES."""
    import hysds.log_utils as lu

    assert ooj.is_job_finalized is lu.is_job_finalized
