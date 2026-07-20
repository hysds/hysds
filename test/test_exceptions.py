import pickle
import sys
import unittest.mock as umock

import pytest

# hysds.celery searches for configuration on import, so mock it before any
# hysds import. setdefault, not bare assignment: module-scope sys.modules
# mutation is the cross-test-pollution pattern that forced test_lock.py into
# its own pytest process in CI.
sys.modules.setdefault("hysds.celery", umock.MagicMock())
# hysds.orchestrator pulls hysds.es_util transitively, which imports
# opensearchpy at module scope.
sys.modules.setdefault("opensearchpy", umock.MagicMock())
sys.modules.setdefault("opensearchpy.exceptions", umock.MagicMock())

from celery.utils.serialization import get_pickleable_exception  # noqa: E402

from hysds.job_worker import JobDedupedError, WorkerExecutionError  # noqa: E402
from hysds.orchestrator import OrchestratorExecutionError  # noqa: E402

EXC_CLASSES = [WorkerExecutionError, OrchestratorExecutionError]
JOB_STATUS = {"status": "job-failed", "error": "SoftTimeLimitExceeded()"}


@pytest.mark.parametrize("cls", EXC_CLASSES)
def test_pickle_round_trip(cls):
    """AC1: cls(*args) reconstruction must succeed."""
    exc = cls("boom", JOB_STATUS)
    restored = pickle.loads(pickle.dumps(exc))
    assert type(restored) is cls
    assert str(restored) == "boom"


@pytest.mark.parametrize("cls", EXC_CLASSES)
def test_args_carry_message_only(cls):
    """AC2: args carries only the message (keeps tracebacks small)."""
    exc = cls("boom", JOB_STATUS)
    assert exc.args == ("boom",)
    assert "job-failed" not in repr(exc)


@pytest.mark.parametrize("cls", EXC_CLASSES)
def test_job_status_optional_and_defaults_none(cls):
    """AC1: single-arg construction is what pickle performs."""
    exc = cls("boom")
    assert exc.job_status is None
    assert exc.message == "boom"


@pytest.mark.parametrize("cls", EXC_CLASSES)
def test_job_status_attribute_preserved(cls):
    """AC3: attribute access still works after removing the dead method."""
    exc = cls("boom", JOB_STATUS)
    assert exc.job_status == JOB_STATUS
    assert not callable(exc.job_status)


@pytest.mark.parametrize("cls", EXC_CLASSES)
def test_celery_does_not_wrap(cls):
    """AC4: this is the regression test for the bug itself."""
    exc = cls("boom", JOB_STATUS)
    safe = get_pickleable_exception(exc)
    assert type(safe) is cls
    assert "UnpickleableExceptionWrapper" not in repr(safe)


def test_job_deduped_error_attribute_not_callable():
    """AC3: JobDedupedError shares the dead-method cleanup."""
    exc = JobDedupedError("dup")
    assert exc.job_status == "job-deduped"
    assert not callable(exc.job_status)
    restored = pickle.loads(pickle.dumps(exc))
    assert type(restored) is JobDedupedError
