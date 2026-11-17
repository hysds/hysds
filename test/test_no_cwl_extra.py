"""hysds without hysds_commons' optional ``cwl`` extra (cwltool, cwl-utils).

In a fresh interpreter with ``sys.modules['cwltool'] = sys.modules['cwl_utils'] = None`` (so any
``import cwltool`` raises ImportError, as on an install without the extra), the celery worker modules,
job_worker and the workflow engine, reaper and rules consumer must all import: none of them needs CWL
(only mozart's workflow registration compiles CWL).
"""
import json
import os
import subprocess
import sys
import textwrap

REPO = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

MODULES = [
    "hysds.job_worker",
    "hysds.orchestrator",
    "hysds.user_rules_job",
    "hysds.workflow_state",
    "hysds.workflow_engine",
    "hysds.workflow_reaper",
    "hysds.workflow_rules",
]

# the celeryconfig keys these modules read at import time (never used: no broker, Redis or OpenSearch)
CELERYCONFIG = textwrap.dedent("""
    JOBS_ES_ENGINE = "elasticsearch"
    JOBS_ES_URL = "http://127.0.0.1:1"
    GRQ_ES_URL = "http://127.0.0.1:1"
    WORKER_MOUNT_BLACKLIST = []
    WORKER_CONTIGUOUS_FAILURE_THRESHOLD = 10
    WORKER_CONTIGUOUS_FAILURE_TIME = 5.0
    USER_RULES_JOB_INDEX = "user_rules-mozart"
    USER_RULES_JOB_QUEUE = "user_rules_job"
    USER_RULES_TRIGGER_QUEUE = "user_rules_trigger"
    USER_RULES_DATASET_INDEX = "user_rules-grq"
    USER_RULES_DATASET_QUEUE = "user_rules_dataset"
    JOBS_PROCESSED_QUEUE = "jobs_processed"
    DATASET_PROCESSED_QUEUE = "dataset_processed"
    ON_DEMAND_JOB_QUEUE = "on_demand_job"
    ON_DEMAND_DATASET_QUEUE = "on_demand_dataset"
    PROCESS_EVENTS_TASKS_QUEUE = "process_events_tasks"
    DATASET_ALIAS = "grq"
""")


def test_hysds_modules_import_without_cwl_extra(tmp_path):
    (tmp_path / "celeryconfig.py").write_text(CELERYCONFIG)
    script = textwrap.dedent("""
        import importlib, json, sys
        sys.modules["cwltool"] = None
        sys.modules["cwl_utils"] = None
        out = {"ok": [], "failed": {}}
        for name in %r:
            try:
                importlib.import_module(name)
                out["ok"].append(name)
            except Exception as e:
                out["failed"][name] = "%%s: %%s" %% (type(e).__name__, e)
        out["cwl_loaded"] = sorted(m for m in sys.modules if "cwl" in m and sys.modules[m] is not None)
        print("RESULT " + json.dumps(out))
    """ % MODULES)
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(tmp_path), REPO, os.environ.get("PYTHONPATH", "")]))
    p = subprocess.run([sys.executable, "-c", script], cwd=str(tmp_path), env=env, capture_output=True,
                       text=True, timeout=120)
    assert p.returncode == 0, p.stdout + p.stderr
    out = json.loads([x for x in p.stdout.splitlines() if x.startswith("RESULT ")][-1][len("RESULT "):])
    assert out["failed"] == {}, out
    assert out["ok"] == MODULES
    assert out["cwl_loaded"] == [], out
