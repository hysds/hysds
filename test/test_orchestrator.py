"""Call-site guards for hysds/orchestrator.py, which cannot be imported
without a cluster celeryconfig, so the checks are made on the source."""
import ast
import pathlib

SRC = pathlib.Path(__file__).resolve().parents[1] / "hysds" / "orchestrator.py"


def _calls(name):
    for node in ast.walk(ast.parse(SRC.read_text())):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == name:
            yield node


def test_every_rule_requeue_is_pinned_to_an_attempt():
    """Both queue_finished_job sites (the dedup path and the failure path)
    pass uuid=, so the settle probe cannot pass on an older attempt's doc
    under the same _id."""
    calls = list(_calls("queue_finished_job"))
    assert len(calls) >= 2, "expected the dedup and failure call sites"
    for call in calls:
        assert "uuid" in {kw.arg for kw in call.keywords}, ast.unparse(call)
