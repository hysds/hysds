"""supervisord.conf.mozart: workflow programs are rendered only when WORKFLOWS_ENABLED is on."""

import configparser
import os

import jinja2
import pytest

TMPL = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                    "configs", "supervisor", "supervisord.conf.mozart")
WF_PROGRAMS = {"program:workflow_rules", "program:watchdog_workflow_run_timeouts"}


def render(**extra):
    ctx = dict(OPS_HOME="/ops", QUEUES=[], LOCALHOST_KEYFILE="k", LOCALHOST_CRT="c")
    ctx.update(extra)
    with open(TMPL) as f:
        text = jinja2.Environment().from_string(f.read()).render(**ctx)
    # supervisor interpolation (%(here)s) is not configparser's; parse raw
    cp = configparser.RawConfigParser(strict=False, interpolation=None)
    cp.read_string(text)
    return text, set(cp.sections())


@pytest.mark.parametrize("extra", [{"WORKFLOWS_ENABLED": False}, {}])
def test_workflows_off_or_missing(extra):
    _, sections = render(**extra)
    assert not (WF_PROGRAMS & sections)
    assert "program:watchdog_job_timeouts" in sections


def test_workflows_on():
    text, sections = render(WORKFLOWS_ENABLED=True)
    assert WF_PROGRAMS <= sections
    assert "program:watchdog_job_timeouts" in sections
    assert "/ops/mozart/ops/hysds/scripts/workflow_rules_consumer.py" in text
