"""Supervisord templates (HC-672).

On a single machine the four component templates are rendered into one process
manager (SINGLE_MACHINE_CLUSTER), so their program names must be unique, and in
that mode they must not repeat the shared sections of the main file.
"""

import os
import re

import jinja2
import pytest

TMPL_DIR = os.path.join(os.path.dirname(__file__), "..", "configs", "supervisor")
GOLDEN_DIR = os.path.join(os.path.dirname(__file__), "data", "supervisor")
COMPONENTS = ["mozart", "grq", "metrics", "factotum"]
SHARED_SECTIONS = [
    "[unix_http_server]",
    "[inet_http_server]",
    "[supervisord]",
    "[rpcinterface:supervisor]",
    "[supervisorctl]",
]
CONTEXT = dict(
    OPS_HOME="/home/ops",
    LOCALHOST_KEYFILE="/home/ops/ssl/server.key",
    LOCALHOST_CRT="/home/ops/ssl/server.pem",
    MOZART_RABBIT_USER="guest",
    MOZART_RABBIT_PASSWORD="pw",
    MOZART_RABBIT_PVT_IP="10.0.0.1",
    VENUE="v",
    QUEUES=[{"QUEUE_NAME": "q-small "}, {"QUEUE_NAME": "q-large", "TOTAL_JOBS_METRIC": True}],
)


def render(name, **extra):
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(TMPL_DIR))
    return env.get_template(f"supervisord.conf.{name}").render(**CONTEXT, **extra)


def programs(text):
    return re.findall(r"^\[program:([^\]]+)\]$", text, flags=re.M)


@pytest.mark.parametrize("comp", COMPONENTS)
def test_default_render_matches_golden(comp):
    """Unset SINGLE_MACHINE_CLUSTER renders what the template always has (plus the renamed programs)."""
    with open(os.path.join(GOLDEN_DIR, f"{comp}.conf")) as f:
        assert render(comp) == f.read()
    assert render(comp, SINGLE_MACHINE_CLUSTER=False) == render(comp)


@pytest.mark.parametrize("single", [False, True])
def test_program_names_unique_across_components(single):
    names = [n for comp in COMPONENTS for n in programs(render(comp, SINGLE_MACHINE_CLUSTER=single))]
    assert len(names) == len(set(names)), sorted(n for n in set(names) if names.count(n) > 1)


@pytest.mark.parametrize("comp", COMPONENTS)
def test_single_machine_render(comp):
    text = render(comp, SINGLE_MACHINE_CLUSTER=True)
    for section in SHARED_SECTIONS:
        assert section not in text
    groups = re.findall(r"^\[group:([^\]]+)\]\nprograms=(.*)$", text, flags=re.M)
    assert [g[0] for g in groups] == [comp]
    assert groups[0][1].split(",") == programs(text)
    # code lives in ~/mozart/ops on a single machine
    for other in ("sciflo", "metrics", "verdi"):
        assert f"/home/ops/{other}/ops/" not in text


def test_single_machine_keeps_component_config_and_logs():
    """Config stays in the component's own folder (job specs mount ~/verdi/etc); logs follow %(here)s."""
    text = render("factotum", SINGLE_MACHINE_CLUSTER=True)
    assert 'HYSDS_DATASETS_CFG="/home/ops/verdi/etc/datasets.json"' in text
    assert "%(here)s/../log/" in text
    assert "/home/ops/mozart/ops/hysds" in text


def test_single_machine_main_file_includes_each_component():
    text = render("single_machine")
    for section in SHARED_SECTIONS:
        assert section in text
    assert "[program:" not in text
    for d in ("mozart", "sciflo", "metrics", "verdi"):
        assert f"/home/ops/{d}/etc/programs.conf" in text
