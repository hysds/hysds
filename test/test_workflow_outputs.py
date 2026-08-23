"""Tests for the ADR-003 ``_workflow_outputs.json`` parser/validator.

``hysds.workflow_outputs`` is deliberately pure -- no celery, no Redis, no index
access -- so unlike most of this suite these tests need no module mocking.
"""

import json
import os

import pytest

from hysds.workflow_outputs import (
    MAX_WORKFLOW_OUTPUTS_SIZE,
    WORKFLOW_OUTPUTS_FILE,
    WorkflowOutputsError,
    get_workflow_outputs_file,
    parse_workflow_outputs,
    read_workflow_outputs,
)


@pytest.fixture
def job_dir(tmp_path):
    return str(tmp_path)


def write_outputs(job_dir, content):
    """Write ``_workflow_outputs.json`` at the job directory root."""
    path = os.path.join(job_dir, WORKFLOW_OUTPUTS_FILE)
    with open(path, "w") as f:
        if isinstance(content, str):
            f.write(content)
        else:
            json.dump(content, f)
    return path


def test_missing_file_is_not_an_error(job_dir):
    """Non-workflow jobs never write the file; the contract is opt-in."""
    assert read_workflow_outputs(job_dir) == []


def test_empty_outputs_list(job_dir):
    write_outputs(job_dir, {"version": "1.0", "outputs": []})
    assert read_workflow_outputs(job_dir) == []


def test_valid_scalar_outputs(job_dir):
    """The ADR's worked example, one entry per typed slot."""
    write_outputs(
        job_dir,
        {
            "version": "1.0",
            "outputs": [
                {"name": "qa_score", "value_d": 0.93},
                {"name": "qa_status", "value_s": "PASS"},
                {"name": "needs_reprocess", "value_b": False},
            ],
        },
    )
    assert read_workflow_outputs(job_dir) == [
        {"name": "qa_score", "value_d": 0.93},
        {"name": "qa_status", "value_s": "PASS"},
        {"name": "needs_reprocess", "value_b": False},
    ]


def test_integer_is_accepted_in_value_d(job_dir):
    write_outputs(
        job_dir, {"version": "1.0", "outputs": [{"name": "count", "value_d": 42}]}
    )
    assert read_workflow_outputs(job_dir) == [{"name": "count", "value_d": 42}]


def test_valid_array_outputs_preserve_order(job_dir):
    """Order matters: ``$.enumerate.items`` feeds a scatter by index."""
    items = ["granule_c.h5", "granule_a.h5", "granule_b.h5"]
    write_outputs(
        job_dir,
        {
            "version": "1.0",
            "outputs": [
                {"name": "items", "value_s": items},
                {"name": "scores", "value_d": [0.9, 0.1, 0.5]},
                {"name": "flags", "value_b": [True, False, True]},
            ],
        },
    )
    outputs = read_workflow_outputs(job_dir)
    assert [o["name"] for o in outputs] == ["items", "scores", "flags"]
    assert outputs[0]["value_s"] == items
    assert outputs[1]["value_d"] == [0.9, 0.1, 0.5]
    assert outputs[2]["value_b"] == [True, False, True]


def test_empty_array_is_valid(job_dir):
    write_outputs(
        job_dir, {"version": "1.0", "outputs": [{"name": "items", "value_s": []}]}
    )
    assert read_workflow_outputs(job_dir) == [{"name": "items", "value_s": []}]


def test_unicode_names_and_values(job_dir):
    write_outputs(
        job_dir,
        {
            "version": "1.0",
            "outputs": [
                {"name": "estación", "value_s": "Ñuñoa — 東京"},
                {"name": "étiquettes", "value_s": ["α", "β", "γ"]},
            ],
        },
    )
    outputs = read_workflow_outputs(job_dir)
    assert outputs[0] == {"name": "estación", "value_s": "Ñuñoa — 東京"}
    assert outputs[1]["value_s"] == ["α", "β", "γ"]


def test_read_uses_exact_path_not_a_walk(job_dir):
    """A file of the same name in a product subdir must not be picked up.

    ``job_worker`` reads ``_alt_error.txt`` by exact path; ``find_pge_metrics()``
    walks. ADR-003 is explicit that this file follows the former, so a job that
    writes several product subdirectories stays unambiguous.
    """
    product_dir = os.path.join(job_dir, "my_product")
    os.makedirs(product_dir)
    write_outputs(
        product_dir,
        {"version": "1.0", "outputs": [{"name": "decoy", "value_s": "nope"}]},
    )
    assert read_workflow_outputs(job_dir) == []

    assert get_workflow_outputs_file(job_dir) == os.path.join(
        job_dir, WORKFLOW_OUTPUTS_FILE
    )


# --- fail-the-job cases ------------------------------------------------------


def test_malformed_json_fails_the_job(job_dir):
    path = write_outputs(job_dir, '{"version": "1.0", "outputs": [')
    with pytest.raises(WorkflowOutputsError) as excinfo:
        read_workflow_outputs(job_dir)
    assert path in str(excinfo.value)
    assert "Failed to parse" in str(excinfo.value)


def test_oversized_file_fails_the_job(job_dir):
    """16KB cap -- the bindings ride a queue message."""
    big = "x" * MAX_WORKFLOW_OUTPUTS_SIZE
    write_outputs(
        job_dir, {"version": "1.0", "outputs": [{"name": "blob", "value_s": big}]}
    )
    with pytest.raises(WorkflowOutputsError) as excinfo:
        read_workflow_outputs(job_dir)
    msg = str(excinfo.value)
    assert str(MAX_WORKFLOW_OUTPUTS_SIZE) in msg
    assert "never payloads" in msg


def test_file_at_exactly_the_cap_is_accepted(job_dir):
    path = os.path.join(job_dir, WORKFLOW_OUTPUTS_FILE)
    doc = {"version": "1.0", "outputs": [{"name": "pad", "value_s": ""}]}
    body = json.dumps(doc)
    padding = MAX_WORKFLOW_OUTPUTS_SIZE - len(body)
    doc["outputs"][0]["value_s"] = "x" * padding
    with open(path, "w") as f:
        f.write(json.dumps(doc))
    assert os.path.getsize(path) == MAX_WORKFLOW_OUTPUTS_SIZE
    assert len(read_workflow_outputs(job_dir)) == 1


def test_top_level_must_be_an_object(job_dir):
    write_outputs(job_dir, [{"name": "qa_score", "value_d": 0.93}])
    with pytest.raises(WorkflowOutputsError, match="must contain a JSON object"):
        read_workflow_outputs(job_dir)


def test_missing_version_fails(job_dir):
    write_outputs(job_dir, {"outputs": []})
    with pytest.raises(WorkflowOutputsError, match="missing the required 'version'"):
        read_workflow_outputs(job_dir)


def test_unsupported_version_fails(job_dir):
    write_outputs(job_dir, {"version": "2.0", "outputs": []})
    with pytest.raises(WorkflowOutputsError, match="unsupported schema version"):
        read_workflow_outputs(job_dir)


def test_missing_outputs_key_fails(job_dir):
    write_outputs(job_dir, {"version": "1.0"})
    with pytest.raises(WorkflowOutputsError) as excinfo:
        read_workflow_outputs(job_dir)
    assert "missing the required 'outputs'" in str(excinfo.value)


def test_free_form_dict_instead_of_list_fails(job_dir):
    """The rejected alternative from ADR-003 must fail loudly, not coerce."""
    write_outputs(job_dir, {"version": "1.0", "outputs": {"qa_score": 0.93}})
    with pytest.raises(WorkflowOutputsError) as excinfo:
        read_workflow_outputs(job_dir)
    assert "must be a list" in str(excinfo.value)
    assert "free-form dict" in str(excinfo.value)


def test_entry_must_be_an_object(job_dir):
    write_outputs(job_dir, {"version": "1.0", "outputs": ["qa_score"]})
    with pytest.raises(WorkflowOutputsError, match=r"outputs\[0\] must be an object"):
        read_workflow_outputs(job_dir)


def test_entry_missing_name_fails(job_dir):
    write_outputs(job_dir, {"version": "1.0", "outputs": [{"value_d": 0.93}]})
    with pytest.raises(WorkflowOutputsError, match="missing the required 'name'"):
        read_workflow_outputs(job_dir)


@pytest.mark.parametrize("name", ["", 7, None, ["a"]])
def test_entry_with_bad_name_fails(job_dir, name):
    write_outputs(
        job_dir, {"version": "1.0", "outputs": [{"name": name, "value_d": 0.93}]}
    )
    with pytest.raises(WorkflowOutputsError, match="must be a non-empty string"):
        read_workflow_outputs(job_dir)


def test_entry_with_no_value_slot_fails(job_dir):
    write_outputs(
        job_dir, {"version": "1.0", "outputs": [{"name": "qa_score", "value": 0.93}]}
    )
    with pytest.raises(WorkflowOutputsError) as excinfo:
        read_workflow_outputs(job_dir)
    msg = str(excinfo.value)
    assert "declares no typed value slot" in msg
    assert "'value'" in msg  # names the unknown key the author actually used


def test_entry_with_multiple_value_slots_fails(job_dir):
    write_outputs(
        job_dir,
        {
            "version": "1.0",
            "outputs": [{"name": "qa_score", "value_d": 0.93, "value_s": "0.93"}],
        },
    )
    with pytest.raises(WorkflowOutputsError) as excinfo:
        read_workflow_outputs(job_dir)
    assert "declares 2 typed value slots" in str(excinfo.value)


@pytest.mark.parametrize(
    "entry",
    [
        {"name": "n", "value_s": 1},
        {"name": "n", "value_d": "0.93"},
        {"name": "n", "value_b": "true"},
        {"name": "n", "value_d": True},  # bool is an int subclass in python
        {"name": "n", "value_s": {"a": 1}},
        {"name": "n", "value_s": None},
    ],
)
def test_wrong_type_for_slot_fails(job_dir, entry):
    write_outputs(job_dir, {"version": "1.0", "outputs": [entry]})
    with pytest.raises(WorkflowOutputsError, match="accepts a"):
        read_workflow_outputs(job_dir)


def test_heterogeneous_array_fails_and_names_the_index(job_dir):
    write_outputs(
        job_dir,
        {"version": "1.0", "outputs": [{"name": "items", "value_s": ["a", 2, "c"]}]},
    )
    with pytest.raises(WorkflowOutputsError) as excinfo:
        read_workflow_outputs(job_dir)
    assert "value_s[1]" in str(excinfo.value)


def test_duplicate_output_names_fail(job_dir):
    write_outputs(
        job_dir,
        {
            "version": "1.0",
            "outputs": [
                {"name": "qa_score", "value_d": 0.93},
                {"name": "qa_score", "value_d": 0.11},
            ],
        },
    )
    with pytest.raises(WorkflowOutputsError, match="declared more than once"):
        read_workflow_outputs(job_dir)


# --- parse_workflow_outputs, without touching the filesystem ------------------


def test_parse_workflow_outputs_is_usable_standalone():
    doc = {"version": "1.0", "outputs": [{"name": "qa_status", "value_s": "PASS"}]}
    assert parse_workflow_outputs(doc) == [{"name": "qa_status", "value_s": "PASS"}]


def test_parse_workflow_outputs_error_names_the_source():
    with pytest.raises(WorkflowOutputsError) as excinfo:
        parse_workflow_outputs({"version": "9"}, source="/jobs/abc/_workflow_outputs.json")
    assert "/jobs/abc/_workflow_outputs.json" in str(excinfo.value)
