"""Parse and validate the job-scoped ``_workflow_outputs.json`` binding file.

Implements the contract in ADR-003 (*Workflow Output Binding via Job-Scoped
`_workflow_outputs.json`*).

A workflow-participating job writes **one** file, ``_workflow_outputs.json``, at
the **job directory root** (the PGE's cwd). It is read here by **exact path** --
the way ``job_worker`` reads ``_alt_error.txt`` -- and never by ``os.walk`` the
way ``find_pge_metrics()`` locates ``pge_metrics.json``, so a job that writes
several product subdirectories stays unambiguous.

The file holds a **typed key/value list**, not a free-form dict::

    {"version": "1.0",
     "outputs": [
       {"name": "qa_score",        "value_d": 0.93},
       {"name": "qa_status",       "value_s": "PASS"},
       {"name": "needs_reprocess", "value_b": false},
       {"name": "items",           "value_s": ["a.h5", "b.h5"]}
     ]}

The producer picks the typed slot at write time, which fixes the indexed-field
count at 4 regardless of how many workflows or output names exist, and rules out
cross-workflow type conflicts.

Three rules from the ADR drive everything below:

- **Absence of the file is not an error.** Non-workflow jobs write nothing and
  are unaffected; the contract is opt-in and costs a non-participating job zero.
- **An unparseable, schema-invalid or oversized file fails the job.** Emitting
  empty bindings instead would surface as a confusing ``$.``-resolution error
  one hop later, far from the cause -- so the error messages here are the whole
  point of the rule.
- **Order is preserved.** Arrays feed scatter enumeration, and the *indexed*
  representation of an array is unordered, so consumers must resolve bindings
  from this parsed structure / the event payload, never from indexed doc values.

This module is deliberately pure: no celery, no Redis, no index access, and no
I/O beyond reading the one file.
"""

import json
import logging
import os

logger = logging.getLogger(__name__)


# file a participating job writes at the job directory root
WORKFLOW_OUTPUTS_FILE = "_workflow_outputs.json"

# ADR-003 size cap: the bindings ride a queue message, so they must stay small.
# Bindings are scalars and dataset refs only, never payloads.
MAX_WORKFLOW_OUTPUTS_SIZE = 16 * 1024

# the only schema version this parser understands
SUPPORTED_VERSION = "1.0"

# typed value slots -> the python type each accepts (scalar, or a list thereof)
VALUE_SLOTS = {
    "value_s": str,
    "value_d": (int, float),
    "value_b": bool,
}


class WorkflowOutputsError(Exception):
    """Raised when ``_workflow_outputs.json`` exists but cannot be honored.

    Per ADR-003 this is a fail-the-job condition, never a warning: a silently
    empty binding set is far worse to debug than a loud failure at the producing
    job.
    """


def get_workflow_outputs_file(job_dir):
    """Return the exact path of the binding file for a job directory."""
    return os.path.join(job_dir, WORKFLOW_OUTPUTS_FILE)


def read_workflow_outputs(job_dir):
    """Read and validate ``_workflow_outputs.json`` from a job directory root.

    :param job_dir: the job directory (the PGE's cwd)
    :returns: the list of validated binding dicts, in file order. An empty list
        means either the file is absent (the common, non-workflow case) or it
        declared no outputs.
    :raises WorkflowOutputsError: the file exists but is oversized, unparseable,
        or schema-invalid.
    """
    outputs_file = get_workflow_outputs_file(job_dir)

    # absence is not an error -- non-workflow jobs never write this file
    if not os.path.exists(outputs_file):
        return []

    size = os.path.getsize(outputs_file)
    if size > MAX_WORKFLOW_OUTPUTS_SIZE:
        raise WorkflowOutputsError(
            "{} is {} bytes, over the {} byte limit. Workflow output bindings "
            "ride a queue message, so they must be scalars and dataset "
            "references only, never payloads. Write the payload as a dataset "
            "and bind its reference instead.".format(
                outputs_file, size, MAX_WORKFLOW_OUTPUTS_SIZE
            )
        )

    with open(outputs_file) as f:
        raw = f.read()

    try:
        doc = json.loads(raw)
    except ValueError as e:
        raise WorkflowOutputsError(
            f"Failed to parse {outputs_file} as JSON: {e}"
        ) from e

    outputs = parse_workflow_outputs(doc, source=outputs_file)
    logger.info(f"Read {len(outputs)} workflow output binding(s) from {outputs_file}")
    return outputs


def parse_workflow_outputs(doc, source=WORKFLOW_OUTPUTS_FILE):
    """Validate an already-decoded ``_workflow_outputs.json`` document.

    Split out from :func:`read_workflow_outputs` so the schema rules can be
    exercised, and reused, without touching the filesystem.

    :param doc: the decoded JSON document
    :param source: where it came from, used only in error messages
    :returns: the list of validated binding dicts, in document order
    :raises WorkflowOutputsError: on any schema violation
    """
    if not isinstance(doc, dict):
        raise WorkflowOutputsError(
            f"{source} must contain a JSON object with 'version' and 'outputs' "
            f"keys, got {_type_name(doc)}."
        )

    if "version" not in doc:
        raise WorkflowOutputsError(
            f"{source} is missing the required 'version' key (expected "
            f"\"{SUPPORTED_VERSION}\")."
        )

    version = doc["version"]
    if version != SUPPORTED_VERSION:
        raise WorkflowOutputsError(
            f"{source} declares unsupported schema version {version!r}; this "
            f"platform understands {SUPPORTED_VERSION!r}."
        )

    if "outputs" not in doc:
        raise WorkflowOutputsError(
            f"{source} is missing the required 'outputs' key. Write "
            f'\'{{"version": "{SUPPORTED_VERSION}", "outputs": []}}\' if the '
            f"job has no bindings to publish."
        )

    raw_outputs = doc["outputs"]
    if not isinstance(raw_outputs, list):
        raise WorkflowOutputsError(
            f"{source}: 'outputs' must be a list of "
            f"{{'name': ..., 'value_s'|'value_d'|'value_b': ...}} entries, got "
            f"{_type_name(raw_outputs)}. A free-form dict is explicitly not the "
            f"schema (see ADR-003)."
        )

    outputs = []
    seen = set()
    for i, entry in enumerate(raw_outputs):
        binding = _parse_entry(entry, i, source)
        name = binding["name"]
        if name in seen:
            raise WorkflowOutputsError(
                f"{source}: output name {name!r} is declared more than once; "
                f"$.<node>.{name} would be ambiguous."
            )
        seen.add(name)
        outputs.append(binding)

    return outputs


def _parse_entry(entry, i, source):
    """Validate a single ``outputs`` entry and return it normalized."""
    where = f"{source}: outputs[{i}]"

    if not isinstance(entry, dict):
        raise WorkflowOutputsError(
            f"{where} must be an object with a 'name' and exactly one of "
            f"{_slot_list()}, got {_type_name(entry)}."
        )

    if "name" not in entry:
        raise WorkflowOutputsError(f"{where} is missing the required 'name' key.")

    name = entry["name"]
    if not isinstance(name, str) or name == "":
        raise WorkflowOutputsError(
            f"{where}: 'name' must be a non-empty string, got {name!r}."
        )

    slots = [slot for slot in VALUE_SLOTS if slot in entry]
    if not slots:
        unknown = sorted(k for k in entry if k != "name")
        raise WorkflowOutputsError(
            f"{where} ({name!r}) declares no typed value slot. Exactly one of "
            f"{_slot_list()} is required; found {unknown or 'nothing'}."
        )
    if len(slots) > 1:
        raise WorkflowOutputsError(
            f"{where} ({name!r}) declares {len(slots)} typed value slots "
            f"({', '.join(sorted(slots))}); exactly one is allowed, since the "
            f"producer picks the slot at write time."
        )

    slot = slots[0]
    value = entry[slot]
    _validate_value(value, slot, name, where)

    return {"name": name, slot: value}


def _validate_value(value, slot, name, where):
    """Check a typed slot's value: a scalar, or an ordered list of scalars."""
    expected = VALUE_SLOTS[slot]

    if isinstance(value, list):
        # arrays need no mapping change (every OpenSearch field is implicitly
        # multi-valued), but their order is meaningful -- a scatter enumerates
        # by index -- so consumers must read them from here, not from the index.
        for j, element in enumerate(value):
            if not _is_slot_type(element, slot, expected):
                raise WorkflowOutputsError(
                    f"{where} ({name!r}): {slot}[{j}] is {_type_name(element)}, "
                    f"but {slot} accepts {_slot_desc(slot)} or a list thereof. "
                    f"Use the slot matching the value's type."
                )
        return

    if not _is_slot_type(value, slot, expected):
        raise WorkflowOutputsError(
            f"{where} ({name!r}): {slot} is {_type_name(value)}, but {slot} "
            f"accepts {_slot_desc(slot)} or a list thereof. Use the slot "
            f"matching the value's type."
        )


def _is_slot_type(value, slot, expected):
    """Type check one element against a typed slot.

    ``bool`` is a subclass of ``int`` in python, so a boolean has to be kept out
    of ``value_d`` explicitly -- otherwise ``true`` would silently index as 1.0.
    """
    if slot != "value_b" and isinstance(value, bool):
        return False
    return isinstance(value, expected)


def _slot_list():
    return ", ".join(f"'{slot}'" for slot in VALUE_SLOTS)


def _slot_desc(slot):
    return {
        "value_s": "a string",
        "value_d": "a number",
        "value_b": "a boolean",
    }[slot]


def _type_name(value):
    if value is None:
        return "null"
    return {
        bool: "a boolean",
        int: "a number",
        float: "a number",
        str: "a string",
        list: "a list",
        dict: "an object",
    }.get(type(value), f"a {type(value).__name__}")
