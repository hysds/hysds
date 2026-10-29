"""HC-664 / HC-665 against a real OpenSearch.

Skipped unless HYSDS_TEST_OS_URL points at a disposable single-node cluster
without auth, for example:

    docker run -d --name hysds-os -p 127.0.0.1:19200:9200 \\
      -e discovery.type=single-node -e DISABLE_SECURITY_PLUGIN=true \\
      -e DISABLE_INSTALL_DEMO_CONFIG=true \\
      -e "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m" \\
      opensearchproject/opensearch:3.6.0
    HYSDS_TEST_OS_URL=http://localhost:19200 pytest -s test/integration

The race behind HC-664 is held open deterministically: the index the
re-ingest writes to has refresh disabled until the test refreshes it. Only
indices, a template and an alias named hysds_it_<random>* are created, and
they are deleted afterwards.
"""

import os
import sys
import threading
import time
import unittest.mock as umock
import uuid

import pytest

URL = os.environ.get("HYSDS_TEST_OS_URL")
if not URL:
    pytest.skip("HYSDS_TEST_OS_URL is not set", allow_module_level=True)

# hysds.celery reads celeryconfig on import; mock it as the unit suites do
sys.modules["hysds.celery"] = umock.MagicMock()

from opensearchpy import OpenSearch  # noqa: E402
from hysds_commons.opensearch_utils import OpenSearchUtility  # noqa: E402

import hysds.es_util as es_util  # noqa: E402
import hysds.log_utils as log_utils  # noqa: E402
import hysds.user_rules_dataset as urd  # noqa: E402

PREFIX = f"hysds_it_{uuid.uuid4().hex[:8]}"
ALIAS = f"{PREFIX}_alias"
PATTERN = f"{PREFIX}_*_slc-*"
OLD = f"{PREFIX}_1_slc-2026.08"
NEW = f"{PREFIX}_1_slc-2026.10"
DOC_ID = "S1C_IW_SLC__1SDV_20260821T015155_20260821T015210_009087_01209F_35DB-r2"

REPROC_ONLY = (
    '{"bool": {"must": [{"term": {"dataset_type.keyword": "L1_S1_SLC"}}, '
    '{"term": {"metadata.processing_mode": "reprocessing"}}]}}'
)
ANY_MODE = '{"bool": {"must": [{"term": {"dataset_type.keyword": "L1_S1_SLC"}}]}}'


def rule(name, query):
    return {
        "_source": {
            "rule_name": name,
            "query_string": query,
            "enabled": True,
            "job_type": "hysds-io-echo:test",
            "priority": 0,
            "query_all": False,
            "index_pattern": PATTERN,
            "kwargs": "{}",
            "queue": "q",
        }
    }


def doc(mode, timestamp):
    return {
        "id": DOC_ID,
        "objectid": DOC_ID,
        "system_version": "1",
        "dataset_type": "L1_S1_SLC",
        "@timestamp": timestamp,
        "metadata": {"processing_mode": mode},
    }


@pytest.fixture(scope="module")
def client():
    # the real hysds_commons wrapper (search/msearch/query as production runs
    # them) around a plain client: the stock constructor forces TLS and reads
    # ~/.netrc-os
    util = OpenSearchUtility.__new__(OpenSearchUtility)
    util.es = OpenSearch([URL], timeout=30)
    util.version, util.engine, util.flavor = None, "opensearch", None
    util.es.indices.put_index_template(
        name=PREFIX,
        body={
            "index_patterns": [f"{PREFIX}_*"],
            "template": {
                "settings": {"number_of_shards": 2, "number_of_replicas": 0},
                "mappings": {
                    "dynamic_templates": [
                        {
                            "strings": {
                                "match_mapping_type": "string",
                                "mapping": {
                                    "type": "text",
                                    "fields": {
                                        "keyword": {
                                            "type": "keyword",
                                            "ignore_above": 256,
                                        }
                                    },
                                },
                            }
                        }
                    ]
                },
                "aliases": {ALIAS: {}},
            },
        },
    )
    yield util
    util.es.indices.delete(index=f"{PREFIX}_*", ignore=[404])
    util.es.indices.delete_index_template(name=PREFIX, ignore=[404])


@pytest.fixture
def same_id_reingest(client):
    """The OPS state: the forward-ingested copy in last month's index (visible),
    and the re-ingested copy with the same _id in this month's index, written
    but not yet refreshed."""
    es = client.es
    es.indices.delete(index=f"{PREFIX}_1_*", ignore=[404])
    es.indices.create(index=OLD)
    es.indices.create(index=NEW, body={"settings": {"index.refresh_interval": "-1"}})
    es.index(
        index=OLD,
        id=DOC_ID,
        body=doc("forward", "2026-08-22T03:10:00Z"),
        refresh="true",
    )
    es.index(index=NEW, id=DOC_ID, body=doc("reprocessing", "2026-10-21T21:41:30Z"))
    # the new copy is in the translog but not searchable
    assert es.get(index=NEW, id=DOC_ID)["found"] is True
    assert es.count(index=NEW)["count"] == 0
    return es


@pytest.fixture
def evaluator(client):
    mozart = umock.MagicMock()
    mocks = {
        "mozart": mozart,
        "trigger": umock.MagicMock(),
        "requeue": umock.MagicMock(),
        "event": umock.MagicMock(),
    }
    patches = [
        umock.patch.object(urd, "get_grq_es", return_value=client),
        umock.patch.object(urd, "get_mozart_es", return_value=mozart),
        umock.patch.object(urd, "queue_dataset_trigger", mocks["trigger"]),
        umock.patch.object(urd, "_send_evaluation_task", mocks["requeue"]),
        umock.patch.object(urd, "log_custom_event", mocks["event"]),
    ]
    for p in patches:
        p.start()
    # the settled probe's real backoff, kept short. celeryconfig is a MagicMock
    # here: set and restore by hand (patch.object would delete the attributes
    # on stop, and a deleted MagicMock attribute raises for later test files)
    conf = log_utils.app.conf
    saved = {k: getattr(conf, k) for k in ("BACKOFF_MAX_TRIES", "BACKOFF_MAX_VALUE")}
    conf.BACKOFF_MAX_TRIES, conf.BACKOFF_MAX_VALUE = 20, 1
    yield mocks
    for k, v in saved.items():
        setattr(conf, k, v)
    for p in patches:
        p.stop()


def set_rules(mocks, *rules):
    """The evaluator rewrites each rule's query_string in place, so hand it
    fresh rule documents on every listing, as Mozart's ES does."""
    mocks["mozart"].query.side_effect = lambda **kwargs: [
        rule(name, query) for name, query in rules
    ]


def evaluate(index=None):
    return urd.evaluate_user_rules_dataset(DOC_ID, "1", alias=ALIAS, index=index)


def triggered_docs(mocks):
    return [
        (
            c.args[1]["rule_name"],
            c.args[0]["_index"],
            c.args[0]["_source"]["metadata"]["processing_mode"],
        )
        for c in mocks["trigger"].call_args_list
    ]


def test_legacy_alias_probe_drops_the_trigger(same_id_reingest, evaluator):
    """The 3.3.5 behaviour, still what a payload without `index` gets: the
    alias-wide probe settles on the old copy at once and the rule misses."""
    set_rules(evaluator, ("reproc-only", REPROC_ONLY))

    assert evaluate() is True

    assert triggered_docs(evaluator) == []
    evaluator["requeue"].assert_not_called()


def test_pinned_probe_waits_for_the_new_copy(same_id_reingest, evaluator):
    set_rules(evaluator, ("reproc-only", REPROC_ONLY))
    done = threading.Event()
    errors = []

    def run():
        try:
            evaluate(index=NEW)
        except Exception as e:  # surfaced below
            errors.append(e)
        finally:
            done.set()

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    time.sleep(3)
    # still waiting: the old copy cannot satisfy a probe pinned to NEW
    assert not done.is_set()
    assert triggered_docs(evaluator) == []

    same_id_reingest.indices.refresh(index=NEW)
    assert done.wait(60), "evaluation did not finish after the refresh"
    assert not errors, errors

    assert triggered_docs(evaluator) == [("reproc-only", NEW, "reprocessing")]
    sent_rule = evaluator["trigger"].call_args.args[1]
    assert {"term": {"_index": NEW}} in sent_rule["query"]["query"]["bool"]["must"]
    evaluator["requeue"].assert_not_called()


def test_both_copies_visible(same_id_reingest, evaluator):
    """A rule both copies satisfy: the pinned evaluation is fed the new copy;
    an alias-wide probe now says it saw two."""
    same_id_reingest.indices.refresh(index=NEW)
    set_rules(evaluator, ("any-mode", ANY_MODE))

    evaluate(index=NEW)
    assert triggered_docs(evaluator) == [("any-mode", NEW, "reprocessing")]

    evaluator["trigger"].reset_mock()
    with umock.patch.object(es_util.logger, "warning") as warning:
        evaluate()
    messages = " ".join(str(c.args[0]) for c in warning.call_args_list)
    assert "2 documents share this _id" in messages
    assert OLD in messages and NEW in messages
    assert len(triggered_docs(evaluator)) == 1


def test_wrong_index_name_falls_back_to_the_alias(same_id_reingest, evaluator):
    same_id_reingest.indices.refresh(index=NEW)
    set_rules(evaluator, ("reproc-only", REPROC_ONLY))

    with umock.patch.object(urd.logger, "warning") as warning:
        evaluate(index=f"{PREFIX}_1_slc-1999.01")

    assert any("is not in index" in str(c.args[0]) for c in warning.call_args_list)
    assert triggered_docs(evaluator) == [("reproc-only", NEW, "reprocessing")]


def _strip_hits(response):
    """HC-665: keep the hit count, drop the hits (a failed fetch phase)."""
    if (response.get("hits") or {}).get("hits"):
        response["hits"]["hits"] = []
    return response


def test_partial_msearch_item_is_rerun(client, same_id_reingest, evaluator):
    same_id_reingest.indices.refresh(index=NEW)
    set_rules(evaluator, ("any-mode", ANY_MODE), ("reproc-only", REPROC_ONLY))
    real_msearch = client.msearch

    def partial_msearch(searches, **kwargs):
        return [_strip_hits(r) for r in real_msearch(searches, **kwargs)]

    with umock.patch.object(client, "msearch", side_effect=partial_msearch):
        evaluate(index=NEW)

    # 3.3.5 raised IndexError on the first rule and lost both
    assert sorted(triggered_docs(evaluator)) == [
        ("any-mode", NEW, "reprocessing"),
        ("reproc-only", NEW, "reprocessing"),
    ]
    evaluator["requeue"].assert_not_called()


def test_persistent_partial_is_requeued(client, same_id_reingest, evaluator):
    same_id_reingest.indices.refresh(index=NEW)
    set_rules(evaluator, ("any-mode", ANY_MODE), ("reproc-only", REPROC_ONLY))
    real_msearch, real_search_es = client.msearch, urd.search_es

    def partial_msearch(searches, **kwargs):
        responses = real_msearch(searches, **kwargs)
        responses[1] = _strip_hits(responses[1])
        return responses

    def partial_search_es(index, body, preference):
        return _strip_hits(
            real_search_es(index=index, body=body, preference=preference)
        )

    with umock.patch.object(
        client, "msearch", side_effect=partial_msearch
    ), umock.patch.object(urd, "search_es", side_effect=partial_search_es):
        evaluate(index=NEW)

    assert triggered_docs(evaluator) == [("any-mode", NEW, "reprocessing")]
    payload, countdown = evaluator["requeue"].call_args.args
    assert payload["kwargs"] == {
        "index": NEW,
        "rule_names": ["reproc-only"],
        "attempt": 1,
    }
    assert countdown == 60


def test_unparseable_rule_stays_terminal(same_id_reingest, evaluator):
    """A real request-parse 400 fails the whole msearch; the per-rule fallback
    isolates the bad rule, which is not re-queued."""
    same_id_reingest.indices.refresh(index=NEW)
    set_rules(evaluator, ("bad", '{"termzz": {"a": 1}}'), ("reproc-only", REPROC_ONLY))

    evaluate(index=NEW)

    assert triggered_docs(evaluator) == [("reproc-only", NEW, "reprocessing")]
    evaluator["requeue"].assert_not_called()
