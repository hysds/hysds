import json
import sys
import unittest

try:
    import unittest.mock as umock
except ImportError:
    from unittest import mock as umock

# hysds.celery searches for configuration on import. So we need to make sure we
# mock it out before the first time it is imported
sys.modules["hysds.celery"] = umock.MagicMock()

import elasticsearch.exceptions  # noqa: E402

import hysds.user_rules_dataset as urd  # noqa: E402
import hysds.user_rules_job as urj  # noqa: E402


def hit(_id, source=None):
    return {
        "hits": {"total": {"value": 1}, "hits": [{"_id": _id, "_source": source or {}}]}
    }


def miss():
    return {"hits": {"total": {"value": 0}, "hits": []}}


def parse_error_item(reason="unknown query [termzz]"):
    return {"error": {"type": "parsing_exception", "reason": reason}, "status": 400}


def make_rule(name, query=None, **kwargs):
    doc = {
        "rule_name": name,
        "query_string": query if isinstance(query, str) else json.dumps(
            query if query is not None else {"term": {"tag": name}}
        ),
        "enabled": True,
        "job_type": kwargs.pop("job_type", "hysds-io-test_job"),
        "priority": 0,
        "query_all": kwargs.pop("query_all", False),
    }
    doc.update(kwargs)
    return {"_source": doc}


def request_error():
    return elasticsearch.exceptions.RequestError(
        400, "parsing_exception", {"error": "unknown query [termzz]"}
    )


def wrapped_request_error():
    """The exception production actually raises: the hysds_commons jittered-backoff
    connection wrapper catches the RequestError and re-raises it as a generic
    exception chained via __cause__ (raise JitteredBackoffException(...) from e).
    The propagating exception is therefore NOT a RequestError instance."""
    try:
        raise request_error()
    except Exception as orig:
        try:
            raise RuntimeError("Exception occurred: RequestError(400, ...)") from orig
        except Exception as wrapped:
            return wrapped


class TestIsRequestError(unittest.TestCase):
    """The fallback hinges on detecting a 400 even when the client library has
    wrapped it -- this is the case the unit suite previously missed and the live
    cluster surfaced (JitteredBackoffException wrapping the real RequestError)."""

    def test_bare_request_error_detected(self):
        self.assertTrue(urd._is_request_error(request_error()))
        self.assertTrue(urj._is_request_error(request_error()))

    def test_wrapped_request_error_detected(self):
        self.assertTrue(urd._is_request_error(wrapped_request_error()))
        self.assertTrue(urj._is_request_error(wrapped_request_error()))

    def test_unrelated_exception_not_detected(self):
        self.assertFalse(urd._is_request_error(ValueError("nope")))
        self.assertFalse(urd._is_request_error(RuntimeError("plain")))


class TestEvaluateUserRulesDataset(unittest.TestCase):
    def setUp(self):
        self.grq_es = umock.MagicMock()
        self.mozart_es = umock.MagicMock()
        self.trigger = umock.MagicMock()
        patches = [
            umock.patch.object(urd, "get_grq_es", return_value=self.grq_es),
            umock.patch.object(urd, "get_mozart_es", return_value=self.mozart_es),
            umock.patch.object(urd, "queue_dataset_trigger", self.trigger),
            umock.patch.object(urd, "validate_index_pattern", lambda p: True),
            # ensure_dataset_indexed is covered by TestAssertDocSettled; no-op here
            umock.patch.object(urd, "ensure_dataset_indexed"),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def evaluate(self):
        return urd.evaluate_user_rules_dataset("ds1", "v1.0", alias="grq")

    def test_response_rule_pairing_and_trigger_submission(self):
        """Responses pair positionally with rules; only hits submit triggers."""
        self.mozart_es.query.return_value = [
            make_rule("rule-a"),
            make_rule("rule-b"),
            make_rule("rule-c", query_all=True),
        ]
        self.grq_es.msearch.return_value = [hit("ds1"), miss(), hit("other-ds")]

        self.assertTrue(self.evaluate())

        # rule-a and rule-c triggered, rule-b did not
        self.assertEqual(self.trigger.call_count, 2)
        triggered = [call.args[1]["rule_name"] for call in self.trigger.call_args_list]
        self.assertEqual(triggered, ["rule-a", "rule-c"])
        # job name derives from job_type with hysds-io- prefix stripped
        self.assertEqual(self.trigger.call_args_list[0].args[2], "test_job-ds1")

        # one msearch round trip carrying one (header, body) pair per rule
        searches = self.grq_es.msearch.call_args.args[0]
        self.assertEqual(len(searches), 3)
        for header, body in searches:
            self.assertIn("index", header)
            self.assertEqual(body["size"], 1)
        # HC-633 replica-lag fix: the rule msearch must be pinned to the objectid
        # so it hits the same shard copy the settled-probe validated
        self.assertEqual(self.grq_es.msearch.call_args.kwargs["preference"], "ds1")
        # non-query_all rules are constrained to the dataset _id; query_all is not
        rule_a_filters = json.dumps(searches[0][1])
        rule_c_filters = json.dumps(searches[2][1])
        self.assertIn('{"_id": "ds1"}', rule_a_filters)
        self.assertNotIn('{"_id": "ds1"}', rule_c_filters)

    def test_per_item_error_does_not_block_other_rules(self):
        """A per-item msearch error skips that rule and evaluates the rest."""
        self.mozart_es.query.return_value = [make_rule("bad"), make_rule("good")]
        self.grq_es.msearch.return_value = [parse_error_item(), hit("ds1")]

        self.assertTrue(self.evaluate())

        self.assertEqual(self.trigger.call_count, 1)
        self.assertEqual(self.trigger.call_args.args[1]["rule_name"], "good")

    def test_request_parse_400_falls_back_to_per_rule_searches(self):
        """A rule with unparseable query DSL 400s the whole msearch request;
        evaluation must fall back to per-rule searches so the bad rule cannot
        block the rest. Production wraps the RequestError (JitteredBackoffException
        chained via __cause__), so the fallback must trigger on the wrapped form."""
        self.mozart_es.query.return_value = [make_rule("good"), make_rule("bad")]
        self.grq_es.msearch.side_effect = wrapped_request_error()
        self.grq_es.search.side_effect = [hit("ds1"), wrapped_request_error()]

        self.assertTrue(self.evaluate())

        self.assertEqual(self.grq_es.search.call_count, 2)
        self.assertEqual(self.trigger.call_count, 1)
        self.assertEqual(self.trigger.call_args.args[1]["rule_name"], "good")

    def test_malformed_json_rule_skipped_at_build(self):
        """A rule whose query_string is not valid JSON is skipped before the
        msearch is built."""
        self.mozart_es.query.return_value = [
            make_rule("malformed", query="this is not json"),
            make_rule("good"),
        ]
        self.grq_es.msearch.return_value = [hit("ds1")]

        self.assertTrue(self.evaluate())

        searches = self.grq_es.msearch.call_args.args[0]
        self.assertEqual(len(searches), 1)
        self.assertEqual(self.trigger.call_args.args[1]["rule_name"], "good")

    def test_invalid_index_pattern_falls_back_to_alias(self):
        """Empty/invalid index_pattern falls back to the dataset alias."""
        self.mozart_es.query.return_value = [
            make_rule("custom", index_pattern="grq_v1.0_l2_*"),
            make_rule("fallback", index_pattern=""),
        ]
        self.grq_es.msearch.return_value = [miss(), miss()]

        self.assertTrue(self.evaluate())

        searches = self.grq_es.msearch.call_args.args[0]
        self.assertEqual(searches[0][0]["index"], "grq_v1.0_l2_*")
        self.assertIs(searches[1][0]["index"], urd.DATASET_ALIAS)

    def test_no_enabled_rules_short_circuits(self):
        """No enabled rules: return without issuing any search."""
        self.mozart_es.query.return_value = []

        self.assertTrue(self.evaluate())

        self.grq_es.msearch.assert_not_called()
        self.trigger.assert_not_called()


class TestEvaluateUserRulesJob(unittest.TestCase):
    def setUp(self):
        self.mozart_es = umock.MagicMock()
        self.trigger = umock.MagicMock()
        patches = [
            umock.patch.object(urj, "get_mozart_es", return_value=self.mozart_es),
            umock.patch.object(urj, "queue_job_trigger", self.trigger),
            # ensure_job_indexed is covered by TestAssertDocSettled; no-op here
            umock.patch.object(urj, "ensure_job_indexed"),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def test_response_rule_pairing_and_default_job_name(self):
        self.mozart_es.query.return_value = [
            make_rule("retry-rule", job_type="hysds-io-retry"),
            make_rule("notify-rule"),
        ]
        self.mozart_es.msearch.return_value = [hit("job1"), miss()]

        self.assertTrue(urj.evaluate_user_rules_job("job1"))

        self.assertEqual(self.trigger.call_count, 1)
        self.assertEqual(self.trigger.call_args.args[2], "retry-job1")

    def test_job_name_path_extracts_name_from_matched_doc(self):
        self.mozart_es.query.return_value = [
            make_rule("retry-rule", job_type="hysds-io-retry",
                      job_name_path="_source.job.name"),
        ]
        self.mozart_es.msearch.return_value = [
            hit("job1", {"job": {"name": "cool_product"}})
        ]

        self.assertTrue(urj.evaluate_user_rules_job("job1"))

        self.assertEqual(self.trigger.call_args.args[2], "retry-cool_product")

    def test_request_parse_400_falls_back_to_per_rule_searches(self):
        # production wraps the RequestError (JitteredBackoffException via __cause__)
        self.mozart_es.query.return_value = [make_rule("good"), make_rule("bad")]
        self.mozart_es.msearch.side_effect = wrapped_request_error()
        self.mozart_es.search.side_effect = [hit("job1"), wrapped_request_error()]

        self.assertTrue(urj.evaluate_user_rules_job("job1"))

        self.assertEqual(self.mozart_es.search.call_count, 2)
        self.assertEqual(self.trigger.call_count, 1)
        self.assertEqual(self.trigger.call_args.args[1]["rule_name"], "good")


class TestAssertDocSettled(unittest.TestCase):
    """Refresh-visibility guard for the field-UPDATE race that dropped forward
    DISP-S1 products once the head sleep was removed: a complete cycle-state-config
    (is_complete flipped false->true) whose searchable copy lagged the trigger, so
    the rule query saw stale state and missed. An _id existence check passes on the
    stale version; assert_doc_settled requires the searchable _seq_no to catch up to
    the realtime (translog) _seq_no."""

    @staticmethod
    def _es(search_seq_no, get_seq_no, hits=True):
        es = umock.MagicMock()
        es.search.return_value = {
            "hits": {"hits": [{"_index": "grq_1_x", "_seq_no": search_seq_no}] if hits else []}
        }
        es.es.get.return_value = {"_seq_no": get_seq_no}
        return es

    def _settled(self):
        from hysds.es_util import assert_doc_settled
        return assert_doc_settled

    def test_settled_when_searchable_caught_up(self):
        # searchable _seq_no == latest -> the update is visible -> no raise
        self._settled()(self._es(7, 7), "grq", "csc-20180326")

    def test_raises_when_searchable_stale(self):
        # searchable copy still on the pre-update version -> backoff retries
        with self.assertRaises(Exception):
            self._settled()(self._es(5, 7), "grq", "csc-20180302")

    def test_raises_when_not_search_visible(self):
        # new-doc ingest race: not in search yet -> backoff retries
        with self.assertRaises(Exception):
            self._settled()(self._es(None, None, hits=False), "grq", "brand-new")

    def test_settled_when_seq_no_unavailable(self):
        # older ES without seq_no -> degrade to existence-only (legacy behavior)
        self._settled()(self._es(None, None), "grq", "legacy")

    def test_passes_extra_must_into_query(self):
        es = self._es(7, 7)
        self._settled()(es, "grq", "d1", extra_must=[{"term": {"system_version.keyword": "v1.0"}}])
        body = es.search.call_args.kwargs["body"]
        self.assertTrue(body.get("seq_no_primary_term"))
        musts = body["query"]["bool"]["must"]
        self.assertIn({"term": {"system_version.keyword": "v1.0"}}, musts)

    def test_settled_probe_pins_preference_to_doc_id(self):
        # HC-633 replica-lag fix: the settled-probe must route by doc_id so it
        # validates the SAME shard copy the rule msearch (also preference=doc_id)
        # will query. Same string in both -> same copy.
        es = self._es(7, 7)
        self._settled()(es, "grq", "csc-20180302")
        self.assertEqual(es.search.call_args.kwargs["preference"], "csc-20180302")


if __name__ == "__main__":
    unittest.main()
