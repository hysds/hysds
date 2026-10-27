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


def partial(total=1, **extra):
    """The HC-665 shape: a 200 msearch item whose hit count is above zero but
    whose hits list is empty (a shard's fetch phase failed or timed out)."""
    return {"hits": {"total": {"value": total}, "hits": []}, **extra}


def parse_error_item(reason="unknown query [termzz]"):
    return {"error": {"type": "parsing_exception", "reason": reason}, "status": 400}


def rejected_item():
    """A search thread pool rejection as it surfaces per-item INSIDE a 200
    msearch response -- the saturation signature under a dense cascade."""
    return {
        "error": {
            "type": "es_rejected_execution_exception",
            "reason": "rejected execution of ... search queue capacity",
        },
        "status": 429,
    }


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


class TestIsRetryableItemError(unittest.TestCase):
    """Per-item msearch errors: 400-class (parse) errors are deterministic and
    terminal; 429 search-queue rejections and 5xx are transient and re-run --
    a saturated search queue must degrade to latency, not dropped triggers."""

    def test_parse_400_not_retryable(self):
        self.assertFalse(urd._is_retryable_item_error(parse_error_item()))
        self.assertFalse(urj._is_retryable_item_error(parse_error_item()))

    def test_rejection_429_retryable(self):
        self.assertTrue(urd._is_retryable_item_error(rejected_item()))
        self.assertTrue(urj._is_retryable_item_error(rejected_item()))

    def test_5xx_retryable(self):
        item = {"error": {"type": "internal_server_error"}, "status": 503}
        self.assertTrue(urd._is_retryable_item_error(item))

    def test_missing_status_retryable(self):
        self.assertTrue(urd._is_retryable_item_error({"error": {"reason": "?"}}))


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
            # ensure_dataset_indexed is covered by TestEnsureDatasetIndexed and
            # TestAssertDocSettled; no-op here. None = the probe ran alias-wide
            # (a MagicMock return would be taken for a pinned index).
            umock.patch.object(urd, "ensure_dataset_indexed", return_value=None),
            umock.patch.object(urd, "_send_evaluation_task"),
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
        # HC-633 replica-lag fix: preference is set PER-SEARCH in each header
        # (msearch() takes NO preference kwarg -- the v3.1.7 regression), pinning
        # the same shard copy the settled-probe validated.
        for header, _body in searches:
            self.assertEqual(header.get("preference"), "ds1")
        self.assertNotIn("preference", self.grq_es.msearch.call_args.kwargs)
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
        # a parse 400 is deterministic: no individual re-run attempted
        self.grq_es.search.assert_not_called()

    def test_per_item_rejection_retried_individually(self):
        """A 429 search-queue rejection inside a 200 msearch response is
        transient: re-run just that rule's search (with backoff) instead of
        dropping its trigger -- the saturation case the old per-rule loop's
        backoff absorbed and batching removed."""
        self.mozart_es.query.return_value = [make_rule("rejected"), make_rule("good")]
        self.grq_es.msearch.return_value = [rejected_item(), hit("ds1")]
        self.grq_es.search.return_value = hit("ds1")

        self.assertTrue(self.evaluate())

        # re-run individually, preserving the shard-pinning preference
        self.assertEqual(self.grq_es.search.call_count, 1)
        self.assertEqual(self.grq_es.search.call_args.kwargs["preference"], "ds1")
        # and BOTH rules trigger
        self.assertEqual(self.trigger.call_count, 2)
        triggered = [call.args[1]["rule_name"] for call in self.trigger.call_args_list]
        self.assertEqual(triggered, ["rejected", "good"])

    def test_per_item_rejection_persistent_failure_skips_only_that_rule(self):
        """If the individual re-run also ultimately fails, only that rule is
        skipped; the rest still evaluate and trigger."""
        self.mozart_es.query.return_value = [make_rule("rejected"), make_rule("good")]
        self.grq_es.msearch.return_value = [rejected_item(), hit("ds1")]
        # giveup fires immediately on the wrapped 400 -- no real backoff sleeps
        self.grq_es.search.side_effect = wrapped_request_error()

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


class TestEvaluateUserRulesDatasetIndexPinning(unittest.TestCase):
    """HC-664: a re-ingested dataset can share its _id with a document in another
    index behind the alias. When the probe settled on the index the ingest wrote,
    every non-query_all rule is bound to that index as well as to the _id."""

    PINNED = "grq_1_l1_s1_slc-2026.10"

    def setUp(self):
        self.grq_es = umock.MagicMock()
        self.mozart_es = umock.MagicMock()
        self.trigger = umock.MagicMock()
        self.ensure = umock.MagicMock(return_value=self.PINNED)
        patches = [
            umock.patch.object(urd, "get_grq_es", return_value=self.grq_es),
            umock.patch.object(urd, "get_mozart_es", return_value=self.mozart_es),
            umock.patch.object(urd, "queue_dataset_trigger", self.trigger),
            umock.patch.object(urd, "validate_index_pattern", lambda p: True),
            umock.patch.object(urd, "ensure_dataset_indexed", self.ensure),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def test_rules_bound_to_pinned_index(self):
        self.mozart_es.query.return_value = [
            make_rule("reproc", index_pattern="grq_*_l1_s1_slc-*"),
            make_rule("everything", query_all=True),
        ]
        self.grq_es.msearch.return_value = [hit("ds1"), miss()]

        urd.evaluate_user_rules_dataset("ds1", "1", alias="grq", index=self.PINNED)

        # the producer's index reaches the probe
        self.assertEqual(self.ensure.call_args.kwargs["index"], self.PINNED)
        searches = self.grq_es.msearch.call_args.args[0]
        reproc_musts = searches[0][1]["query"]["bool"]["must"]
        self.assertIn({"term": {"_index": self.PINNED}}, reproc_musts)
        self.assertIn({"term": {"_id": "ds1"}}, reproc_musts)
        # the rule's own index_pattern is kept: a rule whose pattern does not
        # cover the pinned index still cannot fire
        self.assertEqual(searches[0][0]["index"], "grq_*_l1_s1_slc-*")
        # query_all rules are not bound to the dataset, so not to its index either
        all_musts = searches[1][1]["query"]["bool"]["must"]
        self.assertNotIn({"term": {"_index": self.PINNED}}, all_musts)
        self.assertEqual(self.trigger.call_count, 1)

    def test_no_index_binding_when_probe_fell_back(self):
        self.ensure.return_value = None
        self.mozart_es.query.return_value = [make_rule("reproc")]
        self.grq_es.msearch.return_value = [hit("ds1")]

        urd.evaluate_user_rules_dataset("ds1", "1", alias="grq", index="grq_wrong")

        body = json.dumps(self.grq_es.msearch.call_args.args[0][0][1])
        self.assertNotIn("_index", body)
        self.assertEqual(self.trigger.call_count, 1)

    def test_legacy_call_without_index(self):
        """Positional-only task payloads (older producers) keep working."""
        self.ensure.return_value = None
        self.mozart_es.query.return_value = [make_rule("reproc")]
        self.grq_es.msearch.return_value = [hit("ds1")]

        urd.evaluate_user_rules_dataset("ds1", "1")

        self.assertIsNone(self.ensure.call_args.kwargs["index"])
        self.assertEqual(self.trigger.call_count, 1)


class TestResolveDatasetIndex(unittest.TestCase):
    """The concrete index the ingest wrote, mirrored from grq2's naming."""

    def test_single_ingest_uses_grq2_answer(self):
        info = {
            "grq_index_result": {
                "success": True, "objectid": "x", "index": "grq_1_l1_s1_slc-2026.10",
            },
            "index": {"suffix": "IGNORED"},
        }
        self.assertEqual(urd.resolve_dataset_index(info), "grq_1_l1_s1_slc-2026.10")

    def test_bulk_ingest_with_suffix(self):
        # bulk path: grq_index_result holds the dataset config's raw setting
        info = {
            "grq_index_result": {"index": None},
            "index": {"suffix": "1_L1_S1_SLC-2026.10"},
            "version": "1",
            "dataset": "L1_S1_SLC",
        }
        self.assertEqual(urd.resolve_dataset_index(info), "grq_1_l1_s1_slc-2026.10")

    def test_bulk_ingest_raw_string_index_is_not_trusted(self):
        # a recognizer-configured raw value is not a grq2 answer
        info = {
            "grq_index_result": {"index": "something"},
            "version": "v1.0",
            "dataset": "L3_DISP_S1",
        }
        self.assertEqual(urd.resolve_dataset_index(info), "grq_v1.0_l3_disp_s1")

    def test_default_naming(self):
        info = {"version": "v1.0", "dataset": "L3_DISP_S1"}
        self.assertEqual(urd.resolve_dataset_index(info), "grq_v1.0_l3_disp_s1")

    def test_unresolvable(self):
        self.assertIsNone(urd.resolve_dataset_index({"id": "x", "system_version": "1"}))
        self.assertIsNone(urd.resolve_dataset_index({"index": {"aliases": ["a"]}}))

    def test_resolver_never_raises(self):
        class Broken(dict):
            def get(self, *a, **k):
                raise RuntimeError("boom")
        self.assertIsNone(urd.resolve_dataset_index(Broken(id="x")))

    def test_prefix_override(self):
        with umock.patch.object(urd.app.conf, "get", return_value="mygrq"):
            info = {"index": {"suffix": "1_X-2026.10"}}
            self.assertEqual(urd.resolve_dataset_index(info), "mygrq_1_x-2026.10")


class TestQueueDatasetEvaluation(unittest.TestCase):
    def setUp(self):
        self.task_worker = umock.MagicMock()
        p = umock.patch.object(urd.hysds, "task_worker", self.task_worker, create=True)
        p.start()
        self.addCleanup(p.stop)

    def payload(self):
        return self.task_worker.run_task.apply_async.call_args.args[0][0]

    def test_index_passed_when_resolvable(self):
        urd.queue_dataset_evaluation({
            "id": "ds1", "system_version": "1", "version": "1", "dataset": "L1_S1_SLC",
            "index": {"suffix": "1_L1_S1_SLC-2026.10"},
            "grq_index_result": {"index": None},
        })
        payload = self.payload()
        self.assertEqual(payload["args"], ["ds1", "1"])
        self.assertEqual(payload["kwargs"], {"index": "grq_1_l1_s1_slc-2026.10"})
        self.assertEqual(payload["function"],
                         "hysds.user_rules_dataset.evaluate_user_rules_dataset")

    def test_legacy_payload_when_unresolvable(self):
        urd.queue_dataset_evaluation({"id": "ds1", "system_version": "1"})
        self.assertNotIn("kwargs", self.payload())


class TestEnsureDatasetIndexed(unittest.TestCase):
    """HC-664: the probe runs on the index the ingest wrote, confirmed by a
    realtime GET, so the old copy of a re-ingested _id cannot satisfy it."""

    def setUp(self):
        self.grq_es = umock.MagicMock()
        self.settled = umock.MagicMock()
        for p in (
            umock.patch.object(urd, "get_grq_es", return_value=self.grq_es),
            umock.patch.object(urd, "assert_doc_settled", self.settled),
        ):
            p.start()
            self.addCleanup(p.stop)
        # the undecorated function: the backoff wrapper reads its limits from
        # celeryconfig, which is mocked out in this suite
        self.ensure = urd.ensure_dataset_indexed.__wrapped__

    def test_pinned_when_doc_is_in_named_index(self):
        self.grq_es.es.get.return_value = {"found": True, "_seq_no": 3}

        pinned = self.ensure("ds1", "1", "grq", index="grq_1_x-2026.10")

        self.assertEqual(pinned, "grq_1_x-2026.10")
        self.assertEqual(
            self.grq_es.es.get.call_args.kwargs["index"], "grq_1_x-2026.10"
        )
        self.assertEqual(self.settled.call_args.args[1], "grq_1_x-2026.10")

    def test_pins_the_concrete_index_the_get_answers_with(self):
        # a realtime GET through an alias answers with the concrete index
        self.grq_es.es.get.return_value = {"found": True, "_index": "grq_1_x-2026.10"}

        pinned = self.ensure("ds1", "1", "grq", index="some_alias")

        self.assertEqual(pinned, "grq_1_x-2026.10")
        self.assertEqual(self.settled.call_args.args[1], "grq_1_x-2026.10")

    def test_falls_back_to_alias_when_doc_not_found(self):
        for answer in (
            {"found": False},
            {"error": {"type": "index_not_found_exception"}, "status": 404},
        ):
            self.grq_es.es.get.return_value = answer
            with self.assertLogs(urd.logger, level="WARNING") as logs:
                pinned = self.ensure("ds1", "1", "grq", index="grq_1_x-1999.01")
            self.assertIsNone(pinned)
            self.assertEqual(self.settled.call_args.args[1], "grq")
            self.assertIn("not in index grq_1_x-1999.01", "\n".join(logs.output))

    def test_alias_probe_without_index(self):
        pinned = self.ensure("ds1", "1", "grq")
        self.assertIsNone(pinned)
        self.grq_es.es.get.assert_not_called()
        self.assertEqual(self.settled.call_args.args[1], "grq")
        musts = self.settled.call_args.kwargs["extra_must"]
        self.assertIn({"term": {"system_version.keyword": "1"}}, musts)


class TestIsPartialSearchResponse(unittest.TestCase):
    """HC-665: which empty responses cannot be trusted to mean "no match"."""

    def setUp(self):
        from hysds.es_util import is_partial_search_response, search_hits_total
        self.partial = is_partial_search_response
        self.total = search_hits_total

    def test_total_with_no_hits_is_partial(self):
        self.assertTrue(self.partial(partial(1)))

    def test_plain_miss_is_not_partial(self):
        self.assertFalse(self.partial(miss()))

    def test_hit_is_trusted_even_with_failed_shards(self):
        result = {**hit("ds1"), "_shards": {"total": 8, "failed": 2}}
        self.assertFalse(self.partial(result))

    def test_timed_out_without_hits_is_partial(self):
        self.assertTrue(self.partial({**miss(), "timed_out": True}))

    def test_failed_shards_without_hits_are_partial(self):
        self.assertTrue(self.partial({**miss(), "_shards": {"total": 8, "failed": 1}}))

    def test_failures_on_other_indices_cannot_hide_the_bound_index(self):
        result = {**miss(), "_shards": {"total": 16, "failed": 1, "failures": [
            {"index": "grq_1_slc-2026.08", "shard": 3, "reason": {}}]}}
        self.assertFalse(self.partial(result, "grq_1_slc-2026.10"))
        # unbound query: any failed shard could have held the document
        self.assertTrue(self.partial(result))
        # a failure on the bound index, or one without an index name, counts
        on_bound = {**miss(), "_shards": {"failed": 1, "failures": [
            {"index": "grq_1_slc-2026.10"}]}}
        self.assertTrue(self.partial(on_bound, "grq_1_slc-2026.10"))
        unnamed = {**miss(), "_shards": {"failed": 1, "failures": [{"shard": 0}]}}
        self.assertTrue(self.partial(unnamed, "grq_1_slc-2026.10"))

    def test_hits_total_shapes(self):
        self.assertEqual(self.total(hit("x")), 1)
        self.assertEqual(self.total({"hits": {"total": 5, "hits": []}}), 5)
        self.assertEqual(self.total({}), 0)


class TestDatasetUnresolvedRules(unittest.TestCase):
    """HC-665: a partial response, a transient failure or an exception in one
    rule must neither abort the remaining rules nor silently drop that rule's
    trigger. Unresolved rules are re-run, then re-queued (only those rules),
    with a bounded number of attempts and a loud give-up."""

    def setUp(self):
        self.grq_es = umock.MagicMock()
        self.mozart_es = umock.MagicMock()
        self.trigger = umock.MagicMock()
        self.send = umock.MagicMock()
        self.event = umock.MagicMock()
        self.ensure = umock.MagicMock(return_value=None)
        patches = [
            umock.patch.object(urd, "get_grq_es", return_value=self.grq_es),
            umock.patch.object(urd, "get_mozart_es", return_value=self.mozart_es),
            umock.patch.object(urd, "queue_dataset_trigger", self.trigger),
            umock.patch.object(urd, "validate_index_pattern", lambda p: True),
            umock.patch.object(urd, "ensure_dataset_indexed", self.ensure),
            umock.patch.object(urd, "_send_evaluation_task", self.send),
            umock.patch.object(urd, "log_custom_event", self.event),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def evaluate(self, **kwargs):
        return urd.evaluate_user_rules_dataset("ds1", "v1.1", alias="grq", **kwargs)

    def triggered(self):
        return [c.args[1]["rule_name"] for c in self.trigger.call_args_list]

    def requeued(self):
        self.assertEqual(self.send.call_count, 1)
        payload, countdown = self.send.call_args.args
        self.assertEqual(payload["args"], ["ds1", "v1.1"])
        self.assertEqual(
            payload["function"], "hysds.user_rules_dataset.evaluate_user_rules_dataset"
        )
        return payload["kwargs"], countdown

    def test_production_shape_rerun_fires_the_notify_rule(self):
        """The OPS failure: two misses, then the CNM-S notify rule comes back
        with total > 0 and no hits. 3.3.5 raised IndexError there and lost the
        notify and every rule after it."""
        self.mozart_es.query.return_value = [
            make_rule("trigger-SCIFLO_L3_DSWx_HLS_S30"),
            make_rule("trigger-send_cnm_notify_msg_L4_TROPO"),
            make_rule("trigger-send_cnm_notify_msg_L2_CSLC_S1"),
            make_rule("trigger-after"),
        ]
        self.grq_es.msearch.return_value = [miss(), miss(), partial(1), hit("ds1")]
        self.grq_es.search.return_value = hit("ds1")

        self.assertTrue(self.evaluate())

        self.assertEqual(
            self.triggered(),
            ["trigger-send_cnm_notify_msg_L2_CSLC_S1", "trigger-after"],
        )
        # re-run individually, still pinned to the probe's shard copy
        self.assertEqual(self.grq_es.search.call_count, 1)
        self.assertEqual(self.grq_es.search.call_args.kwargs["preference"], "ds1")
        self.send.assert_not_called()

    def test_still_partial_after_rerun_is_requeued_alone(self):
        self.mozart_es.query.return_value = [make_rule("notify"), make_rule("other")]
        self.grq_es.msearch.return_value = [partial(1), hit("ds1")]
        self.grq_es.search.return_value = partial(1)

        self.assertTrue(self.evaluate(index="grq_1_x-2026.10"))

        self.assertEqual(self.triggered(), ["other"])
        kwargs, countdown = self.requeued()
        self.assertEqual(
            kwargs,
            {"index": "grq_1_x-2026.10", "rule_names": ["notify"], "attempt": 1},
        )
        self.assertEqual(countdown, 60)

    def test_timed_out_and_failed_shards_are_rerun(self):
        self.mozart_es.query.return_value = [make_rule("a"), make_rule("b")]
        self.grq_es.msearch.return_value = [
            {**miss(), "timed_out": True},
            {**miss(), "_shards": {"total": 8, "failed": 1}},
        ]
        self.grq_es.search.side_effect = [hit("ds1"), miss()]

        self.evaluate()

        self.assertEqual(self.grq_es.search.call_count, 2)
        self.assertEqual(self.triggered(), ["a"])
        self.send.assert_not_called()

    def test_failed_shards_elsewhere_do_not_rerun_a_pinned_rule(self):
        self.ensure.return_value = "grq_1_slc-2026.10"
        self.mozart_es.query.return_value = [make_rule("reproc")]
        self.grq_es.msearch.return_value = [{**miss(), "_shards": {
            "total": 16, "failed": 1, "failures": [{"index": "grq_1_slc-2026.08"}]}}]

        self.evaluate(index="grq_1_slc-2026.10")

        self.grq_es.search.assert_not_called()
        self.trigger.assert_not_called()
        self.send.assert_not_called()

    def test_transient_failure_after_fallback_is_requeued(self):
        """A 429 whose individual re-run also fails transiently used to be a
        silent skip; now it is unresolved and re-queued."""
        self.mozart_es.query.return_value = [make_rule("rejected"), make_rule("good")]
        self.grq_es.msearch.return_value = [rejected_item(), hit("ds1")]
        with umock.patch.object(urd, "search_es", side_effect=ConnectionError("reset")):
            self.evaluate()

        self.assertEqual(self.triggered(), ["good"])
        kwargs, _ = self.requeued()
        self.assertEqual(kwargs["rule_names"], ["rejected"])

    def test_parse_error_stays_terminal(self):
        self.mozart_es.query.return_value = [make_rule("bad"), make_rule("good")]
        self.grq_es.msearch.return_value = [parse_error_item(), hit("ds1")]

        with self.assertLogs(urd.logger, level="INFO") as logs:
            self.evaluate()

        self.assertEqual(self.triggered(), ["good"])
        self.send.assert_not_called()
        self.assertTrue(any("errored=['bad'] unresolved=[]" in m for m in logs.output))

    def test_one_rule_raising_does_not_stop_the_rest(self):
        self.mozart_es.query.return_value = [make_rule("boom"), make_rule("good")]
        self.grq_es.msearch.return_value = [hit("ds1"), hit("ds1")]
        self.trigger.side_effect = [RuntimeError("broker gone"), None]

        self.assertTrue(self.evaluate())

        self.assertEqual(self.trigger.call_count, 2)
        kwargs, _ = self.requeued()
        self.assertEqual(kwargs["rule_names"], ["boom"])

    def test_missing_responses_are_unresolved(self):
        self.mozart_es.query.return_value = [make_rule("a"), make_rule("b")]
        self.grq_es.msearch.return_value = [hit("ds1")]

        self.evaluate()

        self.assertEqual(self.triggered(), ["a"])
        kwargs, _ = self.requeued()
        self.assertEqual(kwargs["rule_names"], ["b"])

    def test_retry_evaluates_only_the_named_rules(self):
        self.mozart_es.query.return_value = [
            make_rule("a"), make_rule("b"), make_rule("c")]
        self.grq_es.msearch.return_value = [hit("ds1")]

        self.evaluate(rule_names=["b"], attempt=1)

        searches = self.grq_es.msearch.call_args.args[0]
        self.assertEqual(len(searches), 1)
        self.assertEqual(self.triggered(), ["b"])

    def test_countdown_grows_and_is_capped(self):
        self.mozart_es.query.return_value = [make_rule("notify")]
        self.grq_es.msearch.return_value = [partial(1)]
        self.grq_es.search.return_value = partial(1)

        self.evaluate(rule_names=["notify"], attempt=1)
        self.assertEqual(self.requeued(), (
            {"index": None, "rule_names": ["notify"], "attempt": 2}, 120))

        settings = {"USER_RULES_REQUEUE_DELAY": 400, "USER_RULES_MAX_ATTEMPTS": 5}
        self.send.reset_mock()
        with umock.patch.object(
            urd.app.conf, "get", side_effect=lambda k, d=None: settings.get(k, d)
        ):
            self.evaluate(rule_names=["notify"], attempt=3)
        _, countdown = self.requeued()
        self.assertEqual(countdown, 600)

    def test_gives_up_loudly_after_the_last_attempt(self):
        self.mozart_es.query.return_value = [make_rule("notify")]
        self.grq_es.msearch.return_value = [partial(1)]
        self.grq_es.search.return_value = partial(1)
        self.event.side_effect = RuntimeError("redis down")  # must not propagate

        with self.assertLogs(urd.logger, level="ERROR") as logs:
            self.assertTrue(self.evaluate(rule_names=["notify"], attempt=2))

        self.send.assert_not_called()
        self.assertTrue(any("GAVE UP on ds1 (v1.1) after 3 attempt(s)" in m
                            for m in logs.output))
        event_type, status, event = self.event.call_args.args[:3]
        self.assertEqual((event_type, status), ("user_rules_evaluation", "gave_up"))
        self.assertEqual(event["rules"], ["notify"])

    def test_failed_requeue_gives_up(self):
        self.mozart_es.query.return_value = [make_rule("notify")]
        self.grq_es.msearch.return_value = [partial(1)]
        self.grq_es.search.return_value = partial(1)
        self.send.side_effect = ConnectionError("broker gone")

        with self.assertLogs(urd.logger, level="ERROR") as logs:
            self.evaluate()

        self.assertTrue(any("GAVE UP" in m and "requeue failed" in m
                            for m in logs.output))

    def test_failure_before_any_rule_is_decided_requeues_everything(self):
        self.mozart_es.query.return_value = [make_rule("a")]
        with umock.patch.object(urd, "_msearch", side_effect=ConnectionError("x")):
            self.assertTrue(self.evaluate(index="grq_1_x-2026.10"))
        self.trigger.assert_not_called()
        kwargs, countdown = self.requeued()
        self.assertEqual(
            kwargs, {"index": "grq_1_x-2026.10", "rule_names": None, "attempt": 1})
        self.assertEqual(countdown, 60)

    def test_probe_or_listing_failure_requeues(self):
        self.ensure.side_effect = RuntimeError("doc not yet search-visible: ds1")
        self.evaluate()
        self.assertEqual(self.requeued()[0]["rule_names"], None)

        self.ensure.side_effect = None
        self.send.reset_mock()
        self.mozart_es.query.side_effect = ConnectionError("mozart es down")
        self.evaluate(rule_names=["a"], attempt=1)
        self.assertEqual(self.requeued()[0], {
            "index": None, "rule_names": ["a"], "attempt": 2})

    def test_failure_before_any_rule_reraises_at_the_last_attempt(self):
        self.ensure.side_effect = RuntimeError("doc not yet search-visible: ds1")
        with self.assertRaises(RuntimeError):
            self.evaluate(attempt=2)
        self.send.assert_not_called()
        self.assertEqual(self.event.call_args.args[:2],
                         ("user_rules_evaluation", "gave_up"))

    def test_conf_int_ignores_non_ints(self):
        for value in (True, "5", None, umock.MagicMock()):
            with umock.patch.object(urd.app.conf, "get", return_value=value):
                self.assertEqual(urd._conf_int("USER_RULES_MAX_ATTEMPTS", 3), 3)
        with umock.patch.object(urd.app.conf, "get", return_value=5):
            self.assertEqual(urd._conf_int("USER_RULES_MAX_ATTEMPTS", 3), 5)


class TestJobUnresolvedRules(unittest.TestCase):
    """HC-665 on the job path: the same IndexError existed in
    evaluate_user_rules_job."""

    def setUp(self):
        self.mozart_es = umock.MagicMock()
        self.trigger = umock.MagicMock()
        self.requeue = umock.MagicMock()
        self.ensure = umock.MagicMock()
        patches = [
            umock.patch.object(urj, "get_mozart_es", return_value=self.mozart_es),
            umock.patch.object(urj, "queue_job_trigger", self.trigger),
            umock.patch.object(urj, "ensure_job_indexed", self.ensure),
            umock.patch.object(urj, "queue_finished_job", self.requeue),
            umock.patch.object(urj, "log_custom_event"),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def test_partial_item_rerun_fires(self):
        self.mozart_es.query.return_value = [
            make_rule("retry", job_type="hysds-io-retry"), make_rule("after")]
        self.mozart_es.msearch.return_value = [partial(1), hit("job1")]
        self.mozart_es.search.return_value = hit("job1")

        self.assertTrue(urj.evaluate_user_rules_job("job1", index="job_failed"))

        self.assertEqual(self.trigger.call_count, 2)
        self.assertEqual(self.mozart_es.search.call_args.kwargs["preference"], "job1")
        self.requeue.assert_not_called()

    def test_still_partial_requeues_with_uuid_and_index(self):
        self.mozart_es.query.return_value = [make_rule("retry")]
        self.mozart_es.msearch.return_value = [partial(1)]
        self.mozart_es.search.return_value = partial(1)

        urj.evaluate_user_rules_job("job1", index="job_failed", uuid="u1")

        self.requeue.assert_called_once_with(
            "job1", index="job_failed", uuid="u1", rule_names=["retry"],
            attempt=1, countdown=60,
        )

    def test_failure_before_any_rule_requeues_everything(self):
        self.ensure.side_effect = RuntimeError("doc not yet search-visible: job1")

        self.assertTrue(urj.evaluate_user_rules_job("job1", uuid="u1"))

        self.assertEqual(self.requeue.call_args.kwargs["rule_names"], None)
        self.assertEqual(self.requeue.call_args.kwargs["attempt"], 1)

    def test_gives_up_after_the_last_attempt(self):
        self.mozart_es.query.return_value = [make_rule("retry")]
        self.mozart_es.msearch.return_value = [partial(1)]
        self.mozart_es.search.return_value = partial(1)

        with self.assertLogs(urj.logger, level="ERROR") as logs:
            urj.evaluate_user_rules_job("job1", rule_names=["retry"], attempt=2)

        self.requeue.assert_not_called()
        self.assertTrue(any("user_rules_job GAVE UP on job1" in m for m in logs.output))


class TestQueueFinishedJob(unittest.TestCase):
    def setUp(self):
        self.task_worker = umock.MagicMock()
        p = umock.patch.object(urj.hysds, "task_worker", self.task_worker, create=True)
        p.start()
        self.addCleanup(p.stop)

    def test_first_evaluation_payload_unchanged(self):
        urj.queue_finished_job("job1", index="job_failed", uuid="u1")
        call = self.task_worker.run_task.apply_async.call_args
        self.assertEqual(
            call.args[0][0]["kwargs"], {"index": "job_failed", "uuid": "u1"}
        )
        self.assertNotIn("countdown", call.kwargs)

    def test_requeued_evaluation_payload(self):
        urj.queue_finished_job(
            "job1", index=None, uuid="u1", rule_names=["r"], attempt=1, countdown=60)
        call = self.task_worker.run_task.apply_async.call_args
        self.assertEqual(
            call.args[0][0]["kwargs"],
            {"index": None, "uuid": "u1", "rule_names": ["r"], "attempt": 1},
        )
        self.assertEqual(call.kwargs["countdown"], 60)


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

    def test_per_item_rejection_retried_individually(self):
        """Job-path mirror of the saturation case: a per-item 429 is re-run
        individually (preference preserved) instead of dropping the trigger."""
        self.mozart_es.query.return_value = [
            make_rule("rejected", job_type="hysds-io-retry"),
            make_rule("good"),
        ]
        self.mozart_es.msearch.return_value = [rejected_item(), hit("job1")]
        self.mozart_es.search.return_value = hit("job1")

        self.assertTrue(urj.evaluate_user_rules_job("job1"))

        self.assertEqual(self.mozart_es.search.call_count, 1)
        self.assertEqual(self.mozart_es.search.call_args.kwargs["preference"], "job1")
        self.assertEqual(self.trigger.call_count, 2)


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

    def test_multiple_copies_settle_on_newest_and_warn(self):
        # HC-664: the same _id in two monthly indices behind the alias. The probe
        # must see both, check the newest write, and say that it saw two.
        from hysds.es_util import logger as es_logger
        es = umock.MagicMock()
        es.search.return_value = {"hits": {"hits": [
            {"_index": "grq_1_slc-2026.08", "_seq_no": 4,
             "_source": {"@timestamp": "2026-08-22T03:10:00Z"}},
            {"_index": "grq_1_slc-2026.10", "_seq_no": 1,
             "_source": {"@timestamp": "2026-10-21T21:41:30Z"}},
        ]}}
        es.es.get.return_value = {"_seq_no": 1}
        with self.assertLogs(es_logger, level="WARNING") as logs:
            self._settled()(es, "grq", "slc-35DB-r2")
        self.assertEqual(es.es.get.call_args.kwargs["index"], "grq_1_slc-2026.10")
        out = "\n".join(logs.output)
        self.assertIn("2 documents share this _id", out)
        self.assertIn("grq_1_slc-2026.08", out)
        body = es.search.call_args.kwargs["body"]
        self.assertEqual(body["size"], 10)
        self.assertEqual(body["_source"], ["@timestamp"])

    def test_single_copy_does_not_warn(self):
        from hysds.es_util import logger as es_logger
        es = self._es(7, 7)
        with umock.patch.object(es_logger, "warning") as warn:
            self._settled()(es, "grq", "d1")
        warn.assert_not_called()


class TestClientPreferenceSignature(unittest.TestCase):
    """Signature-faithful guard for the v3.1.7 regression: the real opensearch-py
    client's msearch() rejects a `preference` kwarg (so HC-633 sets preference
    per-search in the header), whereas search() accepts it. Mocks accept any kwarg
    -- exactly how the v3.1.7 'msearch() got an unexpected keyword argument
    preference' crash slipped the suite. TypeError binds before any network call."""

    def test_real_msearch_rejects_preference_kwarg(self):
        import inspect
        try:
            import opensearchpy
        except Exception:
            self.skipTest("opensearchpy not importable")
        # other suites stub sys.modules['opensearchpy'] with a Mock (whose attrs
        # accept any kwarg) -- only run against the real client class
        if not inspect.isclass(getattr(opensearchpy, "OpenSearch", None)):
            self.skipTest("opensearchpy is stubbed in this run")
        c = opensearchpy.OpenSearch(["https://localhost:9"], timeout=1, max_retries=0)
        with self.assertRaises(TypeError):
            c.msearch(body=[{"index": "x"}, {"query": {"match_all": {}}}], preference="p")


if __name__ == "__main__":
    unittest.main()
