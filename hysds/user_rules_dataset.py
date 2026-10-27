from future import standard_library

standard_library.install_aliases()

import json
import socket

import backoff
import elasticsearch.exceptions
import opensearchpy.exceptions

import hysds  # avoids cyclical import
from hysds.celery import app
from hysds.es_util import assert_doc_settled, get_grq_es, get_mozart_es
from hysds.log_utils import backoff_max_tries, backoff_max_value, logger
from hysds.utils import validate_index_pattern

GRQ_ES_URL = app.conf.GRQ_ES_URL  # ES
DATASET_ALIAS = app.conf.DATASET_ALIAS
USER_RULES_DATASET_INDEX = app.conf.USER_RULES_DATASET_INDEX

JOBS_PROCESSED_QUEUE = app.conf.JOBS_PROCESSED_QUEUE  # queue names
USER_RULES_TRIGGER_QUEUE = app.conf.USER_RULES_TRIGGER_QUEUE
USER_RULES_DATASET_QUEUE = app.conf.USER_RULES_DATASET_QUEUE


@backoff.on_exception(
    backoff.expo, Exception, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def ensure_dataset_indexed(objectid, system_version, alias, index=None):
    """Ensure the dataset is indexed AND its latest write is search-visible.

    A rule can be triggered by a field UPDATE on a pre-existing doc (e.g. a
    cycle-state-config's is_complete flipping false->true). Search is refresh-bound,
    so the updated value can lag the trigger; an _id existence check alone passes on
    the stale version and the rule's filtered query then misses, dropping the
    trigger. assert_doc_settled confirms the searchable copy has caught up to the
    latest write (realtime GET for the authoritative _seq_no), bounded by the
    existing exponential backoff -- no fixed sleep, no per-eval _refresh.

    index: the concrete index the ingest wrote this dataset to (HC-664). Two docs
    can share an _id across the indices behind the alias -- monthly or rolling
    indices with stable product ids, e.g. a re-ingest of an aged-out product --
    and an alias-wide probe is then satisfied at once by the OLD copy while the
    new one is not refreshed yet; the rule query that follows sees only the old
    copy and misses. A realtime GET on `index` (translog-served, so independent
    of refresh) confirms the write landed there; the probe then searches that
    index only, so only the new copy can satisfy it. When the GET does not find
    the doc (the producer named the wrong index), fall back to the alias as
    before and say so.

    Returns the concrete index the probe was pinned to, or None when it ran on
    the alias. The caller binds its rule queries to the same index.
    """
    logger.info(f"ensure_dataset_indexed: {objectid} ({system_version}) index={index}")
    try:
        grq_es = get_grq_es()
        probe_index, pinned = alias, None
        if index:
            # ignore=[404]: a missing doc AND a missing index both answer 404;
            # neither has "found": true, so both fall back to the alias.
            latest = grq_es.es.get(index=index, id=objectid, ignore=[404])
            if isinstance(latest, dict) and latest.get("found") is True:
                # the GET answers with the concrete index even when `index`
                # names an alias; bind everything to that
                pinned = latest.get("_index") or index
                probe_index = pinned
            else:
                logger.warning(
                    f"{objectid} ({system_version}) is not in index {index} named by "
                    f"the producer; probing {alias} instead"
                )
        assert_doc_settled(
            grq_es,
            probe_index,
            objectid,
            extra_must=[{"term": {"system_version.keyword": system_version}}],
        )
        logger.info(
            f"Found settled indexed dataset: {objectid} ({system_version}) "
            f"in {probe_index}"
        )
        return pinned
    except (
        elasticsearch.exceptions.ElasticsearchException,
        opensearchpy.exceptions.OpenSearchException,
    ) as e:
        logger.error(e)
        raise e


def update_query(_id, system_version, rule, index=None):
    """
    takes the rule's query_string and adds system version and dataset's id to "filter" in "bool"
    :param _id: ES's _id
    :param system_version: string/int, system_version field in ES document
    :param rule: dict
    :param index: concrete index the triggering document was written to; when given,
        a non-query_all rule is bound to that index as well as to the _id (HC-664)
    :return: dict
    """
    updated_query = json.loads(rule["query_string"])
    filts = [updated_query, {"term": {"system_version.keyword": system_version}}]

    # will add _id if query all False
    if rule.get("query_all", False) is False:
        filts.append({"term": {"_id": _id}})
        if index:
            # Another document with this _id can sit in another index matched by
            # the rule's index_pattern (monthly indices, stable product ids). Bind
            # the rule to the copy the ingest wrote, so the other copy can neither
            # satisfy the rule nor stand in for the new one (HC-664).
            filts.append({"term": {"_index": index}})

    final_query = {"query": {"bool": {"must": filts}}}
    logger.info(f"Final query: {json.dumps(final_query)}")
    return final_query


def _is_request_error(e):
    """True if e is (or wraps) an elasticsearch/opensearchpy RequestError (HTTP 400).
    Query parse 400s are deterministic; retrying cannot change the outcome.

    The hysds_commons jittered-backoff connection wrapper re-raises the original
    client error as JitteredBackoffException chained via __cause__ (raise ... from e),
    so the propagating exception is NOT the RequestError itself -- walk the cause
    chain to find it. Resolved via isinstance per module so a stubbed-out client
    library (as in unit test environments) degrades to False rather than breaking."""
    seen = set()
    while e is not None and id(e) not in seen:
        seen.add(id(e))
        for exceptions_module in (elasticsearch.exceptions, opensearchpy.exceptions):
            cls = getattr(exceptions_module, "RequestError", None)
            if isinstance(cls, type) and issubclass(cls, BaseException) and isinstance(e, cls):
                return True
        e = getattr(e, "__cause__", None) or getattr(e, "__context__", None)
    return False


@backoff.on_exception(
    backoff.expo, Exception, max_tries=5, max_value=32, giveup=_is_request_error
)
def _msearch(searches):
    grq_es = get_grq_es()
    # Use wrapper method instead of direct ES call for closed index handling (HC-600).
    # NOTE: msearch() takes NO `preference` kwarg (raises TypeError); the per-search
    # routing preference is set in each header line by msearch_es (HC-633).
    return grq_es.msearch(searches, request_timeout=30)


@backoff.on_exception(
    backoff.expo, Exception, max_tries=5, max_value=32, giveup=_is_request_error
)
def search_es(index, body, preference):
    grq_es = get_grq_es()
    # Use wrapper method instead of direct ES call for closed index handling (HC-600)
    return grq_es.search(index=index, body=body, request_timeout=30, preference=preference)


def _is_retryable_item_error(item):
    """True when a per-item msearch error is worth re-running individually.
    A 4xx other than 429 is deterministic (parse/validation error; retrying
    cannot change the outcome); 429 (search thread pool rejection under load)
    and 5xx are transient. A missing status degrades to retryable: the re-run
    goes through search_es, whose backoff gives up fast on a genuine 400."""
    status = item.get("status")
    if isinstance(status, int) and 400 <= status < 500 and status != 429:
        return False
    return True


def _fallback_search(header, body, preference):
    """Run one rule's search individually (search_es retries transient errors
    with bounded backoff and gives up on 400s); package a persistent failure
    as a per-item style error so only that rule is skipped."""
    try:
        return search_es(index=header["index"], body=body, preference=preference)
    except Exception as e:
        return {"error": {"reason": str(e)}}


def msearch_es(searches, preference):
    """
    Run all rule queries in one multi search request. A rule whose query is
    valid JSON but unparseable query DSL fails the WHOLE msearch request with
    a 400 at request parse time rather than as a per-item error, so fall back
    to issuing the searches individually in that case -- one bad rule must not
    block evaluation of the remaining rules.

    Transient PER-ITEM errors (e.g. a 429 search-queue rejection under a dense
    cascade, or a 5xx) are re-run individually through the same per-rule
    fallback: batching removed the old per-rule retry, and a saturated search
    queue would otherwise silently drop just that rule's trigger. Deterministic
    400-class item errors stay terminal for their rule. search_es's bounded
    backoff doubles as backpressure -- an overloaded cluster slows this
    evaluator (one task per worker process) instead of receiving fresh
    msearches.

    preference (the triggering objectid) routes every search here to the same
    shard copy the settled-probe checked, closing the replica-lag race (HC-633).
    It is set PER-SEARCH in each header line -- _msearch takes no `preference`
    kwarg, and a URL-level preference is ignored by the _msearch endpoint.
    """
    searches = [({**header, "preference": preference}, body) for header, body in searches]
    try:
        responses = _msearch(searches)
    except Exception as e:
        if not _is_request_error(e):
            raise
        logger.error(
            f"msearch rejected at request parse time ({e}); "
            "falling back to per-rule searches"
        )
        return [_fallback_search(header, body, preference) for header, body in searches]

    for i, response in enumerate(responses):
        if response.get("error") and _is_retryable_item_error(response):
            header, body = searches[i]
            logger.warning(
                f"msearch item error (status {response.get('status')}) is transient, "
                f"retrying rule query individually: {json.dumps(response['error'])}"
            )
            responses[i] = _fallback_search(header, body, preference)
    return responses


def evaluate_user_rules_dataset(
    objectid, system_version, alias=DATASET_ALIAS, job_queue=JOBS_PROCESSED_QUEUE,
    index=None,
):
    """
    Process all user rules in ES database and check if this objectid matches.
    If so, submit jobs. Otherwise do nothing.

    index: the concrete index the ingest wrote the dataset to, passed by
    queue_dataset_evaluation when it can tell (HC-664). The settled-probe and
    every non-query_all rule query are bound to it, so a document with the same
    _id in another index behind the alias cannot decide the outcome. Without it
    (older producers, direct task submissions) evaluation runs alias-wide as
    before.
    """

    # ensure dataset is indexed; pinned is the concrete index the probe settled
    # on, or None when it had to run on the alias
    pinned = ensure_dataset_indexed(objectid, system_version, alias, index=index)

    # get all enabled user rules
    query = {"query": {"term": {"enabled": True}}}
    mozart_es = get_mozart_es()
    rules = mozart_es.query(index=USER_RULES_DATASET_INDEX, body=query)
    logger.info(f"Total {len(rules)} enabled rules to check.")

    # build a single multi search request with one search per rule
    candidates = []
    searches = []
    for document in rules:
        rule = document["_source"]
        logger.info(f"rule: {json.dumps(rule, indent=2)}")

        try:
            updated_query = update_query(objectid, system_version, rule, index=pinned)
            rule["query"] = updated_query
            rule["query_string"] = json.dumps(updated_query)
        except (RuntimeError, Exception) as e:
            logger.error("unable to update user_rule's query, skipping")
            logger.error(e)
            continue

        final_qs = rule["query_string"]

        index_pattern = rule.get("index_pattern", "")
        if index_pattern is None:
            index_pattern = ""
        index_pattern = index_pattern.strip()
        if not index_pattern or not validate_index_pattern(index_pattern):
            logger.warning(
                f"index_pattern {index_pattern} not valid, defaulting to {DATASET_ALIAS}"
            )
            index_pattern = DATASET_ALIAS
        logger.info(f"updated query: {json.dumps(final_qs, indent=2)}")

        searches.append(({"index": index_pattern}, {**updated_query, "size": 1}))
        candidates.append(rule)

    if not searches:
        return True

    # check all rules for matches in a single round trip; preference=objectid pins
    # the same shard copy ensure_dataset_indexed validated (HC-633 replica-lag fix)
    responses = msearch_es(searches, objectid)

    matched = []
    errored = []
    for rule, result in zip(candidates, responses):
        rule_name = rule["rule_name"]

        if result.get("error"):
            logger.error(
                f"Rule '{rule_name}' query failed for {objectid} ({system_version}): "
                f"{json.dumps(result['error'])}"
            )
            errored.append(rule_name)
            continue

        if result["hits"]["total"]["value"] == 0:
            logger.info(
                f"Rule '{rule_name}' didn't match for {objectid} ({system_version})"
            )
            continue
        doc_res = result["hits"]["hits"][0]
        logger.info(
            f"Rule '{rule_name}' successfully matched for {objectid} ({system_version})"
        )

        job_type = rule["job_type"]  # set clean descriptive job name
        if job_type.startswith("hysds-io-"):
            job_type = job_type.replace("hysds-io-", "", 1)
        job_name = f"{job_type}-{objectid}"

        queue_dataset_trigger(doc_res, rule, job_name)  # submit trigger task
        logger.info(
            f"Trigger task submitted for {objectid} ({system_version}): {job_type}"
        )
        matched.append(rule_name)

    logger.info(
        f"Evaluated {len(candidates)} rules for {objectid} ({system_version}): "
        f"matched={matched} errored={errored}"
    )
    return True


def _dataset_index_prefix():
    """grq2 names dataset indices "<GRQ_INDEX>_..." (grq2/lib/dataset.py update()
    and api_v02/datasets.py get_es_index()); GRQ_INDEX is "grq" unless a
    deployment changed it, in which case DATASET_INDEX_PREFIX must match it."""
    prefix = app.conf.get("DATASET_INDEX_PREFIX", None)
    return prefix if isinstance(prefix, str) and prefix else "grq"


def resolve_dataset_index(info):
    """Return the concrete index the ingest wrote this dataset document to, or None.

    Single-document ingest (hysds.dataset_ingest): grq2 answered with the index
    it used, and the ingest stored that answer in grq_index_result. Bulk ingest
    (hysds.dataset_ingest_bulk, the default post-processor): the bulk endpoint
    answers with no per-document index, and grq_index_result holds the dataset
    config's raw index setting instead. Derive the name the way grq2 does, from
    the same fields grq2 received: "<prefix>_<index.suffix>" when the dataset
    carries a suffix (e.g. "1_L1_S1_SLC-2026.10" for a monthly index), else
    "<prefix>_<version>_<dataset>", lower-cased.

    A wrong name costs a WARNING, not a trigger: ensure_dataset_indexed confirms
    the document is in the named index and falls back to the alias when not.
    """
    try:
        gir = info.get("grq_index_result") or {}
        if (
            isinstance(gir, dict)
            and gir.get("success") is True
            and isinstance(gir.get("index"), str)
            and gir["index"]
        ):
            return gir["index"]
        prefix = _dataset_index_prefix()
        index = info.get("index")
        if isinstance(index, dict) and index.get("suffix"):
            return f"{prefix}_{index['suffix']}".lower()
        version, dataset = info.get("version"), info.get("dataset")
        if version and dataset:
            return f"{prefix}_{version}_{dataset}".lower()
    except Exception as e:  # never let a resolver bug block the evaluation
        # (the caller logs the dataset id; do not touch `info` again here)
        logger.warning(f"could not resolve the dataset's index: {e!r}")
    return None


@backoff.on_exception(
    backoff.expo, socket.error, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def queue_dataset_evaluation(info):
    """Queue dataset id for user_rules_dataset evaluation.

    The concrete index the ingest wrote to rides along as the `index` kwarg when
    it can be resolved (HC-664). Consumers must run hysds >= 3.3.6 before a
    producer sends it: an older evaluate_user_rules_dataset rejects the kwarg.
    """
    payload = {
        "type": "user_rules_dataset",
        "function": "hysds.user_rules_dataset.evaluate_user_rules_dataset",
        "args": [info["id"], info["system_version"]],
    }
    index = resolve_dataset_index(info)
    if index:
        payload["kwargs"] = {"index": index}
    logger.info(
        f"queue_dataset_evaluation: {info['id']} ({info['system_version']}) "
        f"index={index}"
    )
    hysds.task_worker.run_task.apply_async(
        (payload,), queue=app.conf.USER_RULES_DATASET_QUEUE
    )  # noqa


@backoff.on_exception(
    backoff.expo, socket.error, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def queue_dataset_trigger(doc_res, rule, job_name):
    """Trigger dataset rule execution."""
    payload = {
        "type": "user_rules_trigger",
        "function": "hysds_commons.job_utils.submit_mozart_job",
        "args": [doc_res, rule],
        "kwargs": {"job_name": job_name, "component": "grq"},
    }
    hysds.task_worker.run_task.apply_async(
        (payload,), queue=USER_RULES_TRIGGER_QUEUE
    )  # noqa
