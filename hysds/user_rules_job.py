from future import standard_library

standard_library.install_aliases()

import json
import socket

import backoff
import elasticsearch.exceptions
import opensearchpy.exceptions

import hysds  # avoids cyclical import
from hysds.celery import app
from hysds.es_util import assert_doc_settled, get_mozart_es, is_partial_search_response
from hysds.log_utils import (
    backoff_max_tries,
    backoff_max_value,
    log_custom_event,
    logger,
)

JOBS_ES_URL = app.conf.JOBS_ES_URL  # ES
USER_RULES_JOB_INDEX = app.conf.USER_RULES_JOB_INDEX

JOBS_PROCESSED_QUEUE = app.conf.JOBS_PROCESSED_QUEUE  # queue names
USER_RULES_TRIGGER_QUEUE = app.conf.USER_RULES_TRIGGER_QUEUE
USER_RULES_JOB_QUEUE = app.conf.USER_RULES_JOB_QUEUE
JOB_STATUS_ALIAS = "job_status-current"


def process_xpath(xpath, trigger):
    """
    Process the xpath to extract data from a trigger
    NOTE: This is a copy of hysds_commons.job_utils.process_xpath to avoid circular dependency
    @param xpath - xpath location in trigger
    @param trigger - trigger metadata to extract XPath
    """
    ret = trigger
    parts = xpath.replace("xpath.", "").split(".")
    for part in parts:
        if ret is None or part == "":
            return ret
        # Try to convert to integer, if possible, for list indicies
        try:
            part = int(part)
            if len(ret) <= part:
                ret = None
            else:
                ret = ret[part]
            continue
        except:
            pass
        ret = ret.get(part, None)
    return ret


@backoff.on_exception(
    backoff.expo, Exception, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def ensure_job_indexed(job_id, alias, uuid=None):
    """Ensure the job is indexed AND its latest write is search-visible.

    Job rules commonly filter on a status field that transitions post-creation
    (job-started -> job-completed/job-failed); search is refresh-bound, so the
    updated status can lag the trigger and an _id existence check alone would pass
    on the stale version. assert_doc_settled confirms the searchable copy has
    caught up to the latest write, bounded by the existing exponential backoff.
    """
    logger.info(f"ensure_job_indexed: {job_id}")
    mozart_es = get_mozart_es()
    # Pin the attempt when the caller knows it: with an unreaped orphan present
    # the probe would otherwise pass instantly on the older attempt's doc.
    extra_must = [{"term": {"uuid": uuid}}] if uuid else None
    assert_doc_settled(mozart_es, alias, job_id, extra_must=extra_must)


def get_job(job_id, rule, result):
    """Return generic json job configuration."""
    priority = rule.get("priority", 0)
    return {
        "job_type": f"job:{rule['job_type']}",
        "priority": priority,
        "payload": {
            "job_id": job_id,
            "rule": rule,
            "rule_hit": result,
        },
    }


def update_query(job_id, rule):
    """
    takes the rule's query_string and adds system version and job's id to "filter" in "bool"
    :param job_id: ES's _id
    :param rule: dict
    :return: dict
    """
    updated_query = json.loads(rule["query_string"])
    filts = [updated_query]

    if rule.get("query_all", False) is False:
        filts.append({"term": {"_id": job_id}})

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
    mozart_es = get_mozart_es()
    # Use wrapper method instead of direct ES call for closed index handling (HC-600).
    # NOTE: msearch() takes NO `preference` kwarg (raises TypeError); the per-search
    # routing preference is set in each header line by msearch_es (HC-633).
    return mozart_es.msearch(searches, request_timeout=30)


@backoff.on_exception(
    backoff.expo, Exception, max_tries=5, max_value=32, giveup=_is_request_error
)
def search_es(index, body, preference):
    mozart_es = get_mozart_es()
    # Use wrapper method instead of direct ES call for closed index handling (HC-600)
    return mozart_es.search(index=index, body=body, request_timeout=30, preference=preference)


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
    as a per-item style error so only that rule is skipped. `transient` tells
    the caller whether a later attempt could succeed: a 400 cannot (HC-665)."""
    try:
        return search_es(index=header["index"], body=body, preference=preference)
    except Exception as e:
        return {"error": {"reason": str(e)}, "transient": not _is_request_error(e)}


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

    preference (the triggering job_id) routes every search here to the same shard
    copy the settled-probe checked, closing the replica-lag race (HC-633). It is
    set PER-SEARCH in each header line -- _msearch takes no `preference` kwarg, and
    a URL-level preference is ignored by the _msearch endpoint.
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


def evaluate_user_rules_job(job_id, index=None, uuid=None, rule_names=None, attempt=0):
    """
    Process all user rules in ES database and check if this job ID matches.
    If so, submit jobs. Otherwise do nothing.

    rule_names, attempt: set on a re-queued evaluation (HC-665). A rule whose
    search failed transiently or came back partial (timed out, failed shards,
    or a hit count with no hits), or whose handling raised, is unresolved: it
    neither matched nor provably missed. Unresolved rules are re-queued with a
    growing countdown and re-evaluated alone, so rules that already fired never
    fire twice, up to USER_RULES_MAX_ATTEMPTS evaluations in all; the last
    failure is logged as "GAVE UP" and posted as a user_rules_evaluation event.
    A rule whose query does not parse is a deterministic error and is not
    retried.
    """
    if attempt:
        logger.info(
            f"evaluate_user_rules_job: {job_id} attempt={attempt} "
            f"rule_names={rule_names}"
        )
    try:
        ensure_job_indexed(job_id, alias=index or JOB_STATUS_ALIAS, uuid=uuid)

        # get all enabled user rules
        query = {"query": {"term": {"enabled": True}}}
        mozart_es = get_mozart_es()
        rules = mozart_es.query(index=USER_RULES_JOB_INDEX, body=query)
        logger.info(f"Total {len(rules)} enabled rules to check.")
        if rule_names is not None:
            rules = [d for d in rules if d["_source"].get("rule_name") in rule_names]
            logger.info(f"Re-evaluating {len(rules)} of them: {rule_names}")

        # build a single multi search request with one search per rule
        candidates = []
        searches = []
        for rule in rules:
            rule = rule["_source"]  # extracting _source from the rule itself
            logger.info(f"rule: {json.dumps(rule, indent=2)}")

            try:
                updated_query = update_query(job_id, rule)  # check for matching rules
                rule["query"] = updated_query
                rule["query_string"] = json.dumps(updated_query)
            except (RuntimeError, Exception) as e:
                logger.error("unable to update user_rule's query, skipping")
                logger.error(e)
                continue

            final_qs = rule["query_string"]
            logger.info(f"updated query: {json.dumps(final_qs, indent=2)}")

            searches.append(
                ({"index": index or JOB_STATUS_ALIAS}, {**updated_query, "size": 1})
            )
            candidates.append(rule)

        if not searches:
            return True

        # check all rules for matches in a single round trip; preference=job_id
        # pins the same shard copy ensure_job_indexed validated (HC-633)
        responses = msearch_es(searches, job_id)
    except Exception as e:
        # Nothing has been submitted yet. A transient fault here (the probe or
        # the rule listing out of retries, the msearch failing) must not end as
        # a dead task with every trigger of this job lost (HC-665).
        logger.exception(
            f"Evaluation of {job_id} failed before any rule was decided: {e!r}"
        )
        if requeue_job_evaluation(
            job_id, rule_names, attempt, index=index, uuid=uuid, reason=repr(e)
        ):
            return True
        raise

    if len(responses) < len(searches):
        missing = {
            "error": {"reason": "no msearch response for this rule"},
            "transient": True,
        }
        responses = list(responses) + [missing] * (len(searches) - len(responses))

    matched = []
    errored = []
    unresolved = []
    for (header, body), rule, result in zip(searches, candidates, responses):
        rule_name = rule["rule_name"]
        try:
            if not result.get("error") and is_partial_search_response(result):
                logger.warning(
                    f"Rule '{rule_name}' got a partial response for {job_id}: "
                    f"timed_out={result.get('timed_out')} "
                    f"_shards={json.dumps(result.get('_shards'))} "
                    f"hits.total={json.dumps((result.get('hits') or {}).get('total'))} "
                    f"with no hits; re-running it individually"
                )
                result = _fallback_search(header, body, job_id)
                if not result.get("error") and is_partial_search_response(result):
                    logger.error(
                        f"Rule '{rule_name}' still got a partial response for "
                        f"{job_id} after the re-run"
                    )
                    unresolved.append(rule_name)
                    continue

            if result.get("error"):
                logger.error(
                    f"Rule '{rule_name}' query failed for {job_id}: "
                    f"{json.dumps(result['error'])}"
                )
                if result.get("transient"):
                    unresolved.append(rule_name)
                else:
                    errored.append(rule_name)
                continue

            hits = (result.get("hits") or {}).get("hits") or []
            if not hits:
                logger.info(f"Rule '{rule_name}' didn't match for {job_id}")
                continue

            doc_res = hits[0]
            logger.info(f"Rule '{rule_name}' successfully matched for {job_id}")

            # Create a more specific job name based on persist_job_name flag
            job_type = rule.get("job_type", "")
            if job_type.startswith("hysds-io-"):
                job_type = job_type.replace("hysds-io-", "", 1)

            # Check if we should persist the original job's name
            job_name_path = rule.get("job_name_path", "")
            if job_name_path:
                # Extract the original job's name from the matched document
                job_name_value = process_xpath(job_name_path, doc_res)
            else:
                job_name_value = ""

            if job_name_value:
                job_name = f"{job_type}-{job_name_value}"
            else:
                # Use the generic job_id (default behavior)
                job_name = f"{job_type}-{job_id}"

            # submit trigger task
            queue_job_trigger(doc_res, rule, job_name)
            logger.info(f"Trigger task submitted for {job_id}: {rule['job_type']}")
            matched.append(rule_name)
        except Exception as e:
            # one rule's failure must not take the remaining rules with it (HC-665)
            logger.exception(f"Rule '{rule_name}' raised for {job_id}: {e!r}")
            unresolved.append(rule_name)

    logger.info(
        f"Evaluated {len(candidates)} rules for {job_id}: "
        f"matched={matched} errored={errored} unresolved={unresolved} "
        f"attempt={attempt}"
    )
    if unresolved:
        requeue_job_evaluation(
            job_id,
            unresolved,
            attempt,
            index=index,
            uuid=uuid,
            reason="partial or failed rule responses",
        )
    return True


def _conf_int(name, default):
    """Integer celeryconfig setting, or `default` when unset or not an int."""
    value = app.conf.get(name, default)
    if isinstance(value, bool) or not isinstance(value, int):
        return default
    return value


def requeue_countdown(attempt):
    """Seconds before re-evaluation `attempt + 1`: USER_RULES_REQUEUE_DELAY
    doubling per attempt, capped at USER_RULES_REQUEUE_MAX_DELAY. Keep the cap
    well under the broker's consumer timeout (RabbitMQ: 30 min): a worker holds
    a countdown message unacked until it runs."""
    base = _conf_int("USER_RULES_REQUEUE_DELAY", 60)
    cap = _conf_int("USER_RULES_REQUEUE_MAX_DELAY", 600)
    return min(base * (2**attempt), cap)


def requeue_job_evaluation(
    job_id, rule_names, attempt, index=None, uuid=None, reason=""
):
    """Re-enqueue a job evaluation that left rules unresolved (HC-665).

    rule_names: the rules to re-evaluate; None re-evaluates every rule (the
    evaluation failed before any rule was decided). Returns True when queued.
    Once USER_RULES_MAX_ATTEMPTS evaluations (default 3) have run, or when the
    re-enqueue itself fails, logs "GAVE UP", posts a user_rules_evaluation
    event and returns False.
    """
    next_attempt = attempt + 1
    if next_attempt < _conf_int("USER_RULES_MAX_ATTEMPTS", 3):
        countdown = requeue_countdown(attempt)
        try:
            queue_finished_job(
                job_id,
                index=index,
                uuid=uuid,
                rule_names=rule_names,
                attempt=next_attempt,
                countdown=countdown,
            )
            logger.warning(
                f"user_rules_job: requeued {job_id} as attempt {next_attempt} in "
                f"{countdown} s; rules: {rule_names or 'all'}; reason: {reason}"
            )
            return True
        except Exception as e:
            reason = f"{reason}; requeue failed: {e!r}"
    logger.error(
        f"user_rules_job GAVE UP on {job_id} after {next_attempt} attempt(s); "
        f"rules left undecided: {rule_names or 'all'}; last reason: {reason}"
    )
    try:
        log_custom_event(
            "user_rules_evaluation",
            "gave_up",
            {
                "evaluator": "user_rules_job",
                "job_id": job_id,
                "index": index,
                "uuid": uuid,
                "rules": rule_names,
                "attempts": next_attempt,
                "reason": reason,
            },
            tags=["user_rules_job", "gave_up"],
        )
    except Exception as e:
        logger.error(f"could not post the gave_up event for {job_id}: {e!r}")
    return False


@backoff.on_exception(
    backoff.expo, socket.error, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def queue_finished_job(
    _id, index=None, uuid=None, rule_names=None, attempt=0, countdown=None
):
    """Queue job id for user_rules_job evaluation.

    `uuid` identifies which attempt the evaluation is about, so the settle
    probe does not pass on an older attempt's doc under the same _id.

    `rule_names`, `attempt` and `countdown` are for a re-queued evaluation
    (HC-665, see requeue_job_evaluation); a first evaluation's payload is
    unchanged.
    """
    kwargs = {"index": index, "uuid": uuid}
    if attempt:
        kwargs.update(rule_names=rule_names, attempt=attempt)
    payload = {
        "type": "user_rules_job",
        "function": "hysds.user_rules_job.evaluate_user_rules_job",
        "args": [_id],
        "kwargs": kwargs,
    }
    options = {"queue": USER_RULES_JOB_QUEUE}
    if countdown:
        options["countdown"] = countdown
    hysds.task_worker.run_task.apply_async((payload,), **options)  # noqa


@backoff.on_exception(
    backoff.expo, socket.error, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def queue_job_trigger(doc_res, rule, job_name):
    """Trigger job rule execution."""
    payload = {
        "type": "user_rules_trigger",
        "function": "hysds_commons.job_utils.submit_mozart_job",
        "args": [doc_res, rule],
        "kwargs": {"job_name": job_name, "component": "mozart"},
    }
    hysds.task_worker.run_task.apply_async(
        (payload,), queue=USER_RULES_TRIGGER_QUEUE
    )  # noqa
