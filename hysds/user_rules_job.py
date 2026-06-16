from future import standard_library

standard_library.install_aliases()

import json
import socket

import backoff
import elasticsearch.exceptions
import opensearchpy.exceptions

import hysds  # avoids cyclical import
from hysds.celery import app
from hysds.es_util import assert_doc_settled, get_mozart_es
from hysds.log_utils import backoff_max_tries, backoff_max_value, logger

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
def ensure_job_indexed(job_id, alias):
    """Ensure the job is indexed AND its latest write is search-visible.

    Job rules commonly filter on a status field that transitions post-creation
    (job-started -> job-completed/job-failed); search is refresh-bound, so the
    updated status can lag the trigger and an _id existence check alone would pass
    on the stale version. assert_doc_settled confirms the searchable copy has
    caught up to the latest write, bounded by the existing exponential backoff.
    """
    logger.info(f"ensure_job_indexed: {job_id}")
    mozart_es = get_mozart_es()
    assert_doc_settled(mozart_es, alias, job_id)


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
    # Use wrapper method instead of direct ES call for closed index handling (HC-600)
    return mozart_es.msearch(searches, request_timeout=30)


@backoff.on_exception(
    backoff.expo, Exception, max_tries=5, max_value=32, giveup=_is_request_error
)
def search_es(index, body):
    mozart_es = get_mozart_es()
    # Use wrapper method instead of direct ES call for closed index handling (HC-600)
    return mozart_es.search(index=index, body=body, request_timeout=30)


def msearch_es(searches):
    """
    Run all rule queries in one multi search request. A rule whose query is
    valid JSON but unparseable query DSL fails the WHOLE msearch request with
    a 400 at request parse time rather than as a per-item error, so fall back
    to issuing the searches individually in that case -- one bad rule must not
    block evaluation of the remaining rules.
    """
    try:
        return _msearch(searches)
    except Exception as e:
        if not _is_request_error(e):
            raise
        logger.error(
            f"msearch rejected at request parse time ({e}); "
            "falling back to per-rule searches"
        )
        responses = []
        for header, body in searches:
            try:
                responses.append(search_es(index=header["index"], body=body))
            except Exception as per_rule_error:
                responses.append({"error": {"reason": str(per_rule_error)}})
        return responses


def evaluate_user_rules_job(job_id, index=None):
    """
    Process all user rules in ES database and check if this job ID matches.
    If so, submit jobs. Otherwise do nothing.
    """

    ensure_job_indexed(job_id, alias=index or JOB_STATUS_ALIAS)  # ensure job is indexed

    # get all enabled user rules
    query = {"query": {"term": {"enabled": True}}}
    mozart_es = get_mozart_es()
    rules = mozart_es.query(index=USER_RULES_JOB_INDEX, body=query)
    logger.info(f"Total {len(rules)} enabled rules to check.")

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

    # check all rules for matches in a single round trip
    responses = msearch_es(searches)

    matched = []
    errored = []
    for rule, result in zip(candidates, responses):
        rule_name = rule["rule_name"]

        if result.get("error"):
            logger.error(
                f"Rule '{rule_name}' query failed for {job_id}: "
                f"{json.dumps(result['error'])}"
            )
            errored.append(rule_name)
            continue

        if result["hits"]["total"]["value"] == 0:
            logger.info(f"Rule '{rule_name}' didn't match for {job_id}")
            continue

        doc_res = result["hits"]["hits"][0]
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

    logger.info(
        f"Evaluated {len(candidates)} rules for {job_id}: "
        f"matched={matched} errored={errored}"
    )
    return True


@backoff.on_exception(
    backoff.expo, socket.error, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def queue_finished_job(_id, index=None):
    """Queue job id for user_rules_job evaluation."""
    payload = {
        "type": "user_rules_job",
        "function": "hysds.user_rules_job.evaluate_user_rules_job",
        "args": [_id],
        "kwargs": {"index": index},
    }
    hysds.task_worker.run_task.apply_async(
        (payload,), queue=USER_RULES_JOB_QUEUE
    )  # noqa


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
