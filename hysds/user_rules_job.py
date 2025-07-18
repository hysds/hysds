from future import standard_library

standard_library.install_aliases()

import json
import socket
import time

import backoff
import elasticsearch.exceptions
import opensearchpy.exceptions

import hysds  # avoids cyclical import
from hysds.celery import app
from hysds.es_util import get_mozart_es
from hysds.log_utils import backoff_max_tries, backoff_max_value, logger

JOBS_ES_URL = app.conf.JOBS_ES_URL  # ES
USER_RULES_JOB_INDEX = app.conf.USER_RULES_JOB_INDEX

JOBS_PROCESSED_QUEUE = app.conf.JOBS_PROCESSED_QUEUE  # queue names
USER_RULES_TRIGGER_QUEUE = app.conf.USER_RULES_TRIGGER_QUEUE
USER_RULES_JOB_QUEUE = app.conf.USER_RULES_JOB_QUEUE
JOB_STATUS_ALIAS = "job_status-current"


@backoff.on_exception(
    backoff.expo, Exception, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def ensure_job_indexed(job_id, alias):
    """Ensure job is indexed."""
    query = {"query": {"term": {"_id": job_id}}}
    logger.info(f"ensure_job_indexed: {json.dumps(query)}")
    mozart_es = get_mozart_es()
    count = mozart_es.get_count(index=alias, body=query)
    if count == 0:
        raise RuntimeError(f"Failed to find indexed job: {job_id}")


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


def evaluate_user_rules_job(job_id, index=None):
    """
    Process all user rules in ES database and check if this job ID matches.
    If so, submit jobs. Otherwise do nothing.
    """

    time.sleep(7)  # sleep 7 seconds to allow ES documents to be indexed
    ensure_job_indexed(job_id, alias=index or JOB_STATUS_ALIAS)  # ensure job is indexed

    # get all enabled user rules
    query = {"query": {"term": {"enabled": True}}}
    mozart_es = get_mozart_es()
    rules = mozart_es.query(index=USER_RULES_JOB_INDEX, body=query)
    logger.info(f"Total {len(rules)} enabled rules to check.")

    for rule in rules:
        time.sleep(1)  # sleep between queries

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

        rule_name = rule["rule_name"]
        final_qs = rule["query_string"]
        logger.info(f"updated query: {json.dumps(final_qs, indent=2)}")

        # check for matching rules
        try:
            mozart_es = get_mozart_es()
            result = mozart_es.es.search(index=index or JOB_STATUS_ALIAS, body=final_qs)
            if result["hits"]["total"]["value"] == 0:
                logger.info(f"Rule '{rule_name}' didn't match for {job_id}")
                continue
        except (
            elasticsearch.exceptions.ElasticsearchException,
            opensearchpy.exceptions.OpenSearchException,
        ) as e:
            logger.error("Failed to query ES")
            logger.error(e)
            continue

        doc_res = result["hits"]["hits"][0]
        logger.info(f"Rule '{rule_name}' successfully matched for {job_id}")

        # submit trigger task
        queue_job_trigger(doc_res, rule)
        logger.info(f"Trigger task submitted for {job_id}: {rule['job_type']}")
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
def queue_job_trigger(doc_res, rule):
    """Trigger job rule execution."""
    payload = {
        "type": "user_rules_trigger",
        "function": "hysds_commons.job_utils.submit_mozart_job",
        "args": [doc_res, rule],
        "kwargs": {"component": "mozart"},
    }
    hysds.task_worker.run_task.apply_async(
        (payload,), queue=USER_RULES_TRIGGER_QUEUE
    )  # noqa
