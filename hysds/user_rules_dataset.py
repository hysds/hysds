from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library

standard_library.install_aliases()

import json
import time
import backoff
import socket
import random
import asyncio

import hysds
from hysds.celery import app
from hysds.utils import validate_index_pattern
from hysds.log_utils import logger, backoff_max_tries, backoff_max_value
from hysds.es_util import get_mozart_es, get_grq_es
from hysds.es_util_async import get_mozart_es_async, get_grq_es_async

from elasticsearch import ElasticsearchException, ConnectionTimeout

GRQ_ES_URL = app.conf.GRQ_ES_URL  # ES
DATASET_ALIAS = app.conf.DATASET_ALIAS
USER_RULES_DATASET_INDEX = app.conf.USER_RULES_DATASET_INDEX

JOBS_PROCESSED_QUEUE = app.conf.JOBS_PROCESSED_QUEUE  # queue names
USER_RULES_TRIGGER_QUEUE = app.conf.USER_RULES_TRIGGER_QUEUE
USER_RULES_DATASET_QUEUE = app.conf.USER_RULES_DATASET_QUEUE

mozart_es = get_mozart_es()
grq_es = get_grq_es()

mozart_es_async = get_mozart_es_async()
grq_es_async = get_grq_es_async()


@backoff.on_exception(
    backoff.expo, Exception, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def ensure_dataset_indexed(objectid, system_version, alias):
    """Ensure dataset is indexed."""
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"_id": objectid}},
                    {"term": {"system_version.keyword": system_version}},
                ]
            }
        }
    }
    logger.info("ensure_dataset_indexed query: %s" % json.dumps(query))

    try:
        count = grq_es.get_count(index=alias, body=query)
        if count == 0:
            error_message = "Failed to find indexed dataset: %s (%s)" % (
                objectid,
                system_version,
            )
            logger.error(error_message)
            raise RuntimeError(error_message)
        logger.info("Found indexed dataset: %s (%s)" % (objectid, system_version))

    except ElasticsearchException as e:
        logger.error("Unable to execute query")
        logger.error(e)


def update_query(_id, system_version, rule):
    """
    takes the rule's query_string and adds system version and dataset's id to "filter" in "bool"
    :param _id: ES's _id
    :param system_version: string/int, system_version field in ES document
    :param rule: dict
    :return: dict
    """
    updated_query = json.loads(rule["query_string"])
    filts = [updated_query, {"term": {"system_version.keyword": system_version}}]

    # will add _id if query all False
    if rule.get("query_all", False) is False:
        filts.append({"term": {"_id": _id}})

    final_query = {
        "query": {
            "bool": {
                "must": filts
            }
        }
    }
    logger.info("Final query (%s): %s" % (rule["rule_name"], json.dumps(final_query)))
    return final_query


@backoff.on_exception(backoff.expo, ConnectionTimeout, max_tries=5, max_value=32)
async def search_es(index, body):
    return await grq_es_async.es.search(index=index, body=body, request_timeout=30)


async def evaluate_rule(_id, system_version, rule, sleep_interval=0.0):
    try:
        updated_query = update_query(_id, system_version, rule)
    except (RuntimeError, Exception) as e:
        logger.error("unable to update user_rule's query, skipping")
        logger.error(e)
        raise e

    index_pattern = rule.get("index_pattern", "")
    if index_pattern is None:
        index_pattern = ""
    index_pattern = index_pattern.strip()
    if not index_pattern or not validate_index_pattern(index_pattern):
        logger.warning("index_pattern %s not valid, defaulting to %s" % (index_pattern, DATASET_ALIAS))
        index_pattern = DATASET_ALIAS

    await asyncio.sleep(random.uniform(0, sleep_interval))
    try:
        result = await search_es(index=index_pattern, body=updated_query)
        if result["hits"]["total"]["value"] == 0:
            return
        doc_res = result["hits"]["hits"][0]
        return doc_res
    except (ElasticsearchException, Exception) as e:
        logger.error("Failed to query ES")
        logger.error(e)
        raise e


async def run_tasks(rules, _id, system_version):
    # breaking into chunks of 25 ease the traffic to ES
    for i in range(0, len(rules), 25):
        tasks = []
        chunk = rules[i:i+25]
        for rule in chunk:
            sleep_interval = float(len(chunk)/4)
            tasks.append(evaluate_rule(_id, system_version, rule["_source"], sleep_interval=sleep_interval))

        task_results = await asyncio.gather(*tasks, return_exceptions=True)  # waiting for all tasks to finish
        for j in range(len(task_results)):
            res = task_results[j]  # getting the async task result

            rule = chunk[j]["_source"]
            rule_name = rule["rule_name"]
            job_type = rule["job_type"]  # set clean descriptive job name

            if isinstance(res, Exception):
                logger.error(res)
                continue
            if res is None:
                logger.info("Rule '%s' didn't match for %s (%s)" % (rule_name, _id, system_version))
                continue

            logger.info("Rule '%s' successfully matched for %s (%s)" % (rule_name, _id, system_version))
            if job_type.startswith("hysds-io-"):
                job_type = job_type.replace("hysds-io-", "", 1)
            job_name = "%s-%s" % (job_type, _id)
            queue_dataset_trigger(res, rule, job_name)  # submit trigger task
            logger.info("Trigger task submitted for %s (%s): %s" % (_id, system_version, job_type))


def evaluate_user_rules_dataset(
    objectid, system_version, alias=DATASET_ALIAS, job_queue=JOBS_PROCESSED_QUEUE
):
    """
    Process all user rules in ES database and check if this objectid matches.
    If so, submit jobs. Otherwise do nothing.
    """

    time.sleep(6)  # sleep for 10 seconds; let any documents finish indexing in ES
    ensure_dataset_indexed(objectid, system_version, alias)  # ensure dataset is indexed

    # get all enabled user rules
    query = {
        "query": {
            "term": {
                "enabled": True
            }
        }
    }
    rules = mozart_es.query(index=USER_RULES_DATASET_INDEX, body=query)
    logger.info("Total %d enabled rules to check." % len(rules))

    # running rule evaluation asynchronously
    asyncio.run(run_tasks(rules, objectid, system_version))
    return True


@backoff.on_exception(
    backoff.expo, socket.error, max_tries=backoff_max_tries, max_value=backoff_max_value
)
def queue_dataset_evaluation(info):
    """Queue dataset id for user_rules_dataset evaluation."""
    payload = {
        "type": "user_rules_dataset",
        "function": "hysds.user_rules_dataset.evaluate_user_rules_dataset",
        "args": [info["id"], info["system_version"]],
    }
    hysds.task_worker.run_task.apply_async((payload,), queue=app.conf.USER_RULES_DATASET_QUEUE)


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
    hysds.task_worker.run_task.apply_async((payload,), queue=USER_RULES_TRIGGER_QUEUE)
