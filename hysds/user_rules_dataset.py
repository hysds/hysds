from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

import json
import copy
import time
import backoff
import socket

from hysds.celery import app
from hysds.log_utils import logger, backoff_max_tries, backoff_max_value
from hysds import mozart_es, grq_es, task_worker

from elasticsearch import ElasticsearchException

GRQ_ES_URL = app.conf.GRQ_ES_URL  # ES
DATASET_ALIAS = app.conf.DATASET_ALIAS
USER_RULES_DATASET_INDEX = app.conf.USER_RULES_DATASET_INDEX

JOBS_PROCESSED_QUEUE = app.conf.JOBS_PROCESSED_QUEUE  # queue names
USER_RULES_TRIGGER_QUEUE = app.conf.USER_RULES_TRIGGER_QUEUE
USER_RULES_DATASET_QUEUE = app.conf.USER_RULES_DATASET_QUEUE


@backoff.on_exception(backoff.expo, Exception, max_tries=backoff_max_tries, max_value=backoff_max_value)
def ensure_dataset_indexed(objectid, system_version, alias):
    """Ensure dataset is indexed."""
    query = {
      "query": {
        "bool": {
          "must": [
            {'term': {'_id': objectid}},
            {'term': {'system_version.keyword': system_version}}
          ]
        }
      }
    }
    logger.info("ensure_dataset_indexed query: %s" % json.dumps(query, indent=2))

    try:
        count = mozart_es.get_count(alias, query)
        if count == 0:
            error_message = "Failed to find indexed dataset: %s (%s)" % (objectid, system_version)
            logger.error(error_message)
            raise RuntimeError(error_message)
        logger.info("Found indexed dataset: %s (%s)" % (objectid, system_version))

    except ElasticsearchException as e:
        logger.error("Unable to execute query")
        logger.error(e)


def update_query(objectid, system_version, rule):
    """
    Update final query.
    TLDR: takes the rule's query and adds system version and dataset's id to "filter" in "bool"
    """
    updated_query = copy.deepcopy(rule['query'])  # build query

    # filters
    filts = [
        {'term': {'system_version.keyword': system_version}}
    ]

    # query all? (will add _id if False)
    if rule.get('query_all', False) is False:
        filts.append({
            "term": {
                "_id": objectid
            }
        })

    updated_query['bool']['filter'] = filts
    updated_query = {"query": updated_query}

    logger.info("Final query: %s" % json.dumps(updated_query, indent=2))
    rule['query'] = updated_query
    rule['query_string'] = json.dumps(updated_query)


def evaluate_user_rules_dataset(objectid, system_version, es_url=GRQ_ES_URL, alias=DATASET_ALIAS,
                                job_queue=JOBS_PROCESSED_QUEUE):
    """Process all user rules in ES database and check if this objectid matches.
       If so, submit jobs. Otherwise do nothing."""

    time.sleep(10)  # sleep for 10 seconds; let any documents finish indexing in ES
    ensure_dataset_indexed(objectid, system_version, alias)  # ensure dataset is indexed

    # get all enabled user rules
    query = {
      "query": {
        "term": {
          "enabled": True
        }
      }
    }
    rules = mozart_es.query(USER_RULES_DATASET_INDEX, query)

    for document in rules:
        time.sleep(1)  # sleep between queries

        rule = document['_source']
        logger.info("rule: %s" % json.dumps(rule, indent=2))

        update_query(objectid, system_version, rule)
        logger.info("updated query: %s" % json.dumps(final_qs, indent=2))

        final_qs = rule['query_string']
        rule_name = rule['rule_name']
        job_type = rule['job_type']  # set clean descriptive job name

        # check for matching rules
        try:
            result = grq_es.es.search(index=alias, body=final_qs)
            if result['hits']['total']['value'] == 0:
                logger.info("Rule '%s' didn't match for %s (%s)" % (rule_name, objectid, system_version))
                continue
            doc_res = result['hits']['hits'][0]
            logger.info("Rule '%s' successfully matched for %s (%s)" % (rule_name, objectid, system_version))
        except ElasticsearchException as e:
            logger.error("Failed to query ES")
            logger.error(e)
            continue

        if job_type.startswith('hysds-io-'):
            job_type = job_type.replace('hysds-io-', '', 1)
        job_name = "%s-%s" % (job_type, objectid)

        # TODO: remove es_url from queue_dataset_trigger?
        # TODO: need to look at this since both hysds_ios indices are in Mozart now
        # TODO: maybe pass the component name (mozart vs. grq)?
        # submit trigger task
        queue_dataset_trigger(doc_res, rule, es_url, job_name)
        logger.info("Trigger task submitted for %s (%s): %s" % (objectid, system_version, job_type))
    return True


@backoff.on_exception(backoff.expo, socket.error, max_tries=backoff_max_tries, max_value=backoff_max_value)
def queue_dataset_evaluation(info):
    """Queue dataset id for user_rules_dataset evaluation."""

    payload = {
        'type': 'user_rules_dataset',
        'function': 'hysds.user_rules_dataset.evaluate_user_rules_dataset',
        'args': [info['id'], info['system_version']],
    }
    task_worker.run_task.apply_async((payload,), queue=app.conf.USER_RULES_DATASET_QUEUE)


@backoff.on_exception(backoff.expo, socket.error, max_tries=backoff_max_tries, max_value=backoff_max_value)
def queue_dataset_trigger(doc_res, rule, es_url, job_name):
    """Trigger dataset rule execution."""

    payload = {
        'type': 'user_rules_trigger',
        'function': 'hysds_commons.job_utils.submit_mozart_job',
        'args': [doc_res, rule],
        'kwargs': {'es_hysdsio_url': es_url, 'job_name': job_name},
    }
    task_worker.run_task.apply_async((payload,), queue=USER_RULES_TRIGGER_QUEUE)
