from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import


from future import standard_library
standard_library.install_aliases()
import os
import sys
import json
import requests
import copy
import time
import types
import backoff
import socket
import traceback

import hysds
from hysds.celery import app
from hysds.log_utils import logger, backoff_max_tries, backoff_max_value
from elasticsearch import Elasticsearch, ElasticsearchException

from hysds_commons.elasticsearch_utils import get_es_scrolled_data

GRQ_ES_URL = app.conf.GRQ_ES_URL
DATASET_ALIAS = app.conf.DATASET_ALIAS
USER_RULES_DATASET_INDEX = app.conf.USER_RULES_DATASET_INDEX
JOBS_PROCESSED_QUEUE = app.conf.JOBS_PROCESSED_QUEUE


@backoff.on_exception(backoff.expo,
                      Exception,
                      max_tries=backoff_max_tries,
                      max_value=backoff_max_value)
def ensure_dataset_indexed(objectid, system_version, es_url, alias):
    """Ensure dataset is indexed."""
    es = Elasticsearch([es_url])

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
        data = es.count(index=alias, body=query)
        count = data['count']

        if count == 0:
            error_message = "Failed to find indexed dataset: %s (%s)" % (objectid, system_version)
            logger.error(error_message)
            raise RuntimeError(error_message)
        logger.info("Found indexed dataset: %s (%s)" % (objectid, system_version))

    except ElasticsearchException as e:
        logger.error("Unable to execute query")
        logger.error(e)


def update_query(objectid, system_version, rule):
    """Update final query."""
    # TODO: need to refactor this function because "filtered" has been changed in ES 7+

    # build query
    query = rule['query']

    # filters
    filts = [
        {'term': {'system_version.keyword': system_version}}
    ]

    # query all?
    if rule.get('query_all', False) is False:
        filts.append({'ids':  {'values': [objectid]}})

    # build final query
    if 'filtered' in query:
        final_query = copy.deepcopy(query)
        if 'and' in query['filtered']['filter']:
            final_query['filtered']['filter']['and'].extend(filts)
        else:
            filts.append(final_query['filtered']['filter'])
            final_query['filtered']['filter'] = {
                'and': filts,
            }
    else:
        final_query = {
            'filtered': {
                'query': query,
                'filter': {
                    'and': filts,
                }
            }
        }
    final_query = {"query": final_query}
    logger.info("Final query: %s" % json.dumps(final_query, indent=2))
    rule['query'] = final_query
    rule['query_string'] = json.dumps(final_query)


def evaluate_user_rules_dataset(objectid, system_version, es_url=GRQ_ES_URL, alias=DATASET_ALIAS,
                                user_rules_idx=USER_RULES_DATASET_INDEX,
                                job_queue=JOBS_PROCESSED_QUEUE):
    """Process all user rules in ES database and check if this objectid matches.
       If so, submit jobs. Otherwise do nothing."""

    # sleep for 10 seconds; let any documents finish indexing in ES
    time.sleep(10)

    # ensure dataset is indexed
    ensure_dataset_indexed(objectid, system_version, es_url, alias)

    # get all enabled user rules
    query = {
      "query": {
        "term": {
          "enabled": True
        }
      }
    }
    rules = get_es_scrolled_data(es_url, user_rules_idx, query)

    # process rules
    es = Elasticsearch([es_url])  # ES connection
    for rule in rules:
        # sleep between queries
        time.sleep(1)

        # check for matching rules
        update_query(objectid, system_version, rule)
        final_qs = rule['query_string']
        logger.info("updated query: %s" % json.dumps(final_qs, indent=2))

        try:
            result = es.search(index=alias, body=final_qs)
            if result['hits']['total']['value'] == 0:
                logger.info("Rule '%s' didn't match for %s (%s)" % (rule['rule_name'], objectid, system_version))
                continue
            doc_res = result['hits']['hits'][0]
            logger.info("Rule '%s' successfully matched for %s (%s)" % (rule['rule_name'], objectid, system_version))
        except ElasticsearchException as e:
            logger.error("Failed to query ES")
            logger.error(e)
            continue

        # set clean descriptive job name
        job_type = rule['job_type']
        if job_type.startswith('hysds-io-'):
            job_type = job_type.replace('hysds-io-', '', 1)
        job_name = "%s-%s" % (job_type, objectid)

        # submit trigger task
        queue_dataset_trigger(doc_res, rule, es_url, job_name)
        logger.info("Trigger task submitted for %s (%s): %s" %
                    (objectid, system_version, rule['job_type']))
    return True


@backoff.on_exception(backoff.expo,
                      socket.error,
                      max_tries=backoff_max_tries,
                      max_value=backoff_max_value)
def queue_dataset_evaluation(info):
    """Queue dataset id for user_rules_dataset evaluation."""

    payload = {
        'type': 'user_rules_dataset',
        'function': 'hysds.user_rules_dataset.evaluate_user_rules_dataset',
        'args': [info['id'], info['system_version']],
    }
    hysds.task_worker.run_task.apply_async((payload,),
                                           queue=app.conf.USER_RULES_DATASET_QUEUE)


@backoff.on_exception(backoff.expo,
                      socket.error,
                      max_tries=backoff_max_tries,
                      max_value=backoff_max_value)
def queue_dataset_trigger(doc_res, rule, es_url, job_name):
    """Trigger dataset rule execution."""

    payload = {
        'type': 'user_rules_trigger',
        'function': 'hysds_commons.job_utils.submit_mozart_job',
        'args': [doc_res, rule],
        'kwargs': {'es_hysdsio_url': es_url, 'job_name': job_name},
    }
    hysds.task_worker.run_task.apply_async((payload,),
                                           queue=app.conf.USER_RULES_TRIGGER_QUEUE)
