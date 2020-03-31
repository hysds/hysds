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

import hysds
from hysds.celery import app
from hysds.log_utils import logger, backoff_max_tries, backoff_max_value
from hysds.es_util import get_mozart_es

from elasticsearch import ElasticsearchException

JOBS_ES_URL = app.conf.JOBS_ES_URL  # ES
STATUS_ALIAS = app.conf.STATUS_ALIAS
USER_RULES_JOB_INDEX = app.conf.USER_RULES_JOB_INDEX

JOBS_PROCESSED_QUEUE = app.conf.JOBS_PROCESSED_QUEUE  # queue names
USER_RULES_TRIGGER_QUEUE = app.conf.USER_RULES_TRIGGER_QUEUE
USER_RULES_JOB_QUEUE = app.conf.USER_RULES_JOB_QUEUE

mozart_es = get_mozart_es()


@backoff.on_exception(backoff.expo, Exception, max_tries=backoff_max_tries, max_value=backoff_max_value)
def ensure_job_indexed(job_id, alias):
    """Ensure job is indexed."""
    query = {
        "query": {
            'term': {
                '_id': job_id
             }
        }
    }
    logger.info("ensure_job_indexed query: %s" % json.dumps(query, indent=2))

    total = mozart_es.get_count(alias, query)
    if total == 0:
        raise RuntimeError("Failed to find indexed job: {}".format(job_id))


def get_job(job_id, rule, result):
    """Return generic json job configuration."""
    priority = rule.get('priority', 0)
    return {
        "job_type": "job:%s" % rule['job_type'],
        "priority": priority,
        "payload": {
            "job_id": job_id,
            "rule": rule,
            "rule_hit": result,
        }
    }


def update_query(job_id, rule):
    """
    Update final query.
    TLDR: takes the rule's query and adds system version and job's id to "filter" in "bool"
    """
    query = rule['query']  # build query

    filts = []  # filters

    if rule.get('query_all', False) is False:
        filts.append({
            "term": {
                "_id": job_id
            }
        })

    query['bool']['filter'] = filts
    query = {"query": query}

    logger.info("Final query: %s" % json.dumps(query, indent=2))
    rule['query'] = query
    rule['query_string'] = json.dumps(query)


def evaluate_user_rules_job(job_id, alias=STATUS_ALIAS):
    """Process all user rules in ES database and check if this job ID matches.
       If so, submit jobs. Otherwise do nothing."""

    time.sleep(10)  # sleep 10 seconds to allow ES documents to be indexed
    ensure_job_indexed(job_id, alias)  # ensure job is indexed

    # get all enabled user rules
    query = {
        "query": {
            "term": {"enabled": True}
        }
    }

    rules = mozart_es.query(USER_RULES_JOB_INDEX, query)
    logger.info("Total %d enabled rules to check." % len(rules))

    for rule in rules:
        logger.info('rule: %s' % json.dumps(rule, indent=2))
        rule = rule['_source']  # extracting _source from the rule itself

        time.sleep(1)  # sleep between queries

        update_query(job_id, rule)  # check for matching rules
        final_qs = rule['query_string']

        try:
            result = mozart_es.es.search(index=alias, body=final_qs)
            if result['hits']['total']['value'] == 0:
                logger.info("Rule '%s' didn't match for %s" % (rule['rule_name'], job_id))
                continue
        except ElasticsearchException as e:
            logger.error("Failed to query ES")
            logger.error(e)
            continue

        doc_res = result['hits']['hits'][0]
        logger.info("Rule '%s' successfully matched for %s" % (rule['rule_name'], job_id))

        # submit trigger task
        queue_job_trigger(doc_res, rule)
        logger.info("Trigger task submitted for %s: %s" % (job_id, rule['job_type']))
    return True


@backoff.on_exception(backoff.expo, socket.error, max_tries=backoff_max_tries, max_value=backoff_max_value)
def queue_finished_job(id):
    """Queue job id for user_rules_job evaluation."""
    payload = {
        'type': 'user_rules_job',
        'function': 'hysds.user_rules_job.evaluate_user_rules_job',
        'args': [id],
    }
    hysds.task_worker.run_task.apply_async((payload,), queue=USER_RULES_JOB_QUEUE)


@backoff.on_exception(backoff.expo, socket.error, max_tries=backoff_max_tries, max_value=backoff_max_value)
def queue_job_trigger(doc_res, rule):
    """Trigger job rule execution."""
    payload = {
        'type': 'user_rules_trigger',
        'function': 'hysds_commons.job_utils.submit_mozart_job',
        'args': [doc_res, rule],
        'kwargs': {'component': 'mozart'},
    }
    hysds.task_worker.run_task.apply_async((payload,), queue=USER_RULES_TRIGGER_QUEUE)
