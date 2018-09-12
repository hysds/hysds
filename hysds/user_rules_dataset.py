from __future__ import absolute_import

import os, sys, json, requests, copy, time, types, backoff, socket, traceback

import hysds
from hysds.celery import app
from hysds.log_utils import logger, backoff_max_tries, backoff_max_value
 

@backoff.on_exception(backoff.expo,
                      Exception,
                      max_tries=backoff_max_tries,
                      max_value=backoff_max_value)
def ensure_dataset_indexed(objectid, system_version, es_url, alias):
    """Ensure dataset is indexed."""

    query = {
        "query":{
            "bool":{
                "must":[
                    { 'term': { '_id': objectid }},
                    { 'term': { 'system_version.raw': system_version }}
                ]
            }
        },
        "fields": [],
    }
    logger.info("ensure_dataset_indexed query: %s" % json.dumps(query, indent=2))
    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, alias)
    else:
        search_url = '%s/%s/_search' % (es_url, alias)
    logger.info("ensure_dataset_indexed url: %s" % search_url)
    r = requests.post(search_url, data=json.dumps(query))
    logger.info("ensure_dataset_indexed status: %s" % r.status_code)
    r.raise_for_status()
    result = r.json()
    logger.info("ensure_dataset_indexed result: %s" % json.dumps(result, indent=2))
    total = result['hits']['total']
    if total == 0:
        raise RuntimeError("Failed to find indexed dataset: %s (%s)" % (objectid, system_version))


def update_query(objectid, system_version, rule):
    """Update final query."""

    # build query
    query = rule['query']

    # filters
    filts = [
        { 'term': { 'system_version.raw': system_version }}
    ]

    # query all?
    if rule.get('query_all', False) is False:
        filts.append({ 'ids':  { 'values': [objectid] }})

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
    final_query = { "query": final_query }
    logger.info("Final query: %s" % json.dumps(final_query, indent=2))
    rule['query'] = final_query
    rule['query_string'] = json.dumps(final_query)


def evaluate_user_rules_dataset(objectid, system_version,
                                es_url=app.conf.GRQ_ES_URL,
                                alias=app.conf.DATASET_ALIAS,
                                user_rules_idx=app.conf.USER_RULES_DATASET_INDEX,
                                job_queue=app.conf.JOBS_PROCESSED_QUEUE):
    """Process all user rules in ES database and check if this objectid matches.
       If so, submit jobs. Otherwise do nothing."""

    # sleep for 10 seconds; let any documents finish indexing in ES
    time.sleep(10)

    # ensure dataset is indexed
    ensure_dataset_indexed(objectid, system_version, es_url, alias)

    # get all enabled user rules
    query = { "query": { "term": { "enabled": True } } }
    r = requests.post('%s/%s/.percolator/_search?search_type=scan&scroll=10m&size=100' %
                      (es_url, user_rules_idx), data=json.dumps(query))
    r.raise_for_status()
    scan_result = r.json()
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']
    rules = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=10m' % es_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        for hit in res['hits']['hits']:
            rules.append(hit['_source'])
    logger.info("Got %d enabled rules to check." % len(rules))

    # process rules
    for rule in rules:
        # sleep between queries
        time.sleep(1)

        # check for matching rules
        update_query(objectid, system_version, rule)
        final_qs = rule['query_string']
        try:
            r = requests.post('%s/%s/_search' % (es_url, alias), data=final_qs)
            r.raise_for_status()
        except:
            logger.error("Failed to query ES. Got status code %d:\n%s" %
                         (r.status_code, traceback.format_exc()))
            continue
        result = r.json()
        if result['hits']['total'] == 0:
            logger.info("Rule '%s' didn't match for %s (%s)" % (rule['rule_name'], objectid, system_version))
            continue
        else: doc_res = result['hits']['hits'][0]
        logger.info("Rule '%s' successfully matched for %s (%s)" % (rule['rule_name'], objectid, system_version))
        #logger.info("doc_res: %s" % json.dumps(doc_res, indent=2))

        # set clean descriptive job name
        job_type = rule['job_type']
        if job_type.startswith('hysds-io-'):
            job_type = job_type.replace('hysds-io-', '', 1)
        job_name = "%s-%s" % (job_type, objectid)

        # submit trigger task
        queue_dataset_trigger(doc_res, rule, es_url, job_name)
        logger.info("Trigger task submitted for %s (%s): %s" % (objectid, system_version, rule['job_type']))

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
        'args': [ info['id'], info['system_version'] ],
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
        'args': [ doc_res, rule ],
        'kwargs': { 'es_hysdsio_url': es_url, 'job_name': job_name },
    }
    hysds.task_worker.run_task.apply_async((payload,),
                                           queue=app.conf.USER_RULES_TRIGGER_QUEUE)
