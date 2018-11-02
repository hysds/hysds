#!/usr/bin/env python
"""
For all incoming, search for zip files not extracted for SLCs
and:
 1. submit data-extract jobs if SLC not found in GRQ
"""

import os, sys, re, requests, json, logging, argparse, boto3, types

from hysds.celery import app

log_format = "[%(asctime)s: %(levelname)s/clean_failed_s3_no_clobber_datasets] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

INCOMING_RE = re.compile('incoming-.{4}-.{2}-.{2}-(.*IW_SLC.*).zip')

S3_MAX_DELETE_CHUNK = 1000


def check_dataset(es_url, id, es_index="grq"):
    """Query for dataset with specified input ID."""

    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "dataset.raw": "S1-IW_SLC"
                        }
                    },
                    {"query_string": {
                        "query": "_id:\"%s\"" % id,
                        "default_operator": "OR"}
                    }

                ]
            }
        },
        "fields": [],
    }

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code == 200:
        result = r.json()
        # logging.info("result: %s" % result)
        total = result['hits']['total']
        id = 'NONE' if total == 0 else result['hits']['hits'][0]['_id']
    else:
        logging.error("Failed to query %s:\n%s" % (es_url, r.text))
        logging.error("query: %s" % json.dumps(query, indent=2))
        logging.error("returned: %s" % r.text)
        if r.status_code == 404:
            total, id = 0, 'NONE'
        else:
            r.raise_for_status()
    return total, id

def tag_job_for_requeue(jobs_es_url, job_es_hit):
    id = job_es_hit['_id']
    src = job_es_hit.get('_source', {})
    tags = src.get('tags', [])

    if 'to-requeue-no-clobber' not in tags:
        tags.append('to-requeue-no-clobber')
        new_doc = {
            "doc": {"tags": tags},
            "doc_as_upsert": True
        }
        r = requests.post('%s/job_status-current/job/%s/_update' % (jobs_es_url, id),
                          data=json.dumps(new_doc))
        result = r.json()
        if r.status_code != 200:
            logging.error("Failed to update tags for %s. Got status code %d:\n%s" %
                          (id, r.status_code, json.dumps(result, indent=2)))
        r.raise_for_status()
        logging.info("Tagged %s as to-requeue-no-clobber." % id)
    else:
        logging.info("%s already tagged as to-requeue-no-clobber." % id)



def dataset_exists(es_url, id, es_index="grq"):
    """Return true if dataset id exists."""

    total, id = check_dataset(es_url, id, es_index)
    if total > 0: return True
    return False


def clean(job_submit_url, grq_es_url, force=False):
    """Look for failed jobs with osaka no-clobber errors during dataset publishing
       and clean them out if dataset was not indexed."""

    # incoming grq query
    incoming_query = \
        {"query":
            {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "dataset.raw": "incoming"
                            }
                        }
                    ]
                }
            }
        }


    url_tmpl = "{}/grq/_search?search_type=scan&scroll=10m&size=100"
    r = requests.post(url_tmpl.format(grq_es_url), data=json.dumps(incoming_query))
    if r.status_code != 200:
        logging.error("Failed to query ES. Got status code %d:\n%s" %
                      (r.status_code, json.dumps(incoming_query, indent=2)))
    r.raise_for_status()
    scan_result = r.json()
    count = scan_result['hits']['total']
    logging.info("%d hits from incoming SLCs query: " % count)

    scroll_id = scan_result['_scroll_id']
    #
    # # get boto client
    # client = boto3.client('s3')

    # get list of results and sort by bucket
    results_to_extract = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=10m' % grq_es_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        for hit in res['hits']['hits']:
            incoming_id = hit['_id']

            # extract s3 url bucket and dataset id
            match = INCOMING_RE.search(incoming_id)

            if not match:
                logging.warn("Failed to find SLC in %s, not an SLC we want. Skipping!" % incoming_id)
                continue

            slc_dataset_id = match.groups()

            if dataset_exists(grq_es_url, slc_dataset_id):
                logging.warning("Found %s in %s. Not appending to submit extract job." % (slc_dataset_id, grq_es_url))
            else:
                logging.warning("%s not extracted!" % (slc_dataset_id))
                results_to_extract.append(incoming_id)

    # tag jobs for requeue
    logging.info("Found %d incoming datasets which can be extracted:" % len(results_to_extract))

    for id in sorted(results_to_extract):
        logging.info(id)
        if force:

            job_params_query = {
                "query": {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "dataset.raw": "S1-IW_SLC"
                                    }
                                },
                                {"query_string": {
                                    "query": "_id:\"%s\"" % id,
                                    "default_operator": "OR"}
                                }
                            ]
                        }
                    },
                    "fields": [],
                }
            }

            params = {}
            params["queue"] = "aria-job_worker-small"
            params["priority"] = "5"
            params["tags"] = '["%s"]' % "data-extract"
            params["type"] = 'job-%s:%s' % ("spyddder-extract", "release-20180823")
            params["params"] = json.dumps(job_params_query)
            params["enable_dedup"] = False

            logging.info('submitting jobs with params:')
            logging.info(json.dumps(params, sort_keys=True, indent=4, separators=(',', ': ')))
            r = requests.post(job_submit_url, params=params, verify=False)

            if r.status_code != 200:
                r.raise_for_status()
            result = r.json()
            if 'result' in result.keys() and 'success' in result.keys():
                if result['success'] == True:
                    job_id = result['result']
                    print 'submitted job: %s' % json.dumps(params)
                else:
                    print 'job not submitted successfully: %s' % result
                    raise Exception('job not submitted successfully: %s' % result)
            else:
                raise Exception('job not submitted successfully: %s' % result)





if __name__ == "__main__":
    jobs_es_url = app.conf['JOBS_ES_URL']
    grq_es_url = app.conf['GRQ_ES_URL']
    job_submit_url = '%s/mozart/api/v0.1/job/submit' % jobs_es_url
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-f', '--force', help="force deletion", action='store_true')

    args = parser.parse_args()

    clean(job_submit_url, grq_es_url, args.force)
