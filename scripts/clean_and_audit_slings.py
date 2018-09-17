#!/usr/bin/env python
"""
Search all failed jobs during sling dataset publishing
and:
 1. clean them out of S3 if the dataset was not indexed.
 2. add tag to jobs to identify which requires requeue / are real errors
 3. remove dedup member in factotum's redis in  "granules-s1a_slc" so we can requeue to asf via qquery
"""

import os, sys, re, requests, json, logging, argparse, boto3, types

from hysds.celery import app

log_format = "[%(asctime)s: %(levelname)s/clean_failed_s3_no_clobber_datasets] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

# S3_RE = re.compile('s3://.+?/(.+?)/(.+/(.+))/.+?')

SLING_NAME_RE = re.compile('sling__release-20180129-factotum-job_worker-(.*)-(.*)-(.*)-(.*)-(.*)')
dtreg = re.compile(r'S1[AB].*?_(\d{4})(\d{2})(\d{2})')
S3_URL = 'incoming/v0.1/%s/%s/%s/%s/%s'
S3_BUCKET = "ntu-hysds-dataset"

S3_MAX_DELETE_CHUNK = 1000


def check_dataset(es_url, id, es_index="grq"):
    """Query for dataset with specified input ID."""

    query = {
        "query": {
            "bool": {
                "must": [
                    {"query_string": {
                        "query": "id:\"%s\"" % id,
                        "default_operator": "OR"}}

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

def tag_job(jobs_es_url, job_es_hit, tag_str):
    id = job_es_hit['_id']
    src = job_es_hit.get('_source', {})
    tags = src.get('tags', [])

    if tag_str not in tags:
        tags.append(tag_str)
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
        logging.info("Tagged %s as %s." % (id, tag_str))
    else:
        logging.info("%s already tagged as %s" % (id, tag_str))


def dataset_exists(es_url, id, es_index="grq"):
    """Return true if dataset id exists."""

    total, id = check_dataset(es_url, id, es_index)
    if total > 0: return True
    return False


def get_matching_s3_objects(client, bucket, prefix='', suffix=''):
    """Return list of objects under an s3 prefix per
       https://alexwlchan.net/2018/01/listing-s3-keys-redux/."""

    kwargs = {'Bucket': bucket}
    if isinstance(prefix, types.StringTypes):
        kwargs['Prefix'] = prefix
    # logging.info("kwargs: %s" % kwargs)
    while True:
        resp = client.list_objects_v2(**kwargs)
        try:
            contents = resp['Contents']
        except KeyError:
            return
        for obj in contents:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield obj
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


def get_matching_s3_keys(client, bucket, prefix='', suffix=''):
    """Generate the keys in an S3 bucket."""

    for obj in get_matching_s3_objects(client, bucket, prefix, suffix):
        yield obj['Key']


def clean(jobs_es_url, grq_es_url, force=False, add_tag=False):
    """Look for failed jobs with osaka no-clobber errors during dataset publishing
       and clean them out if dataset was not indexed."""

    # jobs query
    jobs_query = {
        "query": {
            "bool": {
                "must": [
                    # {
                    #     "term": {
                    #         "status": "job-failed"
                    #     }
                    # },
                    {
                        "term": {
                            "type": "job-sling:release-20180129"
                        }
                    },
                    {
                        "query_string": {
                            "query": "status:job-queued OR status:job-failed",
                            "default_operator": "OR"
                        }
                    }
                    # {
                    #     "term": {
                    #         "short_error.untouched": "Destination, s3://s3.....ber is set"
                    #     }
                    # },
                    # {
                    #     "query_string": {
                    #         "query": "error:\"already exists and no-clobber is set\"",
                    #         "default_operator": "OR"
                    #     }
                    # }
                ]
            }
        },
        "partial_fields": {
            "_source": {
                "include": [
                    "error", "tags", "job.name"
                ]
            }
        }
    }

    url_tmpl = "{}/job_status-current/_search?search_type=scan&scroll=10m&size=100"
    r = requests.post(url_tmpl.format(jobs_es_url), data=json.dumps(jobs_query))
    if r.status_code != 200:
        logging.error("Failed to query ES. Got status code %d:\n%s" %
                      (r.status_code, json.dumps(jobs_query, indent=2)))
    r.raise_for_status()
    scan_result = r.json()
    count = scan_result['hits']['total']
    logging.info("%d hits from no-clobber error query: " % count)

    scroll_id = scan_result['_scroll_id']

    # get boto client
    client = boto3.client('s3')

    # get list of results and sort by bucket
    results_to_clear_s3 = {}
    results_okay = []
    results_to_requeue = []
    dataset_name_list = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=10m' % jobs_es_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        for hit in res['hits']['hits']:
            # error = hit['fields']['_source'][0]['error']

            # extract s3 url bucket and dataset id
            # match = S3_RE.search(error)
            # if not match: raise RuntimeError("Failed to find S3 url in error: %s" % error)
            # sling__release-20180129-factotum-job_worker-scihub_throttled-S1A_IW_SLC__1SDV_20180204T012542_20180204T012609_020450_022F70_D4B2-13_Aug_2018_11__44__16-AOI_Karachi-20180813T114416.959892Z
            job_name = hit['fields']['_source'][0]['job']['name']
            match_id = SLING_NAME_RE.search(job_name)
            logging.info("Checking for: %s" % job_name)

            if not match_id: raise RuntimeError("Failed to id in job name: %s" % job_name)
            id = match_id.group(2)

            match_date = dtreg.search(id)
            if match_date:
                yr, mon, day = (match_date.group(1), match_date.group(2), match_date.group(3))

            dataset_id = id + ".zip"
            s3_prefix = S3_URL % (yr, mon, day, dataset_id)

            if dataset_id not in dataset_name_list:
                dataset_name_list.append(dataset_id)

                # query if dataset exists in GRQ; if it is; we can add the job to list of okay jobs and skip.
                if dataset_exists(grq_es_url, dataset_id):
                    logging.warning("Found %s in %s. Job completed. Not cleaning out from s3." % (dataset_id, grq_es_url))
                    results_okay.append(hit)
                else:
                    # get list of all objects under the prefix
                    dataset_objs = list(get_matching_s3_keys(client, S3_BUCKET, s3_prefix))
                    logging.info("Found %d objects for dataset %s" % (len(dataset_objs), s3_prefix))
                    results_to_clear_s3.setdefault(S3_BUCKET, []).extend(dataset_objs)

                    # get list of jobs in mozart es that needs to be tagged for requeue
                    results_to_requeue.append(hit)
            else:
                # for repeated failed jobs
                logging.info("%s already registered, skipping checks." % dataset_id)
                continue






    # print results per bucket
    for bucket in sorted(results_to_clear_s3):
        logging.info("Found %d osaka no-clobber errors for bucket %s" % (len(results_to_clear_s3[bucket]), bucket))

    # perform cleanup
    for bucket in sorted(results_to_clear_s3):
        # chunk
        chunks = [results_to_clear_s3[bucket][x:x + S3_MAX_DELETE_CHUNK] for x in
                  xrange(0, len(results_to_clear_s3[bucket]), S3_MAX_DELETE_CHUNK)]

        for chunk in chunks:
            if force:
                del_obj = {"Objects": [{'Key': obj} for obj in chunk]}
                logging.info(json.dumps(del_obj, indent=2))
                client.delete_objects(Bucket=bucket, Delete=del_obj)
            else:
                logging.info("Running dry-run. These objects would've been deleted:")
                for obj in chunk: logging.info(obj)

    # tag jobs for requeue
    logging.info("Found %d jobs which can be requeued if s3 has been cleared:" % len(results_to_requeue))

    for job in sorted(results_to_requeue):
        src = job['fields']['_source'][0]
        job_name = src.get('job', {}).get('name')
        job_id = job['_id']
        logging.info("id: %s name: %s" % (job_id, job_name))
        if add_tag:
            tag_job(jobs_es_url, job, "dataset-not-in-grq-requeue")

    for job in sorted(results_okay):
        src = job['fields']['_source'][0]
        job_name = src.get('job', {}).get('name')
        job_id = job['_id']
        logging.info("id: %s name: %s" % (job_id, job_name))
        if add_tag:
            tag_job(jobs_es_url, job, "dataset-in-grq-job-ok")






if __name__ == "__main__":
    jobs_es_url = app.conf['JOBS_ES_URL']
    grq_es_url = app.conf['GRQ_ES_URL']
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-f', '--force', help="force deletion", action='store_true')
    parser.add_argument('-t', '--add-tag', help="add tag for associated jobs to be marked ok or requeued", action='store_true')

    args = parser.parse_args()

    clean(jobs_es_url, grq_es_url, args.force, args.add_tag)
