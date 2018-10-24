#!/usr/bin/env python
"""
Search all of granules-s1a in redis
and:
 1. clean them out of redis if dataset not found in GRQ
"""

import os, sys, re, requests, json, logging, argparse, boto3, types
from hysds.celery import app
from redis import ConnectionPool, StrictRedis

log_format = "[%(asctime)s: %(levelname)s/clean_failed_s3_no_clobber_datasets] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

# S3_RE = re.compile('s3://.+?/(.+?)/(.+/(.+))/.+?')

SLING_NAME_RE = re.compile('sling__(.*)factotum-job_worker(.*)throttled-(.{67})-(.*)')
dtreg = re.compile(r'S1[AB].*?_(\d{4})(\d{2})(\d{2})')
S3_URL = 'incoming/v0.1/%s/%s/%s/%s/'
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

    # perform cleanup in redis
    global POOL
    POOL = ConnectionPool(host='172.31.46.101', port=6379, db=0)
    rd = StrictRedis(connection_pool=POOL)
    key = "granules-s1a_slc"
    slcs_in_redis = rd.smembers("granules-s1a_slc")
    slcs_to_remove_in_redis = []
    for slc_id in slcs_in_redis:
        if dataset_exists(grq_es_url, slc_id, es_index="grq"):
            logging.info("%s found in GRQ. Not removing in redis." % slc_id)
        else:
            logging.info("%s not found in GRQ. Adding to removal list in redis." % slc_id)
            slcs_to_remove_in_redis.append(slc_id)

    logging.info("Found %s entries in redis that are not in GRQ, removing for qquery to requeue and sling." % len(slcs_to_remove_in_redis))

    for member in slcs_to_remove_in_redis:
        ismember = rd.sismember(key, member)
        if ismember:
            logging.info("Redis: %s found in %s." % (member, key))
            if force:
                logging.info("Redis: removed %s from %s." % (member, key))
                rd.srem(key, member)
        else:
            logging.info("Redis: %s not in %s. Doing nothing." % (member, key))


if __name__ == "__main__":
    jobs_es_url = app.conf['JOBS_ES_URL']
    grq_es_url = app.conf['GRQ_ES_URL']
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-f', '--force', help="force deletion", action='store_true')

    args = parser.parse_args()

    clean(jobs_es_url, grq_es_url, args.force, args.add_tag)
