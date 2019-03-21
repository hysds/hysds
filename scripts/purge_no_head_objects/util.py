import sys
import json
import requests
import re
import boto3, botocore
from datetime import datetime


def pull_mozart_errors_es(es_url, error):
    """
    :param es_url: ElasticSearch URL
    :param error: specific error string for the ES query
    :return: List[str] of traceback errors
    """
    query = {
        "size": 10000,
        "fields": ["_id", "traceback", "@timestamp"],
        "query": {
        "bool": {
            "must": [
                {"term": {"status": "job-failed"}},
                {"term": {"short_error.untouched": error}}
            ]
        }
    }}

    req = requests.post(es_url, data=json.dumps(query), verify=False)
    if req.status_code != 200:
        raise "Elasticsearch went wrong"
    elasticsearch_results = req.json()
    return [{
        "traceback": row["fields"]["traceback"][0],
        "timestamp": row["fields"].get("@timestamp", None)
    } for row in elasticsearch_results["hits"]["hits"]]


def process_head_object_errors(errors, slc_regex):
    """
    :param errors: List[{timestamp: str, traceback: str}]
    :param slc_regex:
    :return: List[str] of SLC files
    """
    slc_pattern = re.compile(slc_regex)
    slc_files = []

    for error in errors:
        error_timestamp = error["timestamp"]
        error = error["traceback"].replace("\n", " ")
        match = slc_pattern.match(error)
        if match:
            slc_id = match.group(1)
            slc_files.append({
                "id": slc_id,
                "timestamp": error_timestamp
            })
    return slc_files


def check_dataset_exists_grq(es_url, slc_id):
    """
    :param es_url: string
    :param slc_id: string, the UUID for the record in GRQ
    :return: Str, url path for s3 if it exists, else ''
    """
    es_query = {
      "fields": ["_id", "urls"],
      "query": {
        "bool": {
          "must": [
            {"term": {"_id": slc_id}},
            {"term": {"dataset_type.raw": "slc"}}
          ]
        }
      }
    }
    req = requests.post(es_url, data=json.dumps(es_query), verify=False)
    if req.status_code != 200:
        print("Elasticsearch went wrong, but don't want to accidently delete S3 object, so will mark True")
        raise "Elasticsearch went wrong"

    elasticsearch_results = req.json()
    if elasticsearch_results["hits"]["total"] > 0:
        s3_url = elasticsearch_results["hits"]["hits"][0]["fields"]["urls"]
        return s3_url[1], s3_url[0]
    return None, None


def check_file_in_s3_bucket(s3_resource, bucket, key):
    """
    :param s3_resource: boto3.resource('s3')
    :param bucket: List[str], list of bucket to loop through and check if file exists
    :param key: string
    :return: boolean
    """
    try:
        s3_resource.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":  # The object does not exist.
            return False
        else:  # Something else has gone wrong.
            print(e)
            print("ERROR: Not able to call to s3, will stop script")
            sys.exit()
    else:  # The object does exist.
        return True


def mozart_purge_job(slc_id, logger):
    """
    :param slc_id: string
    :param logger: logging object
    :return: void
    """
    mozart_base_url = "https://c-mozart.aria.hysds.io"
    job_submit_url = mozart_base_url + '/mozart/api/v0.1/job/submit'
    queue = 'system-jobs-queue'
    job_type = "job-lw-tosca-purge"
    job_release = "v0.0.5"
    tag_name = "od_bulk_purge_broken_slc_{}".format(datetime.now().date())

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"dataset_type.raw": "slc"}},
                    {"term": {"_id": slc_id}}
                ]
            }
        }
    }
    purge_params = {
        "query": query,
        "operation": "purge",
        "component": "tosca"
    }
    params = {
        'queue': queue,
        'priority': '3',
        'job_name': job_type,
        'tags': '["{}_{}"]'.format(tag_name, slc_id),
        'type': "{}:{}".format(job_type, job_release),
        'params': json.dumps(purge_params),
        'enable_dedup': False
    }

    req = requests.post(job_submit_url, params=params, verify=False)
    if req.status_code != 200:
        req.raise_for_status()
    result = req.json()

    if 'result' in result.keys() and 'success' in result.keys():
        if result['success'] is True:
            job_id = result['result']
            logger.info("Submitted job for dataset ID: {}".format(slc_id))
            logger.info("TAG NAME: {}".format(tag_name))
        else:
            logger.info('job not submitted successfully: %s' % result)
            raise Exception('job not submitted successfully: %s' % result)
    else:
        logger.info('job not submitted successfully: %s' % result)
        raise Exception('job not submitted successfully: %s' % result)



def pull_old_slcs(es_url, start, batch_size):
    es_query = {
      "size": batch_size,
      "from": start,
      "fields": ["_id", "urls"],
      "query": {
        "bool": {
          "must": [
            {"term": {"dataset.raw": "S1-IW_SLC"}},
          ]
        }
      }
    }

    req = requests.post(es_url, data=json.dumps(es_query), verify=False)
    if req.status_code != 200:
        # logger.info("Elasticsearch went wrong")
        raise "Elasticsearch went wrong"
    elasticsearch_results = req.json()
    return [{
        "_id": row["_id"],
        "urls": row["fields"]["urls"]
    } for row in elasticsearch_results["hits"]["hits"]]
