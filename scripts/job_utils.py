#!/usr/bin/env python
from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
from builtins import int
from future import standard_library
standard_library.install_aliases()
import os
import sys
import json
import time
import traceback
import logging
from datetime import datetime

from hysds.utils import parse_iso8601
from hysds.celery import app
import hysds.es_util as es_util
from elasticsearch import Elasticsearch, exceptions


def get_timedout_query(timeout, status, source_data):
    """Tag jobs stuck in job-started or job-offline that have timed out."""

    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "status": status
                        }
                    },
                    {
                        "range": {
                            "@timestamp": {
                                "lt": "now-%ds" % timeout
                            }
                        }
                    }
                ]
            }
        },
        "_source": source_data
    }
    return query

def run_query(query):
    
    print(query)
    ES = es_util.get_mozart_es()
    result = ES.search(index="job_status-current", body=json.dumps(query))
    print("run_query : result : \n{}".format(json.dumps(result, indent = 2)))
    return result


def run_query_with_scroll(query, url="localhost:9200", index = "job_status-current"):
    client = Elasticsearch(url)
    resp = client.search(
            index = index,
            body = json.dumps(query),
            scroll = '10m' # length of time to keep search context
        )
    old_scroll_id = resp['_scroll_id']
    results = []
    while len(resp['hits']['hits']):
        resp = client.scroll(
                scroll_id = old_scroll_id,
                scroll = '10s' # length of time to keep search context
            )
        old_scroll_id = resp['_scroll_id']
        if len(resp['hits']['hits']) == 0:
            break
        for hit in resp['hits']['hits']:
            results.append(hit)
    
    return results

def update_es(doc_id, data, url="localhost:9200", index = "job_status-current"):
    client = Elasticsearch(url)
    response = client.update(index=index, id=doc_id, body=data)
    print(response)
    return response
