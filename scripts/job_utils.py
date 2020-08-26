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

def es_query(query, index="job_status-current"):
    
    print("es_query : query : {}".format(query))

    ES = es_util.get_mozart_es()
    result = ES.search(index=index, body=json.dumps(query))
    print("run_query : result : \n{}".format(json.dumps(result, indent = 2)))
    return result


def run_query_with_scroll(query, index = "job_status-current"):
    print(query)
    ES = es_util.get_mozart_es()
    results = ES.query(body=query, index=index)
    print(results)
    return results

    
def update_es(doc_id, data, index = "job_status-current"):
    ES = es_util.get_mozart_es()
    response = ES.update_document(index=index, id=doc_id, body=data)
    print(response)
    return response
