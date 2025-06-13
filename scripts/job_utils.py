#!/usr/bin/env python
from future import standard_library

standard_library.install_aliases()
import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime

import hysds.es_util as es_util
from hysds.celery import app
from hysds.utils import parse_iso8601


def get_timedout_query(timeout, status, source_data):
    """Tag jobs stuck in job-started or job-offline that have timed out."""

    query = {
        "query": {
            "bool": {
                "must": [
                    {"terms": {"status": status}},
                    {"range": {"@timestamp": {"lt": f"now-{timeout}s"}}},
                ]
            }
        },
        "_source": source_data,
    }
    return query


def es_query(query, index="job_status-current"):
    print(f"es_query : query : {query}")
    ES = es_util.get_mozart_es()
    result = ES.search(index=index, body=json.dumps(query))
    print(f"run_query : result : \n{json.dumps(result, indent=2)}")
    return result


def run_query_with_scroll(query, index="job_status-current"):
    print(query)
    ES = es_util.get_mozart_es()
    results = ES.query(body=query, index=index)
    print(results)
    return results


def update_es(doc_id, data, index="job_status-current"):
    print(data)
    ES = es_util.get_mozart_es()
    response = ES.update_document(index=index, id=doc_id, body=data)
    print(response)
    return response
