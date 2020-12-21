#!/usr/bin/env python
"""
Search for failed jobs with osaka no-clobber errors during dataset publishing
and clean them out of S3 if the dataset was not indexed.
"""
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from builtins import range
from future import standard_library
standard_library.install_aliases()
import os
import sys
import re
import requests
import json
import logging
import argparse
import boto3
import types
import hashlib
import elasticsearch

from hysds.celery import app


log_format = "[%(asctime)s: %(levelname)s/clean_failed_s3_no_clobber_datasets] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

def update_full_id_hash(es_url, id, full_id_hash):
    try:
        es_index = "grq_v2.0.3_s1-gunw"
        _type = "S1-GUNW"

        ES = elasticsearch.Elasticsearch(es_url)
        ES.update(index=es_index, doc_type=_type, id=id,
              body={"doc": {"metadata": {"full_id_hash": full_id_hash}}}) 
        print("Updated  S1_GUNW : %s full_id_hash :  %s successfully" %(id, full_id_hash))    
    except Exception as err:
        print("ERROR : updationg full_id_hash : %s" %str(err))

def remove_local(s, suffix):
    if suffix and s.endswith(suffix):
        return s[:-len(suffix)]
    return s

def query_es(query, rest_url, es_index):
    """Query ES."""
    url = "{}/{}/_search?search_type=scan&scroll=60&size=100".format(rest_url, es_index)
    #print("url: {}".format(url))
    r = requests.post(url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    #r.raise_for_status()
    scan_result = r.json()
    #print("scan_result: {}".format(json.dumps(scan_result, indent=2)))
    count = scan_result['hits']['total']
    if '_scroll_id' not in scan_result:
        print("_scroll_id not found in scan_result. Returning empty array for the query :\n%s" %query)
        return []

    scroll_id = scan_result['_scroll_id']
    hits = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        if r.status_code != 200:
            print("Failed to query %s:\n%s" % (es_url, r.text))
            print("query: %s" % json.dumps(query, indent=2))
            print("returned: %s" % r.text)
            r.raise_for_status()

        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        hits.extend(res['hits']['hits'])
    return hits

def get_ifg_hash(master_slcs,  slave_slcs):

    master_ids_str=""
    slave_ids_str=""

    for slc in sorted(master_slcs):
        #print("get_ifg_hash : master slc : %s" %slc)
        if isinstance(slc, tuple) or isinstance(slc, list):
            slc = slc[0]

        slc = remove_local(slc, "-local")
        if master_ids_str=="":
            master_ids_str= slc
        else:
            master_ids_str += " "+slc

    for slc in sorted(slave_slcs):
        #print("get_ifg_hash: slave slc : %s" %slc)
        if isinstance(slc, tuple) or isinstance(slc, list):
            slc = slc[0]

        slc = remove_local(slc, "-local")
        if slave_ids_str=="":
            slave_ids_str= slc
        else:
            slave_ids_str += " "+slc

    id_hash = hashlib.md5(json.dumps([
            master_ids_str,
            slave_ids_str
            ]).encode("utf8")).hexdigest()
    return id_hash

def update_hash(grq_es_url, hit):
    ifg_id = hit["_id"]
   
    met = hit['_source']['metadata']
    full_id_hash = met["full_id_hash"]
    ref_slcs = met["reference_scenes"]
    sec_slcs = met["secondary_scenes"]
    new_ifg_hash = get_ifg_hash(ref_slcs, sec_slcs)

    if new_ifg_hash != full_id_hash:
        print("NOT SAME : {} : {} : {}".format(ifg_id, full_id_hash, new_ifg_hash))
        update_full_id_hash(grq_es_url, ifg_id, new_ifg_hash)
    else:
        print("SAME : {} : {} : {}".format(ifg_id, full_id_hash, new_ifg_hash))

def get_tops_ifg(rest_url):
    
    es_index = "grq_v2.0.3_s1-gunw"
    url = "{}/{}/_search?search_type=scan&scroll=60&size=100".format(rest_url, es_index)
    query = {
      "query": {
        "bool": {
          "must": [
            {
              "term": {
                "_type": "S1-GUNW"
              }
            }
          ]
        }
      }
    }


    print(query)
    hits = query_es(query, rest_url, es_index)
    #print(json.dumps(hits[0],indent =4))
    #hits = [i['fields']['partial'][0] for i in query_es(query, rest_url, es_index)]
    print("count : {}".format(len(hits))) 

    return hits


if __name__ == "__main__":
    jobs_es_url = app.conf['JOBS_ES_URL']
    grq_es_url = app.conf['GRQ_ES_URL']
    print(grq_es_url)
    hits = get_tops_ifg(grq_es_url)
    for hit in hits:
        update_hash(grq_es_url, hit)
