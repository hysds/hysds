#!/usr/bin/env python
"""
Search for failed jobs with osaka no-clobber errors during dataset publishing
and clean them out of WebDAV if the dataset was not indexed.
"""

from future import standard_library

standard_library.install_aliases()
import argparse
import json
import logging
import os
import re
import sys
import traceback
import types

import requests

from hysds.celery import app

log_format = (
    "[%(asctime)s: %(levelname)s/clean_failed_dav_no_clobber_datasets] %(message)s"
)
logging.basicConfig(format=log_format, level=logging.INFO)


DAV_RE = re.compile(
    r"Destination,\s+(davs?)://(.+?/.+/(.+))/.+?, already exists and no-clobber is set"
)


def check_dataset(es_url, id, es_index="grq"):
    """Query for dataset with specified input ID."""

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"_id": id}},
                ]
            }
        },
        "fields": [],
    }

    if es_url.endswith("/"):
        search_url = f"{es_url}{es_index}/_search"
    else:
        search_url = f"{es_url}/{es_index}/_search"
    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code == 200:
        result = r.json()
        # logging.info("result: %s" % result)
        total = result["hits"]["total"]
        id = "NONE" if total == 0 else result["hits"]["hits"][0]["_id"]
    else:
        logging.error(f"Failed to query {es_url}:\n{r.text}")
        logging.error(f"query: {json.dumps(query, indent=2)}")
        logging.error(f"returned: {r.text}")
        if r.status_code == 404:
            total, id = 0, "NONE"
        else:
            r.raise_for_status()
    return total, id


def dataset_exists(es_url, id, es_index="grq"):
    """Return true if dataset id exists."""

    total, id = check_dataset(es_url, id, es_index)
    if total > 0:
        return True
    return False


def clean(jobs_es_url, grq_es_url, force=False):
    """Look for failed jobs with osaka no-clobber errors during dataset publishing
    and clean them out if dataset was not indexed."""

    # jobs query
    jobs_query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"status": "job-failed"}},
                    {
                        "term": {
                            "short_error.untouched": "Destination, davs://.....ber is set"
                        }
                    },
                    {
                        "query_string": {
                            "query": 'error:"already exists and no-clobber is set"',
                            "default_operator": "OR",
                        }
                    },
                ]
            }
        },
        "partial_fields": {"_source": {"include": ["error"]}},
    }
    url_tmpl = "{}/job_status-current/_search?search_type=scan&scroll=10m&size=100"
    r = requests.post(url_tmpl.format(jobs_es_url), data=json.dumps(jobs_query))
    if r.status_code != 200:
        logging.error(
            "Failed to query ES. Got status code %d:\n%s"
            % (r.status_code, json.dumps(jobs_query, indent=2))
        )
    r.raise_for_status()
    scan_result = r.json()
    count = scan_result["hits"]["total"]
    scroll_id = scan_result["_scroll_id"]

    # get list of results and sort by bucket
    results = {}
    while True:
        r = requests.post(f"{jobs_es_url}/_search/scroll?scroll=10m", data=scroll_id)
        res = r.json()
        scroll_id = res["_scroll_id"]
        if len(res["hits"]["hits"]) == 0:
            break
        for hit in res["hits"]["hits"]:
            error = hit["fields"]["_source"][0]["error"]

            # extract dav url and dataset id
            match = DAV_RE.search(error)
            if not match:
                raise RuntimeError(f"Failed to find DAV url in error: {error}")
            proto, prefix, dataset_id = match.groups()

            # query if dataset exists in GRQ; then no-clobber happened because of dataset deduplication
            if dataset_exists(grq_es_url, dataset_id):
                logging.warning(
                    f"Found {dataset_id} in {grq_es_url}. Not cleaning out from dav."
                )
                continue

            # remove
            ds_url = f"{'https' if proto == 'davs' else 'http'}://{prefix}"
            try:
                r = requests.delete(ds_url, verify=False)
                r.raise_for_status()
            except Exception as e:
                logging.warning(f"Failed to delete {ds_url}: {traceback.format_exc()}")
                pass


if __name__ == "__main__":
    jobs_es_url = app.conf["JOBS_ES_URL"]
    grq_es_url = app.conf["GRQ_ES_URL"]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-f", "--force", help="force deletion", action="store_true")
    args = parser.parse_args()

    clean(jobs_es_url, grq_es_url, args.force)
