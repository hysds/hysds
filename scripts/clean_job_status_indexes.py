#!/usr/bin/env python
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library

standard_library.install_aliases()
import os
import sys
import requests
import json
import logging
from subprocess import Popen, PIPE
from pprint import pprint

from hysds.celery import app


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


def delete_job_status(es_url):
    """Remove any started jobs from ES job_status index if
    the start_time for the task is earlier than the passed
    in start_time."""

    alias = "job_status-current"
    r = requests.get(f"{es_url}/_alias/{alias}")
    r.raise_for_status()
    scan_result = r.json()
    print(json.dumps(scan_result, indent=2))
    for index in scan_result.keys():
        logging.info(f"Deleting from Mozart ES: {index}")
        r = requests.delete("f{es_url}/{index}")
        r.raise_for_status()


if __name__ == "__main__":
    mozart_es_url = sys.argv[1]
    delete_job_status(mozart_es_url)
