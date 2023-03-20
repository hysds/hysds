#!/usr/bin/env python
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library

standard_library.install_aliases()
import sys
import requests
import json
import logging


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


def delete_job_status(es_url):
    """
    Finds the indices associated with the job_status-current alias.
    For each one found, delete it.
    """

    alias = "job_status-current"
    r = requests.get(f"{es_url}/_alias/{alias}")
    if r.status_code == 404:
        # If we get a 404, its safe to assume this is a fresh install
        # and we don’t have to proceed with any subsequent request calls
        logging.info(f"404 Client Error: Not Found for url: {es_url}/_alias/{alias}")
    else:
        scan_result = r.json()
        logging.info(
            f"Found indices associated with alias {alias}:\n{json.dumps(scan_result, indent=2)}"
        )
        for index in scan_result.keys():
            logging.info(f"Deleting from Mozart ES: {index}")
            r = requests.delete(f"{es_url}/{index}")
            r.raise_for_status()


if __name__ == "__main__":
    mozart_es_url = sys.argv[1]
    delete_job_status(mozart_es_url)
