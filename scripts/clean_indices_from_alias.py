#!/usr/bin/env python
from future import standard_library

standard_library.install_aliases()
import json
import logging
import sys

from hysds.es_util import get_mozart_es

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


def delete_job_status(alias):
    """
    Finds the indices associated with the job_status-current alias.
    For each one found, delete it.
    """
    mozart_es = get_mozart_es()

    res = mozart_es.es.indices.get_alias(name=alias, ignore=404)
    if res.get("status") == 404:
        logging.info(f"404 Client Error: Not Found for querying for alias={alias}")
    else:
        logging.info(
            f"Found indices associated with alias {alias}:\n{json.dumps(res, indent=2)}"
        )
        for index in res.keys():
            logging.info(f"Deleting from Mozart ES: {index}")
            mozart_es.es.indices.delete(index=index)


if __name__ == "__main__":
    alias_name = sys.argv[1]
    delete_job_status(alias_name)
