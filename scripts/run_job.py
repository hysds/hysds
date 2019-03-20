#!/usr/bin/env python
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import open
from future import standard_library
standard_library.install_aliases()
import sys
import argparse
import json

from hysds.job_worker import run_job
from hysds.celery import app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run job.")
    parser.add_argument('job_json', help="job JSON file")
    args = parser.parse_args()
    with open(args.job_json) as f:
        job = json.load(f)
    job = run_job(job, queue_when_finished=False)
    sys.exit(job['job_info']['status'])
