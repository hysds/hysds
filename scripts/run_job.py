#!/usr/bin/env python
from future import standard_library

standard_library.install_aliases()
import argparse
import json
import sys

from hysds.celery import app
from hysds.job_worker import run_job

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run job.")
    parser.add_argument("job_json", help="job JSON file")
    args = parser.parse_args()
    with open(args.job_json) as f:
        job = json.load(f)
    job = run_job(job, queue_when_finished=False)
    sys.exit(job["job_info"]["status"])
