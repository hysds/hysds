#!/bin/bash
ES_URL=$1
cwd=$(pwd)

python ${cwd}/clean_job_status_indexes.py ${ES_URL}

