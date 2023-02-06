#!/bin/bash
ES_URL=$1

python ${MOZART_DIR}/ops/hysds/scripts/clean_job_status_indexes.py ${ES_URL}

