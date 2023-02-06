#!/bin/bash
ES_URL=$1

python clean_job_status_indexes.py ${ES_URL}
curl -XDELETE "${ES_URL}/worker_status-current"
curl -XDELETE "${ES_URL}/task_status-current"
curl -XDELETE "${ES_URL}/event_status-current"
curl -XDELETE "${ES_URL}/logstash-*"
