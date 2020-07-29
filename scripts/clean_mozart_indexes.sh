#!/bin/bash
ES_URL=$1

curl -XDELETE "${ES_URL}/job_status-current"
curl -XDELETE "${ES_URL}/worker_status-current"
curl -XDELETE "${ES_URL}/task_status-current"
curl -XDELETE "${ES_URL}/event_status-current"
curl -XDELETE "${ES_URL}/logstash-*"
