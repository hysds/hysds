#!/bin/bash
ES_URL=$1

curl -XDELETE "${ES_URL}/job_status*"
curl -XDELETE "${ES_URL}/worker_status*"
curl -XDELETE "${ES_URL}/task_status*"
curl -XDELETE "${ES_URL}/event_status*"
curl -XDELETE "${ES_URL}/logstash-*"
