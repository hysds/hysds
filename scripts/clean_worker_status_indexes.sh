#!/bin/bash
ES_URL=$1

curl -XDELETE "${ES_URL}/worker_status-current"
