#!/bin/bash
ES_URL=$1

curl -XDELETE "${ES_URL}/job_specs"
curl -XDELETE "${ES_URL}/containers"
