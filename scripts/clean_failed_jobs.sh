#!/bin/bash
ES_URL=$1

curl -XDELETE "${ES_URL}/job_status/job/_query?q=status:job-failed"
