#!/usr/bin/env python
from hysds.job_worker import run_job


queues = ['job_worker', 'job_worker-lopri', 'job_worker-hipri']
for i in range(33):
    r = run_job.apply_async((i,1), queue=queues[i%3])
    print r.ready()
