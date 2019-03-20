#!/usr/bin/env python
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import range
from future import standard_library
standard_library.install_aliases()
from hysds.job_worker import run_job


queues = ['job_worker', 'job_worker-lopri', 'job_worker-hipri']
for i in range(33):
    r = run_job.apply_async((i, 1), queue=queues[i % 3])
    print(r.ready())
