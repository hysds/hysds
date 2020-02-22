from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from hysds.celery import app
from hysds.log_utils import logger

from hysds_commons.elasticsearch_utils import ElasticsearchUtility

__version__ = "0.4.0"
__url__ = "https://github.com/hysds/hysds"
__description__ = "HySDS (Hybrid Cloud Science Data System)"

mozart_es = ElasticsearchUtility(app.conf.JOBS_ES_URL, logger)
grq_es = ElasticsearchUtility(app.conf.GRQ_ES_URL, logger)
