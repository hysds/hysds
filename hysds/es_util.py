from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from hysds.celery import app
from hysds.log_utils import logger

try:
    from hysds_commons.elasticsearch_utils import ElasticsearchUtility
except (ImportError, ModuleNotFoundError):
    logger.error('Cannot import hysds_commons.elasticsearch_utils')

MOZART_ES = None
GRQ_ES = None


def get_mozart_es():
    global MOZART_ES
    if MOZART_ES is None:
        MOZART_ES = ElasticsearchUtility(app.conf.JOBS_ES_URL, logger)
    return MOZART_ES


def get_grq_es():
    global GRQ_ES
    if GRQ_ES is None:
        GRQ_ES = ElasticsearchUtility(app.conf.JOBS_ES_URL, logger)
    return GRQ_ES
