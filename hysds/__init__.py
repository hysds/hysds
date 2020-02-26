from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from hysds.celery import app
from hysds.log_utils import logger

try:
    from hysds_commons.elasticsearch_utils import ElasticsearchUtility
except ImportError:
    raise ImportError('Cannot import hysds_commons.elasticsearch_utils')

__version__ = "0.4.0"
__url__ = "https://github.com/hysds/hysds"
__description__ = "HySDS (Hybrid Cloud Science Data System)"

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
