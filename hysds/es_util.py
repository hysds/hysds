from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
from elasticsearch import RequestsHttpConnection
import urllib3

from hysds.celery import app
from hysds.log_utils import logger

try:
    from hysds_commons.elasticsearch_utils import ElasticsearchUtility
except (ImportError, ModuleNotFoundError):
    logger.error('Cannot import hysds_commons.elasticsearch_utils')

urllib3.disable_warnings()

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
        aws_es = app.conf['GRQ_AWS_ES']
        es_host = app.conf['GRQ_ES_HOST']
        es_url = app.conf['GRQ_ES_URL']
        region = app.conf['AWS_REGION']

        if aws_es:
            aws_auth = BotoAWSRequestsAuth(aws_host=es_host, aws_region=region, aws_service='es')
            GRQ_ES = ElasticsearchUtility(
                es_url=es_host,
                logger=logger,
                http_auth=aws_auth,
                connection_class=RequestsHttpConnection,
                use_ssl=True,
                verify_certs=False,
            )
        else:
            GRQ_ES = ElasticsearchUtility(es_url, logger)
    return GRQ_ES
