from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
from elasticsearch import RequestsHttpConnection

from hysds.celery import app
from hysds.log_utils import logger

try:
    from hysds_commons.elasticsearch_utils_async import ElasticsearchUtilityAsync
except (ImportError, ModuleNotFoundError):
    logger.error("Cannot import hysds_commons.elasticsearch_utils_async")

MOZART_ES_ASYNC = None
GRQ_ES_ASYNC = None


def get_mozart_es_async():
    global MOZART_ES_ASYNC
    if MOZART_ES_ASYNC is None:
        MOZART_ES_ASYNC = ElasticsearchUtilityAsync(app.conf.JOBS_ES_URL)
    return MOZART_ES_ASYNC


def get_grq_es_async():
    global GRQ_ES_ASYNC

    if GRQ_ES_ASYNC is None:
        aws_es = app.conf.get("GRQ_AWS_ES", False)
        es_host = app.conf["GRQ_ES_HOST"]
        es_url = app.conf["GRQ_ES_URL"]
        region = app.conf["AWS_REGION"]

        if aws_es is True:
            aws_auth = BotoAWSRequestsAuth(
                aws_host=es_host, aws_region=region, aws_service="es"
            )
            GRQ_ES_ASYNC = ElasticsearchUtilityAsync(
                es_url=es_url,
                http_auth=aws_auth,
                connection_class=RequestsHttpConnection,
                use_ssl=True,
                verify_certs=False,
                ssl_show_warn=False,
                timeout=30,
                max_retries=10,
                retry_on_timeout=True,
            )
        else:
            GRQ_ES_ASYNC = ElasticsearchUtilityAsync(es_url)
    return GRQ_ES_ASYNC
