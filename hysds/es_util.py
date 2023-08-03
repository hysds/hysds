from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import boto3
from opensearchpy import AWSV4SignerAuth
from elasticsearch import RequestsHttpConnection as RequestsHttpConnectionES
from opensearchpy import RequestsHttpConnection as RequestsHttpConnectionOS

from hysds.celery import app
from hysds.log_utils import logger

try:
    from hysds_commons.elasticsearch_utils import ElasticsearchUtility
except (ImportError, ModuleNotFoundError):
    logger.error("Cannot import hysds_commons.elasticsearch_utils")

try:
    from hysds_commons.opensearch_utils import OpenSearchUtility
except (ImportError, ModuleNotFoundError):
    logger.error("Cannot import hysds_commons.opensearch_utils")

MOZART_ES = None
GRQ_ES = None


def get_mozart_es():
    global MOZART_ES
    if MOZART_ES is None:
        jobs_es_engine = app.conf.get("JOBS_ES_ENGINE", "elasticsearch")
        aws_es = app.conf.get("JOBS_AWS_ES", False)
        es_url = app.conf["JOBS_ES_URL"]
        region = app.conf.get("AWS_REGION", "us-west-2")

        if jobs_es_engine == "opensearch":
            if aws_es is True:
                credentials = boto3.Session().get_credentials()
                auth = AWSV4SignerAuth(credentials, region)
                MOZART_ES = OpenSearchUtility(
                    es_url,
                    http_auth=auth,
                    connection_class=RequestsHttpConnectionOS,
                    use_ssl=True,
                    verify_certs=False,
                    ssl_show_warn=False,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                )
            else:
                MOZART_ES = OpenSearchUtility(es_url)
        else:
            if aws_es is True:
                credentials = boto3.Session().get_credentials()
                auth = AWSV4SignerAuth(credentials, region)
                MOZART_ES = ElasticsearchUtility(
                    es_url,
                    http_auth=auth,
                    connection_class=RequestsHttpConnectionES,
                    use_ssl=True,
                    verify_certs=False,
                    ssl_show_warn=False,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                )
            else:
                MOZART_ES = ElasticsearchUtility(es_url)
    return MOZART_ES


def get_grq_es():
    global GRQ_ES

    if GRQ_ES is None:
        grq_es_engine = app.conf.get("GRQ_ES_ENGINE", "elasticsearch")
        aws_es = app.conf.get("GRQ_AWS_ES", False)
        es_url = app.conf["GRQ_ES_URL"]
        region = app.conf.get("AWS_REGION", "us-west-2")

        if grq_es_engine == "opensearch":
            if aws_es is True:
                credentials = boto3.Session().get_credentials()
                auth = AWSV4SignerAuth(credentials, region)
                GRQ_ES = OpenSearchUtility(
                    es_url,
                    http_auth=auth,
                    connection_class=RequestsHttpConnectionOS,
                    use_ssl=True,
                    verify_certs=False,
                    ssl_show_warn=False,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                )
            else:
                GRQ_ES = OpenSearchUtility(es_url)
        else:
            if aws_es is True:
                credentials = boto3.Session().get_credentials()
                auth = AWSV4SignerAuth(credentials, region)
                GRQ_ES = ElasticsearchUtility(
                    es_url,
                    http_auth=auth,
                    connection_class=RequestsHttpConnectionES,
                    use_ssl=True,
                    verify_certs=False,
                    ssl_show_warn=False,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                )
            else:
                GRQ_ES = ElasticsearchUtility(es_url)
    return GRQ_ES
