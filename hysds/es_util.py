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
METRICS_ES = None


def get_mozart_es_engine():
    return app.conf.get("JOBS_ES_ENGINE", "elasticsearch")


def get_mozart_es(hosts=None):
    global MOZART_ES
    if MOZART_ES is None:
        jobs_es_engine = get_mozart_es_engine()
        aws_es = app.conf.get("JOBS_AWS_ES", False)
        es_url = hosts or app.conf["JOBS_ES_URL"]
        region = app.conf.get("AWS_REGION", "us-west-2")

        if jobs_es_engine == "opensearch":
            if aws_es is True or "es.amazonaws.com" in es_url:
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
                    sniff_on_start=True,
                )
            else:
                MOZART_ES = OpenSearchUtility(
                    es_url,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    sniff_on_start=True,
                )
        else:
            if aws_es is True or "es.amazonaws.com" in es_url:
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
                    sniff_on_start=True,
                )
            else:
                MOZART_ES = ElasticsearchUtility(
                    es_url,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    sniff_on_start=True,
                )
    return MOZART_ES


def get_grq_es_engine():
    return app.conf.get("GRQ_ES_ENGINE", "elasticsearch")


def get_grq_es(hosts=None):
    global GRQ_ES

    if GRQ_ES is None:
        grq_es_engine = get_grq_es_engine()
        aws_es = app.conf.get("GRQ_AWS_ES", False)
        es_url = hosts or app.conf["GRQ_ES_URL"]
        region = app.conf.get("AWS_REGION", "us-west-2")

        if grq_es_engine == "opensearch":
            if aws_es is True or "es.amazonaws.com" in es_url:
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
                    sniff_on_start=True,
                )
            else:
                GRQ_ES = OpenSearchUtility(
                    es_url,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    sniff_on_start=True,
                )
        else:
            if aws_es is True or "es.amazonaws.com" in es_url:
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
                    sniff_on_start=True,
                )
            else:
                GRQ_ES = ElasticsearchUtility(
                    es_url,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    sniff_on_start=True,
                )
    return GRQ_ES


def get_metrics_es_engine():
    return app.conf.get("METRICS_ES_ENGINE", "elasticsearch")


def get_metrics_es(hosts=None):
    global METRICS_ES

    if METRICS_ES is None:
        grq_es_engine = get_metrics_es_engine()
        aws_es = app.conf.get("METRICS_AWS_ES", False)
        es_url = hosts or app.conf["METRICS_ES_URL"]
        region = app.conf.get("AWS_REGION", "us-west-2")

        if grq_es_engine == "opensearch":
            if aws_es is True or "es.amazonaws.com" in es_url:
                credentials = boto3.Session().get_credentials()
                auth = AWSV4SignerAuth(credentials, region)
                METRICS_ES = OpenSearchUtility(
                    es_url,
                    http_auth=auth,
                    connection_class=RequestsHttpConnectionOS,
                    use_ssl=True,
                    verify_certs=False,
                    ssl_show_warn=False,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    sniff_on_start=True,
                )
            else:
                METRICS_ES = OpenSearchUtility(
                    es_url,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    sniff_on_start=True,
                )
        else:
            if aws_es is True or "es.amazonaws.com" in es_url:
                credentials = boto3.Session().get_credentials()
                auth = AWSV4SignerAuth(credentials, region)
                METRICS_ES = ElasticsearchUtility(
                    es_url,
                    http_auth=auth,
                    connection_class=RequestsHttpConnectionES,
                    use_ssl=True,
                    verify_certs=False,
                    ssl_show_warn=False,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    sniff_on_start=True,
                )
            else:
                METRICS_ES = ElasticsearchUtility(
                    es_url,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    sniff_on_start=True,
                )
    return METRICS_ES
