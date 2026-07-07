import boto3
from elasticsearch import RequestsHttpConnection as RequestsHttpConnectionES
from opensearchpy import AWSV4SignerAuth
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


def assert_doc_settled(es_util, index, doc_id, extra_must=None):
    """Confirm a doc's LATEST write is search-visible, not merely that it exists.

    Search is refresh-bound, so a field UPDATE on a pre-existing doc (e.g. a
    state-config's is_complete flipping false->true) can lag the trigger that
    fired on that write. An _id existence check passes on the stale version and
    the rule query then misses. So: realtime GET-by-id reads the translog
    (refresh-independent) for the authoritative _seq_no, and we require the
    searchable copy's _seq_no to be >= it before proceeding.

    Raises (so the caller's exponential backoff retries) when the doc is not yet
    search-visible or its searchable copy is stale. No fixed sleep, and no per-eval
    _refresh (which would flush a segment across all shards every ingest and cause
    refresh storms under a dense cascade); the realtime GET is single-shard and
    translog-served.

    Shard routing: with replicas, search round-robins across copies, so this probe
    and the caller's rule query could land on different copies -- this one settled,
    that one a lagging replica -- and the rule would miss. So this probe pins
    `preference=doc_id`; the caller MUST pass the same `preference=doc_id` on its
    rule query so both bind to the same copy. (Opaque routing key: same string ->
    same copy; spreads load across copies by doc, unlike `_primary`.)
    """
    must = [{"term": {"_id": doc_id}}]
    if extra_must:
        must.extend(extra_must)
    body = {
        "query": {"bool": {"must": must}},
        "seq_no_primary_term": True,
        "size": 1,
    }
    hits = es_util.search(index=index, body=body, preference=doc_id)["hits"]["hits"]
    if not hits:
        raise RuntimeError(f"doc not yet search-visible: {doc_id}")
    hit = hits[0]
    searchable_seq_no = hit.get("_seq_no")
    latest_seq_no = es_util.es.get(index=hit["_index"], id=doc_id).get("_seq_no")
    if (
        searchable_seq_no is not None
        and latest_seq_no is not None
        and searchable_seq_no < latest_seq_no
    ):
        raise RuntimeError(
            f"doc search copy stale: {doc_id} seq_no {searchable_seq_no} < "
            f"latest {latest_seq_no}; awaiting refresh"
        )


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
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    # sniff_on_start=True,
                )
            else:
                MOZART_ES = OpenSearchUtility(
                    es_url,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    # sniff_on_start=True,
                )
        else:
            if aws_es is True or "es.amazonaws.com" in es_url:
                credentials = boto3.Session().get_credentials()
                auth = AWSV4SignerAuth(credentials, region)
                MOZART_ES = ElasticsearchUtility(
                    es_url,
                    http_auth=auth,
                    connection_class=RequestsHttpConnectionES,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    # sniff_on_start=True,
                )
            else:
                MOZART_ES = ElasticsearchUtility(
                    es_url,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    # sniff_on_start=True,
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
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    # sniff_on_start=True,
                )
            else:
                GRQ_ES = OpenSearchUtility(
                    es_url,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    # sniff_on_start=True,
                )
        else:
            if aws_es is True or "es.amazonaws.com" in es_url:
                credentials = boto3.Session().get_credentials()
                auth = AWSV4SignerAuth(credentials, region)
                GRQ_ES = ElasticsearchUtility(
                    es_url,
                    http_auth=auth,
                    connection_class=RequestsHttpConnectionES,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    # sniff_on_start=True,
                )
            else:
                GRQ_ES = ElasticsearchUtility(
                    es_url,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    # sniff_on_start=True,
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
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    # sniff_on_start=True,
                )
            else:
                METRICS_ES = OpenSearchUtility(
                    es_url,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    # sniff_on_start=True,
                )
        else:
            if aws_es is True or "es.amazonaws.com" in es_url:
                credentials = boto3.Session().get_credentials()
                auth = AWSV4SignerAuth(credentials, region)
                METRICS_ES = ElasticsearchUtility(
                    es_url,
                    http_auth=auth,
                    connection_class=RequestsHttpConnectionES,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    # sniff_on_start=True,
                )
            else:
                METRICS_ES = ElasticsearchUtility(
                    es_url,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True,
                    # sniff_on_start=True,
                )
    return METRICS_ES
