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

    Attempt: an _id can be served by an older attempt's doc while an orphan is
    unreaped, and this probe would then pass on it -- the rule decision would
    describe the wrong failure, invisibly, because the retry job pulls
    retry_job_id from job.job_info.id, which is identical across attempts.
    Callers that know which attempt they mean should pass it through
    extra_must as a uuid term.

    Shard routing: with replicas, search round-robins across copies, so this probe
    and the caller's rule query could land on different copies -- this one settled,
    that one a lagging replica -- and the rule would miss. So this probe pins
    `preference=doc_id`; the caller MUST pass the same `preference=doc_id` on its
    rule query so both bind to the same copy. (Opaque routing key: same string ->
    same copy; spreads load across copies by doc, unlike `_primary`.)

    Index copies: behind an alias or pattern the same _id can live in several
    indices (monthly or rolling indices with stable ids). This probe then sees
    every visible copy, settles on the newest write (@timestamp, stamped at index
    time by grq2 and logstash) and logs a warning naming the indices. It cannot
    see a copy that is not refreshed yet, so a caller that knows which index it
    means should pass that index, not the alias (HC-664).
    """
    must = [{"term": {"_id": doc_id}}]
    if extra_must:
        must.extend(extra_must)
    body = {
        "query": {"bool": {"must": must}},
        "seq_no_primary_term": True,
        "size": 10,
        "_source": ["@timestamp"],
    }
    hits = es_util.search(index=index, body=body, preference=doc_id)["hits"]["hits"]
    if not hits:
        raise RuntimeError(f"doc not yet search-visible: {doc_id}")
    if len(hits) > 1:
        hits = sorted(
            hits,
            key=lambda h: str((h.get("_source") or {}).get("@timestamp") or ""),
            reverse=True,
        )
        logger.warning(
            f"{doc_id}: {len(hits)} documents share this _id in {index} "
            f"({[h.get('_index') for h in hits]}); settling on "
            f"{hits[0].get('_index')}, the newest by @timestamp"
        )
    hit = hits[0]
    searchable_seq_no = hit.get("_seq_no")
    # ignore=[404]: the doc can be deleted between the search and this GET --
    # the orphan reaper removes job_failed docs on its own schedule -- and an
    # unignored 404 would raise NotFoundError, which the caller's backoff then
    # blind-retries for minutes on a deterministic miss. Absent means not
    # settled, which is what the raise below says.
    latest = es_util.es.get(index=hit["_index"], id=doc_id, ignore=[404])
    if not isinstance(latest, dict) or latest.get("found") is False:
        raise RuntimeError(f"doc disappeared while settling: {doc_id}")
    latest_seq_no = latest.get("_seq_no")
    if (
        searchable_seq_no is not None
        and latest_seq_no is not None
        and searchable_seq_no < latest_seq_no
    ):
        raise RuntimeError(
            f"doc search copy stale: {doc_id} seq_no {searchable_seq_no} < "
            f"latest {latest_seq_no}; awaiting refresh"
        )


def search_hits_total(result):
    """hits.total of a search response as an int (a dict on ES 7+/OpenSearch, an
    int on older engines); 0 when absent."""
    total = (result.get("hits") or {}).get("total")
    if isinstance(total, dict):
        total = total.get("value")
    return total if isinstance(total, int) else 0


def is_partial_search_response(result, index=None):
    """True when a 200 search response with NO hits cannot be trusted to mean
    "no match".

    That is the case when the engine says the result is incomplete (timed_out,
    or failed shards) or contradicts itself: a hit count above zero with an
    empty hits list, which a shard whose fetch phase failed or timed out
    produces under load (HC-665). A response WITH a hit is trusted: a rule query
    bound to the triggering _id has found its document.

    index: the concrete index the query was bound to (a term on _index). Failed
    shards that all belong to other indices cannot hide its document, so those
    do not make the response partial. Failures without an index name do.
    """
    hits = result.get("hits") or {}
    if hits.get("hits"):
        return False
    if result.get("timed_out"):
        return True
    if search_hits_total(result) > 0:
        return True
    shards = result.get("_shards") or {}
    if (shards.get("failed") or 0) > 0:
        failures = shards.get("failures")
        if (
            index
            and isinstance(failures, list)
            and failures
            and all(
                isinstance(f, dict) and f.get("index") and f["index"] != index
                for f in failures
            )
        ):
            return False
        return True
    return False


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
