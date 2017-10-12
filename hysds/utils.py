from __future__ import absolute_import
from __future__ import print_function

import os, sys, re, urllib, json, requests, math, backoff, hashlib, copy, errno
from datetime import datetime
from subprocess import check_output
from urllib2 import urlopen
from urlparse import urlparse,ParseResult
from StringIO import StringIO
from lxml.etree import XMLParser, parse, tostring
from celery.result import AsyncResult
from urlparse import urlparse
from hysds.log_utils import logger
from hysds.celery import app

import osaka.main

# disk usage setting converter
DU_CALC = {
    "GB": 1024**3,
    "MB": 1024**2,
    "KB": 1024
}


@app.task
def error_handler(uuid):
    """Error handler function."""

    result = AsyncResult(uuid) 
    exc = result.get(propagate=False)
    logger.info("Task %s raised exception: %s\n%s" %
                (uuid, exc, result.traceback))


class download_file_error(Exception): pass


def download_file(url, path, oauth_url=None, s3_region=None):
    """Download file for input."""
    return osaka.main.get(url, path, params={ "oauth": oauth_url,
                                              "s3-region": s3_region })


def disk_space_info(path):
    """Return disk usage info."""

    disk = os.statvfs(path)
    capacity = disk.f_frsize * disk.f_blocks
    free = disk.f_frsize * disk.f_bavail
    used = disk.f_frsize * (disk.f_blocks - disk.f_bavail)
    percent_free = math.ceil(float(100) / float(capacity) * free)
    return capacity, free, used, percent_free


def get_threshold(path, disk_usage):
    """Return required threshold based on disk usage of a job type."""

    capacity, free, used, percent_free = disk_space_info(path)
    du_bytes = None
    for unit in DU_CALC:
        if disk_usage.endswith(unit):
            du_bytes = int(disk_usage[0:-2]) * DU_CALC[unit]
            break
    if du_bytes is None:
        raise RuntimeError("Failed to determine disk usage requirements from verdi config: %s" % disk_usage)
    return math.ceil(float(100) / float(capacity) * du_bytes)


def get_disk_usage(path):
    """Return disk size, "du -sk", for a path."""

    size = 0
    try:
        size = int(check_output(['du', '-sk', path]).split()[0]) * DU_CALC['KB']
    except: pass
    return size


def makedirs(dir, mode=0777):
    """Make directory along with any parent directory that may be needed."""

    try: os.makedirs(dir, mode)
    except OSError, e:
        if e.errno == errno.EEXIST and os.path.isdir(dir): pass
        else: raise

    
def validateDirectory(dir, mode=0755, noExceptionRaise=False):
    """Validate that a directory can be written to by the current process and return 1.
    Otherwise, try to create it.  If successful, return 1.  Otherwise return None.
    """

    if os.path.isdir(dir):
        if os.access(dir, 7): return 1
        else: return None
    else:
        try:
            makedirs(dir, mode)
            os.chmod(dir, mode)
        except:
            if noExceptionRaise: pass
            else: raise
        return 1


def getXmlEtree(xml):
    """Return a tuple of [lxml etree element, prefix->namespace dict].
    """

    parser = XMLParser(remove_blank_text=True)
    if xml.startswith('<?xml') or xml.startswith('<'):
        return (parse(StringIO(xml), parser).getroot(),
                getNamespacePrefixDict(xml))
    else:
        if os.path.isfile(xml): xmlStr = open(xml).read()
        else: xmlStr = urlopen(xml).read()
        return (parse(StringIO(xmlStr), parser).getroot(),
                getNamespacePrefixDict(xmlStr))


def getNamespacePrefixDict(xmlString):
    """Take an xml string and return a dict of namespace prefixes to
    namespaces mapping."""
    
    nss = {} 
    defCnt = 0
    matches = re.findall(r'\s+xmlns:?(\w*?)\s*=\s*[\'"](.*?)[\'"]', xmlString)
    for match in matches:
        prefix = match[0]; ns = match[1]
        if prefix == '':
            defCnt += 1
            prefix = '_' * defCnt
        nss[prefix] = ns
    return nss


def xpath(elt, xp, ns, default=None):
    """
    Run an xpath on an element and return the first result.  If no results
    were returned then return the default value.
    """
    
    res = elt.xpath(xp, namespaces=ns)
    if len(res) == 0: return default
    else: return res[0]
    

def pprintXml(et):
    """Return pretty printed string of xml element."""
    
    return tostring(et, pretty_print=True)


def parse_iso8601(t):
    """Return datetime from ISO8601 string."""

    return datetime.strptime(t, '%Y-%m-%dT%H:%M:%S.%fZ')


def get_short_error(e):
    """Return shortened version of error message."""

    e_str = str(e)
    if len(e_str) > 35:
        return "%s.....%s" % (e_str[:20], e_str[-10:])
    else: return e_str


def get_payload_hash(payload):
    """Return unique hash of HySDS job JSON payload."""

    clean_payload = copy.deepcopy(payload)
    for k in ('_disk_usage', '_sciflo_job_num', '_sciflo_wuid'):
        if k in clean_payload: del clean_payload[k]
    return hashlib.md5(json.dumps(clean_payload, sort_keys=2, 
                                  ensure_ascii=True)).hexdigest()


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException,
                      max_tries=8, max_value=32)
def query_dedup_job(dedup_key, filter_id=None, states=None):
    """
    Return job IDs with matching dedup key defined in states
    'job-queued', 'job-started', 'job-completed', by default.
    """

    # get states
    if states is None: states = [ 'job-queued', 'job-started', 'job-completed' ]

    # build query
    query = {
        "sort" : [ { "job.job_info.time_queued" : {"order" : "asc"} } ],
        "size": 1,
        "fields": [ "_id", "status" ],
        "query": {
            "filtered": {
                "filter": {
                    "bool": {
                        "must": {
                            "term": {
                                "payload_hash": dedup_key
                            }
                        }
                    }
                }
            }
        }
    }
    for state in states:
        query['query']['filtered']['filter']['bool'].setdefault('should', []).append({
            "term": {
                "status": state
            }
        })
    if filter_id is not None:
        query['query']['filtered']['filter']['bool']['must_not'] = {
            "term": {
                "uuid": filter_id
            }
        }
    es_url = "%s/job_status-current/_search" % app.conf['JOBS_ES_URL']
    r = requests.post(es_url, data=json.dumps(query))
    if r.status_code != 200:
        if r.status_code == 404: pass
        else: r.raise_for_status()
    hits = []
    j = r.json()
    if j.get('hits', {}).get('total', 0) == 0:
        return None
    else:
        hit = j['hits']['hits'][0]
        logger.info("Found duplicate job: %s" % json.dumps(hit, indent=2, sort_keys=True))
        return { '_id': hit['_id'],
                 'status': hit['fields']['status'][0],
                 'query_timestamp': datetime.utcnow().isoformat() }
