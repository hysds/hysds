from __future__ import absolute_import
from __future__ import print_function

import os, sys, re, urllib, json, requests, math, backoff, hashlib, copy, errno
import shutil, traceback
from glob import glob
from datetime import datetime
from subprocess import check_output
from urllib2 import urlopen
from urlparse import urlparse,ParseResult
from StringIO import StringIO
from lxml.etree import XMLParser, parse, tostring
from importlib import import_module
from celery.result import AsyncResult
from urlparse import urlparse
from atomicwrites import atomic_write
from bisect import insort

import hysds
from hysds.log_utils import logger, log_prov_es
from hysds.celery import app

import osaka.main

# disk usage setting converter
DU_CALC = {
    "GB": 1024**3,
    "MB": 1024**2,
    "KB": 1024
}


def get_module(m):
    """Import module and return."""

    try:
        return import_module(m)
    except ImportError:
        logger.error('Failed to import module "%s".' % m)
        raise


def get_func(f):
    """Import function and return."""

    if '.' in f:
        mod_name, func_name = f.rsplit('.', 1)
        mod = get_module(mod_name)
        try: return getattr(mod, func_name)
        except AttributeError:
            logger.error('Failed to get function "%s" from module "%s".' % (func_name, mod_name))
            raise
    else:
        try: return eval(f)
        except NameError:
            logger.error('Failed to get function "%s".' % (f))
            raise


@app.task
def error_handler(uuid):
    """Error handler function."""

    result = AsyncResult(uuid) 
    exc = result.get(propagate=False)
    logger.info("Task %s raised exception: %s\n%s" %
                (uuid, exc, result.traceback))


def get_download_params(url):
    """Set osaka download params."""

    params = {}

    # set profile
    for prof in app.conf.get('BUCKET_PROFILES', []):
        if 'profile_name' in params: break
        if prof.get('bucket_patterns', None) is None:
            params['profile_name'] = prof['profile']
            break
        else:
            if isinstance(prof['bucket_patterns'], list):
                bucket_patterns = prof['bucket_patterns']
            else: bucket_patterns = [ prof['bucket_patterns'] ]
            for bucket_pattern in prof['bucket_patterns']:
                regex = re.compile(bucket_pattern)
                match = regex.search(url)
                if match:
                    logger.info("{} matched '{}' for profile {}.".format(url, bucket_pattern, prof['profile']))
                    params['profile_name'] = prof['profile']
                    break
                
    return params

 
def download_file(url, path, cache=False):
    """Download file/dir for input."""

    params = get_download_params(url)
    if cache:
        url_hash = hashlib.md5(url).hexdigest()
        hash_dir = os.path.join(app.conf.ROOT_WORK_DIR, 'cache', *url_hash[0:4])
        cache_dir = os.path.join(hash_dir, url_hash)
        makedirs(cache_dir)
        signal_file = os.path.join(cache_dir, '.localized')
        if os.path.exists(signal_file):
            logger.info("cache hit for {} at {}".format(url, cache_dir))
        else:
            logger.info("cache miss for {}".format(url))
            try: osaka.main.get(url, cache_dir, params=params)
            except Exception, e:
                shutil.rmtree(cache_dir)
                tb = traceback.format_exc()
                raise(RuntimeError("Failed to download %s to cache %s: %s\n%s" % \
                    (url, cache_dir, str(e), tb)))
            with atomic_write(signal_file, overwrite=True) as f:
                f.write("%sZ\n" % datetime.utcnow().isoformat())
        for i in os.listdir(cache_dir):
            if i == '.localized': continue
            cached_obj = os.path.join(cache_dir, i)
            if os.path.isdir(cached_obj):
                dst = os.path.join(path, i) if os.path.isdir(path) else path
                try: os.symlink(cached_obj, dst)
                except:
                    logger.error("Failed to soft link {} to {}".format(cached_obj, dst))
                    raise
            else:
                try: os.symlink(cached_obj, path)
                except:
                    logger.error("Failed to soft link {} to {}".format(cached_obj, path))
                    raise
    else: return osaka.main.get(url, path, params=params)


def find_cache_dir(cache_dir):
    """Search for *.localized files."""

    cache_dirs = []
    for root, dirs, files in os.walk(cache_dir, followlinks=True):
        files.sort()
        dirs.sort()
        for file in files:
            if file == '.localized':
                signal_file = os.path.join(root, file)
                with open(signal_file) as f:
                    timestamp = f.read()
                insort(cache_dirs, (timestamp, signal_file, root))
    return cache_dirs[::-1]


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


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException,
                      max_tries=8, max_value=32)
def get_job_status(id):
    """Get job status."""

    es_url = "%s/job_status-current/job/%s" % (app.conf['JOBS_ES_URL'], id)
    r = requests.get(es_url, params={ 'fields': 'status' })
    logger.info("get_job_status status: %s" % r.status_code)
    result = r.json()
    logger.info("get_job_status result: %s" % json.dumps(result, indent=2))
    return result['fields']['status'][0] if result['found'] else None


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException,
                      max_tries=8, max_value=32)
def check_dataset(id, es_index="grq"):
    """Query for dataset with specified input ID."""

    query = {
        "query":{
            "bool":{
                "must":[
                    {"term":{"_id":id}},
                ]
            }
        },
        "fields": [],
    }
    es_url = app.conf['GRQ_ES_URL']
    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code == 200:
        result = r.json()
        total = result['hits']['total']
    else:
        logger.warn("Failed to query %s:\n%s" % (es_url, r.text))
        logger.warn("query: %s" % json.dumps(query, indent=2))
        logger.warn("returned: %s" % r.text)
        if r.status_code == 404: total = 0
        else: r.raise_for_status()
    return total


def dataset_exists(id, es_index="grq"):
    """Return true if dataset id exists."""

    return True if check_dataset(id, es_index)  > 0 else False


def localize_urls(job, ctx):
    """Localize urls for job inputs. Track metrics."""

    # get job info
    job_dir = job['job_info']['job_dir']

    # localize urls
    for i in job['localize_urls']:
        url = i['url']
        path = i.get('local_path', None)
        cache = i.get('cache', True)
        if path is None: path = '%s/' % job_dir
        else:
            if path.startswith('/'): pass
            else: path = os.path.join(job_dir, path)
        if os.path.isdir(path) or path.endswith('/'):
            path = os.path.join(path, os.path.basename(url))
        dir_path = os.path.dirname(path)
        makedirs(dir_path)
        loc_t1 = datetime.utcnow()
        try: download_file(url, path, cache=cache)
        except Exception, e:
            tb = traceback.format_exc()
            raise(RuntimeError("Failed to download %s: %s\n%s" % (url, str(e), tb)))
        loc_t2 = datetime.utcnow()
        loc_dur = (loc_t2 - loc_t1).total_seconds()
        path_disk_usage = get_disk_usage(path)
        job['job_info']['metrics']['inputs_localized'].append({
            'url': url,
            'path': path,
            'disk_usage': path_disk_usage,
            'time_start': loc_t1.isoformat() + 'Z',
            'time_end': loc_t2.isoformat() + 'Z',
            'duration': loc_dur,
            'transfer_rate': path_disk_usage/loc_dur
        })

    # signal run_job() to continue
    return True


def find_dataset_json(work_dir):
    """Search for *.dataset.json files."""

    dataset_re = re.compile(r'^(.*)\.dataset\.json$')
    for root, dirs, files in os.walk(work_dir, followlinks=True):
        files.sort()
        dirs.sort()
        for file in files:
            match = dataset_re.search(file)
            if match:
                dataset_file = os.path.join(root, file)
                prod_dir = os.path.join(os.path.dirname(root), match.group(1))
                if prod_dir != root:
                    logger.info("%s exists in directory %s. Should be in %s. Not uploading."
                                % (dataset_file, root, prod_dir))
                elif not os.path.exists(prod_dir):
                    logger.info("Couldn't find product directory %s for dataset.json %s. Not uploading."
                                % (prod_dir, dataset_file))
                else: yield (dataset_file, prod_dir)


def publish_dataset(prod_dir, dataset_file, job, ctx):
    """Publish a dataset. Track metrics."""

    # get job info
    job_dir = job['job_info']['job_dir']
    time_start_iso = job['job_info']['time_start']
    context_file = job['job_info']['context_file']
    datasets_cfg_file = job['job_info']['datasets_cfg_file']

    # time start
    time_start = datetime.strptime(time_start_iso, '%Y-%m-%dT%H:%M:%S.%fZ')

    # check for PROV-ES JSON from PGE; if exists, append related PROV-ES info;
    # also overwrite merged PROV-ES JSON file
    prod_id = os.path.basename(prod_dir)
    prov_es_file = os.path.join(prod_dir, "%s.prov_es.json" % prod_id)
    prov_es_info = {}
    if os.path.exists(prov_es_file):
        with open(prov_es_file) as f:
            try: prov_es_info = json.load(f)
            except Exception, e:
                tb = traceback.format_exc()
                raise(RuntimeError("Failed to log PROV-ES from %s: %s\n%s" % (prov_es_file, str(e), tb)))
        log_prov_es(job, prov_es_info, prov_es_file)

    # copy _context.json
    prod_context_file = os.path.join(prod_dir, "%s.context.json" % prod_id)
    shutil.copy(context_file, prod_context_file)

    # upload
    tx_t1 = datetime.utcnow()
    metrics, prod_json = apply(get_func("hysds.dataset_ingest.ingest"),
                               (prod_id, datasets_cfg_file,
                                app.conf.GRQ_UPDATE_URL,
                                app.conf.DATASET_PROCESSED_QUEUE,
                                prod_dir, job_dir))
    tx_t2 = datetime.utcnow()
    tx_dur = (tx_t2 - tx_t1).total_seconds()
    prod_dir_usage = get_disk_usage(prod_dir)

    # set product provenance
    prod_prov = {
        'product_type': metrics['ipath'],
        'processing_start_time': time_start.isoformat() + 'Z',
        'availability_time': tx_t2.isoformat() + 'Z',
        'processing_latency': (tx_t2 - time_start).total_seconds()/60.,
        'total_latency': (tx_t2 - time_start).total_seconds()/60.,
    }
    prod_prov_file = os.path.join(
        prod_dir, "%s.prod_prov.json" % prod_id)
    if os.path.exists(prod_prov_file):
        with open(prod_prov_file) as f:
            prod_prov.update(json.load(f))
    if 'acquisition_start_time' in prod_prov:
        if 'source_production_time' in prod_prov:
            prod_prov['ground_system_latency'] = (
                parse_iso8601(prod_prov['source_production_time']) -
                parse_iso8601(prod_prov['acquisition_start_time'])).total_seconds()/60.
            prod_prov['total_latency'] += prod_prov['ground_system_latency']
            prod_prov['access_latency'] = ( 
                tx_t2 - parse_iso8601(prod_prov['source_production_time'])).total_seconds()/60.
            prod_prov['total_latency'] += prod_prov['access_latency']
    # write product provenance of the last product; not writing to an array under the 
    # product because kibana table panel won't show them correctly:
    # https://github.com/elasticsearch/kibana/issues/998
    job['job_info']['metrics']['product_provenance'] = prod_prov

    job['job_info']['metrics']['products_staged'].append({
        'path': prod_dir,
        'disk_usage': prod_dir_usage,
        'time_start': tx_t1.isoformat() + 'Z',
        'time_end': tx_t2.isoformat() + 'Z',
        'duration': tx_dur,
        'transfer_rate': prod_dir_usage/tx_dur,
        'id': prod_json['id'],
        'urls': prod_json['urls'],
        'browse_urls': prod_json['browse_urls'],
        'dataset': prod_json['dataset'],
        'ipath': prod_json['ipath'],
        'system_version': prod_json['system_version'],
        'dataset_level': prod_json['dataset_level'],
        'dataset_type': prod_json['dataset_type'],
        'index': prod_json['grq_index_result']['index'],
    })

    return prod_json


def publish_datasets(job, ctx):
    """Perform dataset publishing if job exited with zero status code."""

    # if exit code of job command is non-zero, don't publish anything
    exit_code = job['job_info']['status']
    if exit_code != 0:
        logger.info("Job exited with exit code %s. Bypassing dataset publishing." % exit_code)
        return True

    # if job command never ran, don't publish anything
    pid = job['job_info']['pid']
    if pid == 0:
        logger.info("Job command never ran. Bypassing dataset publishing.")
        return True

    # get job info
    job_dir = job['job_info']['job_dir']

    # find and publish
    published_prods = []
    for dataset_file, prod_dir in find_dataset_json(job_dir):

        # skip if marked as localized input
        signal_file = os.path.join(prod_dir, '.localized')
        if os.path.exists(signal_file):
            logger.info("Skipping publish of %s. Marked as localized input." % prod_dir)
            continue

        # publish
        prod_json = publish_dataset(prod_dir, dataset_file, job, ctx)

        # save json for published product
        published_prods.append(prod_json)

    # write published products to file
    pub_prods_file = os.path.join(job_dir, '_datasets.json')
    with open(pub_prods_file, 'w') as f:
        json.dump(published_prods, f, indent=2, sort_keys=True)

    # signal run_job() to continue
    return True


def triage(job, ctx):
    """Triage failed job's context and job json as well as _run.sh."""

    # if exit code of job command is zero, don't triage anything
    exit_code = job['job_info']['status']
    if exit_code == 0:
        logger.info("Job exited with exit code %s. No need to triage." % exit_code)
        return True

    # disable triage
    if ctx.get('_triage_disabled', False):
        logger.info("Flag _triage_disabled set to True. Not performing triage.")
        return True

    # get job info
    job_dir = job['job_info']['job_dir']
    job_id = job['job_info']['id']

    # create triage dataset
    triage_id = "triaged_job-{}".format(job_id)
    triage_dir = os.path.join(job_dir, triage_id)
    makedirs(triage_dir)

    # create dataset json
    ds_file = os.path.join(triage_dir, '{}.dataset.json'.format(triage_id))
    ds = {
        'version': 'v{}'.format(hysds.__version__),
        'label': 'triage for job {}'.format(job_id),
    }
    if 'cmd_start' in job['job_info']:
        ds['starttime'] = job['job_info']['cmd_start']
    if 'cmd_end' in job['job_info']:
        ds['endtime'] = job['job_info']['cmd_end']
    with open(ds_file, 'w') as f:
        json.dump(ds, f, sort_keys=True, indent=2)

    # create met json
    met_file = os.path.join(triage_dir, '{}.met.json'.format(triage_id))
    with open(met_file, 'w') as f:
        json.dump(job['job_info'], f, sort_keys=True, indent=2)

    # triage job-related files
    for f in glob(os.path.join(job_dir, '_*')):
        shutil.copy(f, triage_dir)

    # triage log files
    for f in glob(os.path.join(job_dir, '*.log')):
        shutil.copy(f, triage_dir)

    # triage additional globs
    for g in ctx.get('_triage_additional_globs', []):
        for f in glob(os.path.join(job_dir, g)):
            if os.path.isdir(f):
                shutil.copytree(f, os.path.join(triage_dir, os.path.basename(f)))
            else:
                shutil.copy(f, triage_dir)

    # publish
    prod_json = publish_dataset(triage_dir, ds_file, job, ctx)

    # write published triage to file
    pub_triage_file = os.path.join(job_dir, '_triaged.json')
    with open(pub_triage_file, 'w') as f:
        json.dump(prod_json, f, indent=2, sort_keys=True)

    # signal run_job() to continue
    return True


def mark_localized_datasets(job, ctx):
    """Mark localized datasets to prevent republishing."""

    # get job info
    job_dir = job['job_info']['job_dir']

    # find localized datasets and mark
    for dataset_file, prod_dir in find_dataset_json(job_dir):
        signal_file = os.path.join(prod_dir, '.localized')
        with atomic_write(signal_file, overwrite=True) as f:
            f.write("%sZ\n" % datetime.utcnow().isoformat())

    # signal run_job() to continue
    return True
