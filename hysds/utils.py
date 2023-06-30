from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import

from builtins import str
from builtins import int
from builtins import open
from future import standard_library

standard_library.install_aliases()

import os
import re
import json
import requests
import math
import backoff
import hashlib
import copy
import errno
import shutil
import traceback
import logging

from datetime import datetime
from subprocess import check_output
from urllib.request import urlopen

from io import StringIO
from lxml.etree import XMLParser, parse, tostring
from importlib import import_module
from celery.result import AsyncResult
from atomicwrites import atomic_write
from bisect import insort

from hysds.log_utils import logger, payload_hash_exists
from hysds.celery import app
from hysds.es_util import get_grq_es, get_mozart_es

import osaka.main

grq_es = get_grq_es()
mozart_es = get_mozart_es()

# disk usage setting converter
DU_CALC = {"GB": 1024 ** 3, "MB": 1024 ** 2, "KB": 1024}


class NoDedupJobFoundException(Exception):
    def __init__(self, message):
        self.message = message
        super(NoDedupJobFoundException, self).__init__(message)


def get_module(m):
    """Import module and return."""

    try:
        return import_module(m)
    except ImportError:
        logger.error('Failed to import module "%s".' % m)
        raise


def get_func(f):
    """Import function and return."""

    if "." in f:
        mod_name, func_name = f.rsplit(".", 1)
        mod = get_module(mod_name)
        try:
            return getattr(mod, func_name)
        except AttributeError:
            logger.error('Failed to get function "%s" from module "%s".' % (func_name, mod_name))
            raise
    else:
        try:
            return eval(f)
        except NameError:
            logger.error('Failed to get function "%s".' % (f))
            raise


@app.task
def error_handler(uuid):
    """Error handler function."""

    result = AsyncResult(uuid)
    exc = result.get(propagate=False)
    logger.info("Task %s raised exception: %s\n%s" % (uuid, exc, result.traceback))


def get_download_params(url):
    """Set osaka download params."""

    params = {}

    # set profile
    for prof in app.conf.get("BUCKET_PROFILES", []):
        if "profile_name" in params:
            break
        if prof.get("bucket_patterns", None) is None:
            params["profile_name"] = prof["profile"]
            break
        else:
            if isinstance(prof["bucket_patterns"], list):
                bucket_patterns = prof["bucket_patterns"]
            else:
                bucket_patterns = [prof["bucket_patterns"]]
            for bucket_pattern in prof["bucket_patterns"]:
                regex = re.compile(bucket_pattern)
                match = regex.search(url)
                if match:
                    logger.info(
                        "{} matched '{}' for profile {}.".format(
                            url, bucket_pattern, prof["profile"]
                        )
                    )
                    params["profile_name"] = prof["profile"]
                    break

    return params


def download_file(url, path, cache=False):
    """
    Download file/dir for input
    @param url: Str
    @param path: Str
    @param cache: Bool (default False) pull from cache
    """

    params = get_download_params(url)
    if cache:
        url_hash = hashlib.md5(url.encode()).hexdigest()
        hash_dir = os.path.join(app.conf.ROOT_WORK_DIR, "cache", *url_hash[0:4])
        cache_dir = os.path.join(hash_dir, url_hash)
        makedirs(cache_dir)
        signal_file = os.path.join(cache_dir, ".localized")
        if os.path.exists(signal_file):
            logger.info("cache hit for {} at {}".format(url, cache_dir))
        else:
            logger.info("cache miss for {}".format(url))
            try:
                logger.info("downloading to cache %s" % url)
                osaka.main.get(url, cache_dir, params=params)
            except Exception as e:
                shutil.rmtree(cache_dir)
                tb = traceback.format_exc()
                raise RuntimeError(
                    "Failed to download {} to cache {}: {}\n{}".format(
                        url, cache_dir, str(e), tb
                    )
                )
            with atomic_write(signal_file, overwrite=True) as f:
                f.write("%sZ\n" % datetime.utcnow().isoformat())
        for i in os.listdir(cache_dir):
            if i == ".localized":
                continue
            cached_obj = os.path.join(cache_dir, i)
            if os.path.isdir(cached_obj):
                dst = os.path.join(path, i) if os.path.isdir(path) else path
                try:
                    os.symlink(cached_obj, dst)
                except Exception:
                    logger.error("Failed to soft link {} to {}".format(cached_obj, dst))
                    raise
            else:
                try:
                    os.symlink(cached_obj, path)
                except Exception:
                    logger.error("Failed to soft link {} to {}".format(cached_obj, path))
                    raise
    else:
        try:
            logger.info("downloading %s" % url)
            return osaka.main.get(url, path, params=params)
        except Exception as e:
            logger.error(e)
            logger.warning("rolling back localized data: {}".format(path))
            shutil.rmtree(path, ignore_errors=True)
            if os.path.exists(path + ".osaka.locked.json"):
                logger.warning(".osaka.locked.json file found, rolling back...")
                shutil.rmtree(path + ".osaka.locked.json")
            raise


def find_cache_dir(cache_dir):
    """Search for *.localized files."""

    cache_dirs = []
    for root, dirs, files in os.walk(cache_dir, followlinks=True):
        files.sort()
        dirs.sort()
        for file in files:
            if file == ".localized":
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
        raise RuntimeError(
            "Failed to determine disk usage requirements from verdi config: {}".format(
                disk_usage
            )
        )
    return math.ceil(float(100) / float(capacity) * du_bytes)


def get_disk_usage(path, follow_symlinks=True):
    """Return disk usage size in bytes."""

    opts = "-sbL" if follow_symlinks else "-sb"
    size = 0
    try:
        size = int(check_output(["du", opts, path]).split()[0])
    except:
        pass
    return size


def makedirs(_dir, mode=0o777):
    """Make directory along with any parent directory that may be needed."""

    try:
        os.makedirs(_dir, mode)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(_dir):
            pass
        else:
            raise  # TODO: raise a more specific error here


def validateDirectory(_dir, mode=0o755, noExceptionRaise=False):
    """Validate that a directory can be written to by the current process and return 1.
    Otherwise, try to create it.  If successful, return 1.  Otherwise return None.
    """

    if os.path.isdir(_dir):
        if os.access(_dir, 7):
            return 1
        else:
            return None
    else:
        try:
            makedirs(_dir, mode)
            os.chmod(_dir, mode)
        except:
            if noExceptionRaise:
                pass
            else:
                raise
        return 1


def getXmlEtree(xml):
    """Return a tuple of [lxml etree element, prefix->namespace dict]."""

    parser = XMLParser(remove_blank_text=True)
    if xml.startswith("<?xml") or xml.startswith("<"):
        return (parse(StringIO(xml), parser).getroot(), getNamespacePrefixDict(xml))
    else:
        if os.path.isfile(xml):
            xmlStr = open(xml).read()
        else:
            xmlStr = urlopen(xml).read()
        return (
            parse(StringIO(xmlStr), parser).getroot(),
            getNamespacePrefixDict(xmlStr),
        )


def getNamespacePrefixDict(xmlString):
    """Take an xml string and return a dict of namespace prefixes to
    namespaces mapping."""

    nss = {}
    defCnt = 0
    matches = re.findall(r'\s+xmlns:?(\w*?)\s*=\s*[\'"](.*?)[\'"]', xmlString)
    for match in matches:
        prefix = match[0]
        ns = match[1]
        if prefix == "":
            defCnt += 1
            prefix = "_" * defCnt
        nss[prefix] = ns
    return nss


def xpath(elt, xp, ns, default=None):
    """
    Run an xpath on an element and return the first result.  If no results
    were returned then return the default value.
    """

    res = elt.xpath(xp, namespaces=ns)
    if len(res) == 0:
        return default
    else:
        return res[0]


def pprintXml(et):
    """Return pretty printed string of xml element."""

    return tostring(et, pretty_print=True)


def parse_iso8601(t):
    """Return datetime from ISO8601 string."""

    try:
        return datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ")


def get_short_error(e):
    """Return shortened version of error message."""

    e_str = str(e)
    if len(e_str) > 35:
        return "%s.....%s" % (e_str[:20], e_str[-10:])
    else:
        return e_str


def get_payload_hash(payload):
    """Return unique hash of HySDS job JSON payload."""

    clean_payload = copy.deepcopy(payload)
    for k in ("_disk_usage", "_sciflo_job_num", "_sciflo_wuid"):
        if k in clean_payload:
            del clean_payload[k]
    return hashlib.md5(
        json.dumps(clean_payload, sort_keys=2, ensure_ascii=True).encode()
    ).hexdigest()


def no_dedup_job(details):
    logger.info("Giving up querying for dedup jobs with args {args} and kwargs {kwargs}".format(**details))
    return None


@backoff.on_exception(
    backoff.expo, requests.exceptions.RequestException, max_tries=8, max_value=32
)
@backoff.on_exception(
    backoff.expo, NoDedupJobFoundException, max_tries=8, max_value=32, on_giveup=no_dedup_job
)
def query_dedup_job(dedup_key, filter_id=None, states=None, is_worker=False):
    """
    Return job IDs with matching dedup key defined in states
    'job-queued', 'job-started', 'job-completed', by default.
    """

    hash_exists_in_redis = payload_hash_exists(dedup_key)
    if hash_exists_in_redis is True:
        logger.info("Payload hash already exists in REDIS: {}".format(dedup_key))
    elif hash_exists_in_redis is False:
        logger.info("Payload hash does not exist in REDIS: {}".format(dedup_key))

    # get states
    if states is None:
        states = ["job-queued", "job-started", "job-completed"]

    # build query
    query = {
        "sort": [{"job.job_info.time_queued": {"order": "asc"}}],
        "size": 1,
        "_source": ["_id", "status"],
        "query": {
            "bool": {
                "must": [
                    {"term": {"payload_hash": dedup_key}},
                    {
                        "bool": {
                            "should": [
                                {"terms": {"status": states}}  # should be an list
                            ]
                        }
                    },
                ]
            }
        },
    }

    if filter_id is not None:
        query["query"]["bool"]["must_not"] = {"term": {"uuid": filter_id}}

    logger.info("constructed query: %s" % json.dumps(query, indent=2))
    j = mozart_es.search(index="job_status-current", body=query)
    logger.info(j)
    if j["hits"]["total"]["value"] == 0:
        if hash_exists_in_redis is True:
            if is_worker:
                return None
            else:
                raise NoDedupJobFoundException("Could not find any dedup jobs with the following query: {}".format(
                    json.dumps(query, indent=2)))
        elif hash_exists_in_redis is False:
            return None
        else:
            raise RuntimeError("Could not determine if payload hash already exists in REDIS: {}".format(dedup_key))
    else:
        hit = j["hits"]["hits"][0]
        logger.info(
            "Found duplicate job: %s" % json.dumps(hit, indent=2, sort_keys=True)
        )
        return {
            "_id": hit["_id"],
            "status": hit["_source"]["status"],
            "query_timestamp": datetime.utcnow().isoformat(),
        }


@backoff.on_exception(
    backoff.expo, requests.exceptions.RequestException, max_tries=8, max_value=32
)
def get_job_status(_id):
    """Get job status."""
    query = {
        "query": {
            "bool": {
                "must": [{"term": {"_id": _id}}]
            }
        }
    }

    res = mozart_es.search(index="job_status-current", body=query, _source_includes=["status"])
    if res["hits"]["total"]["value"] == 0:
        logger.warning("job not found, _id: %s" % _id)
        return None

    logger.info("get_job_status result: %s" % json.dumps(res, indent=2))
    doc = res["hits"]["hits"][0]
    return doc["_source"]["status"]


@backoff.on_exception(
    backoff.expo, requests.exceptions.RequestException, max_tries=8, max_value=32
)
def check_dataset(_id, es_index="grq"):
    """Query for dataset with specified input ID."""

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"_id": _id}},
                ]
            }
        }
    }
    count = grq_es.get_count(index=es_index, body=query)
    return count


def dataset_exists(_id, es_index="grq"):
    """Return true if dataset id exists."""
    return True if check_dataset(_id, es_index) > 0 else False


def init_pool_logger():
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('[%(asctime)s: %(levelname)s/%(name)s] %(message)s'))
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)


def find_dataset_json(work_dir):
    """Search for *.dataset.json files."""

    dataset_re = re.compile(r"^(.*)\.dataset\.json$")
    for root, dirs, files in os.walk(work_dir, followlinks=True):
        files.sort()
        dirs.sort()
        for file in files:
            match = dataset_re.search(file)
            if match:
                dataset_file = os.path.join(root, file)
                prod_dir = os.path.join(os.path.dirname(root), match.group(1))
                if prod_dir != root:
                    logger.info(
                        "%s exists in directory %s. Should be in %s. Not uploading."
                        % (dataset_file, root, prod_dir)
                    )
                elif not os.path.exists(prod_dir):
                    logger.info(
                        "Couldn't find product directory %s for dataset.json %s. Not uploading."
                        % (prod_dir, dataset_file)
                    )
                else:
                    yield dataset_file, prod_dir


def find_non_localized_datasets(work_dir):
    """
    :param work_dir - Str; work directory to traverse for dataset directories
    :return: List[str] - list of dataset directories
    """
    datasets_list = find_dataset_json(work_dir)
    return [prod_dir for _, prod_dir in datasets_list if not os.path.isfile(os.path.join(prod_dir, ".localized"))]


def mark_localized_datasets(job, ctx):
    """Mark localized datasets to prevent republishing."""

    # get job info
    job_dir = job["job_info"]["job_dir"]

    # find localized datasets and mark
    for dataset_file, prod_dir in find_dataset_json(job_dir):
        signal_file = os.path.join(prod_dir, ".localized")
        with atomic_write(signal_file, overwrite=True) as f:
            f.write("%sZ\n" % datetime.utcnow().isoformat())

    # signal run_job() to continue
    return True


def hashlib_mapper(algo):
    """
    :param algo: string
    :return:  hashlib library for specified algorithm

    algorithms available in python3 but not in python2:
        sha3_224 sha3_256, sha3_384, blake2b, blake2s, sha3_512, shake_256, shake_128
    """
    algo = algo.lower()
    if algo == "md5":
        return hashlib.md5()
    elif algo == "sha1":
        return hashlib.sha1()
    elif algo == "sha224":
        return hashlib.sha224()
    elif algo == "sha256":
        return hashlib.sha256()
    elif algo == "sha384":
        return hashlib.sha384()
    elif algo == "sha3_224":
        return hashlib.sha3_224()
    elif algo == "sha3_256":
        return hashlib.sha3_256()
    elif algo == "sha3_384":
        return hashlib.sha3_384()
    elif algo == "sha3_512":
        return hashlib.sha3_512()
    elif algo == "sha512":
        return hashlib.sha512()
    elif algo == "blake2b":
        return hashlib.blake2b()
    elif algo == "blake2s":
        return hashlib.blake2s()
    elif algo == "shake_128":
        return hashlib.shake_128()
    elif algo == "shake_256":
        return hashlib.shake_256()
    else:
        raise Exception("Unsupported hashing algorithm: %s" % algo)


def calculate_checksum_from_localized_file(file_name, hash_algo):
    """
    :param file_name: file path to the localized file after download
    :param hash_algo: string, hashing algorithm (md5, sha256, etc.)
    :return: string, ex. 8e15beebbbb3de0a7dbed50a39b6e41b ALL LOWER CASE

    ******** IF USING SHAKE_256 OR SHAKE_128, I DEFAULT THE HEXDIGEST LENGTH TO 255 ********
    """
    hash_tool = hashlib_mapper(hash_algo)
    with open(file_name, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_tool.update(chunk)

    if hash_tool.name in ("shake_256", "shake_128"):
        return hash_tool.hexdigest(255)
    else:
        return hash_tool.hexdigest()


def check_file_is_checksum(file_path):
    """
    checks if the file has a .hash extension
    hashlib.algorithms_guaranteed is a list of all checksum file extensions
    return algorithm type (md5, sha256, etc) if it file has a .<algorithm> appended
    """
    for algo in hashlib.algorithms_guaranteed:
        checksum_file_extension = ".%s" % algo  # ex. S1W_SLC_843290304820.zip.md5
        if file_path.endswith(checksum_file_extension):
            return algo
    return None


def read_checksum_file(file_path):
    with open(file_path, "r") as f:
        checksum = f.readline().rstrip(
            "\n"
        )  # checksum file is only 1 line, for some reason it adds \n at the end
        return checksum


def generate_list_checksum_files(job):
    """
    :param job: Dict
    :return: list of all checksum files, so we can compare one by one
             ex. list of dictionaries: [ {'file_path': '/home/ops/hysds/...', 'algo': 'md5'}, { ... } ]
    """
    # reusing directory code from the localize_urls() function
    job_dir = job["job_info"]["job_dir"]  # get job info

    files_with_checksum = []
    for i in job["localize_urls"]:
        url = i["url"]
        path = i.get("local_path", None)
        cache = i.get("cache", True)
        if path is None:
            path = "%s/" % job_dir
        else:
            if path.startswith("/"):
                pass
            else:
                path = os.path.join(job_dir, path)
        if os.path.isdir(path) or path.endswith("/"):
            path = os.path.join(path, os.path.basename(url))
        dir_path = os.path.dirname(path)

        if os.path.isdir(path):  # if path is a directory, loop through each file in directory
            for file in os.listdir(path):
                full_file_path = os.path.join(path, file)
                hash_algo = check_file_is_checksum(full_file_path)
                if hash_algo:
                    files_with_checksum.append(
                        {"file_path": full_file_path, "algo": hash_algo}
                    )
        else:  # if path is a actually a file
            hash_algo = check_file_is_checksum(path)
            if hash_algo:
                files_with_checksum.append({"file_path": path, "algo": hash_algo})
    return files_with_checksum


def validate_checksum_files(job, cxt):
    """
    :param job: _job.json
    :param cxt: _context.json
    :return: void, will raise exception if localized files have mismatched checksum values
    """
    # list of dictionaries: ex. [ {'file_path': '/home/ops/hysds/...', 'algo': 'md5'}, { ... } ]
    logger.info("validating checksum files:")
    files_to_validate = generate_list_checksum_files(job)
    logger.info(files_to_validate)

    mismatched_checksums = []
    exception_string = "Files with mismatched checksum:\n"

    logger.info(files_to_validate)
    for file_info in files_to_validate:
        algo = file_info["algo"]
        file_path_checksum = file_info["file_path"]
        # this has the hash extension to the file, we need to remove it
        file_path = file_path_checksum.replace("." + algo, "")

        if not os.path.isfile(file_path):
            # if checksum file exists but original file does not exist, we should skip it
            # ex. data_set_1.zip.md5 vs data_set_1.zip
            logger.info("%s does not exist, skipping" % file_path)
            continue

        calculated_checksum = calculate_checksum_from_localized_file(file_path, algo)
        pre_computed_checksum = read_checksum_file(file_path_checksum)

        logger.info(
            "calculated_checksum: %s pre_computed_checksum: %s"
            % (calculated_checksum, pre_computed_checksum)
        )
        if calculated_checksum.lower() != pre_computed_checksum.lower():
            mismatched_checksums.append(file_path)
            exception_string += (
                "%s: calculated checksum: %s, pre-computed checksum: %s\n"
                % (file_path, calculated_checksum, pre_computed_checksum)
            )

    if len(mismatched_checksums) > 0:
        logger.info(exception_string)
        raise Exception(exception_string)
    else:
        logger.info("checksum preprocessing completed successfully")
    return True


def validate_index_pattern(index):
    """
    validates the elasticsearch index pattern
        - no trailing commas
        - no broad wildcards, ex. '*' or "**"
    :param index: [Str] ES index pattern
    :return: Boolean
    """
    index = index.strip()
    if index.startswith(',') or index.endswith(','):
        return False
    if ''.join(set(index)) == '*':
        return False
    return True
