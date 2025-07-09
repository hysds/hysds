# from builtins import int
# from builtins import open
from future import standard_library

standard_library.install_aliases()

import logging
import os
import traceback
from datetime import timezone, datetime

import backoff
from billiard import Manager, get_context  # noqa
from billiard.pool import Pool, cpu_count  # noqa

from hysds.log_utils import logger
from hysds.utils import download_file, get_disk_usage, makedirs, datetime_iso_naive


def download_file_wrapper_backoff_handler(b, max_tries=6):
    """
    @param b: (Dict) backoff information
        target: function wrapped by backoff
        args: (tuple) function arguments
        kwargs: (Dict) keyword arguments
            cache: Bool (default False) pull from cache
            event: (optional) Manager().event()
        tries: (int) number of tries
        elapsed: (float) function runtime
        wait: (float) wait time after error
        exception: (str) error/traceback raised by the function
    @param max_tries: maximum number of tries allowed by backoff
    """
    tries = b["tries"]
    kwargs = b["kwargs"]
    args = b["args"]
    logger.error(f"download_file_wrapper failed ({tries}) {args} {kwargs}")

    if tries >= max_tries - 1:
        event = kwargs.get("event", None)
        if event:
            event.set()
        exception = b["exception"]
        raise exception


@backoff.on_exception(
    backoff.constant,
    Exception,
    max_tries=6,
    interval=5,
    on_backoff=download_file_wrapper_backoff_handler,
)
def download_file_wrapper(url, path, cache=False, event=None):
    """
    @param url: Str
    @param path: Str
    @param cache: Bool (default False) pull from cache
    @param event: Manager().event() (optional)
    :return: Dict[Str: any] or None
        if successful, will return localized data information
        if None that means a previous task failed and will exit early
    """
    if event and event.is_set():
        logger.warning(f"Previous localize task failed, skipping {url}...")
        return

    loc_t1 = datetime.now(timezone.utc)
    try:
        download_file(url, path, cache=cache)
        loc_t2 = datetime.now(timezone.utc)
        loc_dur = (loc_t2 - loc_t1).total_seconds()
        path_disk_usage = get_disk_usage(path)
        return {
            "url": url,
            "path": path,
            "disk_usage": path_disk_usage,
            "time_start": datetime_iso_naive(loc_t1) + "Z",
            "time_end": datetime_iso_naive(loc_t2) + "Z",
            "duration": loc_dur,
            "transfer_rate": path_disk_usage / loc_dur,
        }
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(tb)
        raise RuntimeError(f"Failed to download {url}: {str(e)}\n{tb}")


def init_pool_logger():
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("[%(asctime)s: %(levelname)s/%(name)s] %(message)s")
    )
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)


def localize_urls_parallel(job, ctx):
    """
    Localize urls for job inputs. Track metrics.
    parallelized
    """
    job_dir = job["job_info"]["job_dir"]  # get job info

    async_tasks = []
    localize_urls_list = job.get("localize_urls", [])
    num_procs = min(max(cpu_count() - 2, 1), len(localize_urls_list))
    logger.info(f"multiprocessing procs used: {num_procs}")

    with get_context("spawn").Pool(
        num_procs, initializer=init_pool_logger
    ) as pool, Manager() as manager:
        event = manager.Event()
        for i in localize_urls_list:  # localize urls
            url = i["url"]
            path = i.get("local_path", None)
            cache = i.get("cache", True)
            if path is None:
                path = f"{job_dir}/"
            else:
                if path.startswith("/"):
                    pass
                else:
                    path = os.path.join(job_dir, path)
            if os.path.isdir(path) or path.endswith("/"):
                path = os.path.join(path, os.path.basename(url))
            dir_path = os.path.dirname(path)
            makedirs(dir_path)

            async_task = pool.apply_async(
                download_file_wrapper,
                args=(
                    url,
                    path,
                ),
                kwds={"cache": cache, "event": event},
            )
            async_tasks.append(async_task)
        pool.close()
        logger.info("Waiting for dataset localization tasks to complete...")
        pool.join()

        logger.handlers.clear()  # clearing the queue and removing the handler to prevent broken pipe error

        has_error, err = False, ""
        for t in async_tasks:
            if t.successful():
                result = t.get()
                if result:
                    job["job_info"]["metrics"]["inputs_localized"].append(result)
            else:
                has_error = True
                logger.error(t._value)  # noqa
                err = t._value  # noqa
        if has_error is True:
            raise RuntimeError(f"Failed to download {err}")

    return True  # signal run_job() to continue


def localize_urls(job, ctx):
    """Localize urls for job inputs. Track metrics."""

    # get job info
    job_dir = job["job_info"]["job_dir"]

    # localize urls
    for i in job["localize_urls"]:
        url = i["url"]
        path = i.get("local_path", None)
        cache = i.get("cache", True)
        if path is None:
            path = f"{job_dir}/"
        else:
            if path.startswith("/"):
                pass
            else:
                path = os.path.join(job_dir, path)
        if os.path.isdir(path) or path.endswith("/"):
            path = os.path.join(path, os.path.basename(url))
        dir_path = os.path.dirname(path)
        makedirs(dir_path)
        localized_metadata = download_file_wrapper(url, path, cache=cache)
        job["job_info"]["metrics"]["inputs_localized"].append(localized_metadata)

    return True  # signal run_job() to continue
