#!/usr/bin/env python
from future import standard_library

standard_library.install_aliases()
import argparse
import json
import logging
import os
import socket
import sys
import time
from datetime import datetime, UTC

import msgpack
import psutil
from redis import BlockingConnectionPool, StrictRedis

from hysds.celery import app

log_format = "[%(asctime)s: %(levelname)s/log_instance_stats] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


# redis connection pool
POOL = None


def set_redis_pool(redis_url):
    """Set redis connection pool."""

    global POOL
    if POOL is None:
        POOL = BlockingConnectionPool.from_url(redis_url)


def log_instance_stats(redis_url, redis_key, instance_stats):
    """Print instance stats."""

    set_redis_pool(redis_url)
    global POOL
    instance_stats = {
        "type": "instance_stats",
        "@version": "1",
        "@timestamp": f"{datetime.now(UTC).replace(tzinfo=None).isoformat()}Z",
        "stats": instance_stats,
    }

    # send update to redis
    r = StrictRedis(connection_pool=POOL)
    r.rpush(redis_key, msgpack.dumps(instance_stats))

    # print log
    try:
        logging.info(f"instance_stats:{json.dumps(instance_stats)}")
    except Exception as e:
        logging.error(f"Got exception trying to log instance stats: {str(e)}")


def daemon(redis_url, redis_key, interval):
    """Dump instance stats as JSON."""

    while True:
        stats = {
            "host": socket.getfqdn(),
            "host_up": 1,
            "per_cpu": psutil.cpu_percent(interval=1, percpu=True),
            "cpu": psutil.cpu_percent(interval=1),
            "memory": psutil.virtual_memory()._asdict(),
            "swap": psutil.swap_memory()._asdict(),
            "disk": {"all": []},
            "disk_io": psutil.disk_io_counters()._asdict(),
            "net_io": psutil.net_io_counters()._asdict(),
        }
        for device, mnt_point, fs_type, fs_opts, *other in psutil.disk_partitions():
            disk_info = {
                "device": device,
                "mount_point": mnt_point,
                "fs_type": fs_type,
                "fs_opts": fs_opts,
            }
            try:
                disk_info.update(psutil.disk_usage(mnt_point)._asdict())
            except Exception as e:
                logging.error(
                    f"Got exception trying to get disk usage for {mnt_point}: {str(e)}\nSkipping."
                )
                continue
            stats["disk"]["all"].append(disk_info)
            if mnt_point == "/":
                stats["disk"]["root"] = disk_info
            elif mnt_point == "/data":
                stats["disk"]["data"] = disk_info
        log_instance_stats(redis_url, redis_key, stats)
        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Dump instance statistics as JSON log lines."
    )
    parser.add_argument(
        "--redis_url", default=app.conf.REDIS_INSTANCE_METRICS_URL, help="redis URL"
    )
    parser.add_argument(
        "--redis_key", default=app.conf.REDIS_INSTANCE_METRICS_KEY, help="redis key"
    )
    parser.add_argument(
        "--interval", type=int, default=600, help="dump interval in seconds"
    )
    args = parser.parse_args()
    daemon(args.redis_url, args.redis_key, args.interval)
