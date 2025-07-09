import backoff
import redis

from redis import BlockingConnectionPool, StrictRedis, RedisError

from hysds.celery import app

from hysds.log_utils import logger

PUBLISH_CONTEXT_HASH_POOL = None


def publish_wait_backoff_max_value():
    """Return max value for backoff."""
    return app.conf.get("PUBLISH_WAIT_BACKOFF_MAX_VALUE", 30)


def publish_wait_backoff_max_time():
    """Return max time for backoff."""
    return app.conf.get("PUBLISH_WAIT_BACKOFF_MAX_TIME", 300)


def dedup_publish_context(details):
    logger.info("Giving up waiting for lock to expire with kwargs {kwargs}".format(**details))
    return None


class NoClobberPublishContextException(Exception):
    pass


class PublishContextLockException(Exception):
    pass


class DedupPublishContextFoundException(Exception):
    def __init__(self, message):
        self.message = message
        super(DedupPublishContextFoundException, self).__init__(message)


class PublishContextLock:
    _connection_pool = None

    @classmethod
    def _get_connection_pool(cls):
        if cls._connection_pool is None:
            cls._connection_pool = BlockingConnectionPool.from_url(
                app.conf.REDIS_JOB_STATUS_URL,
            )
        return cls._connection_pool

    def __init__(self):
        self.redis_client = StrictRedis(
            connection_pool=self._get_connection_pool()
        )
        self.lock_status = None

    @backoff.on_exception(
        backoff.expo, RedisError, max_tries=app.conf.BACKOFF_MAX_TRIES, max_value=app.conf.BACKOFF_MAX_VALUE
    )
    def close(self):
        try:
            self.redis_client.close()
        except redis.ConnectionError as e:
            raise RedisError(f"Error occurred while trying to close the REDIS connection: {str(e)}")


    def get_lock_status(self):
        """ Returns the lock status. 'True' if a lock was successfully acquired. 'None' otherwise."""
        return self.lock_status


    @backoff.on_exception(
        backoff.expo, RedisError, max_tries=app.conf.BACKOFF_MAX_TRIES, max_value=app.conf.BACKOFF_MAX_VALUE
    )
    @backoff.on_exception(
        backoff.expo,
        DedupPublishContextFoundException,
        max_time=publish_wait_backoff_max_time,
        max_value=publish_wait_backoff_max_value,
        on_giveup=dedup_publish_context
    )
    def acquire_lock(self, publish_context_url, task_id, prevent_overwrite=True):

        # According to the REDIS set function, a return value of "True" means that the hash does not exist and it was
        # able to store it successfully. Otherwise, a "None" value is returned, meaning the key/value already exists.
        # This None return value only occurs if nx=True. Otherwise, the record will be overwritten
        self.lock_status = self.redis_client.set(
            publish_context_url,
            task_id,
            #ex=app.conf.PUBLISH_WAIT_STATUS_EXPIRES,
            ex=1200,
            nx=prevent_overwrite
        )
        if self.lock_status is None:
            # If None, that means the lock exists. Check the value (the task_id associated with the lock)
            # and see if it matches with the given task_id. If it matches, then just re-acquire the lock.
            # This is to satisfy the re-delivery use-case.
            value = self.redis_client.get(publish_context_url)
            if value:
                value = value.decode() if hasattr(value, "decode") else value
                if value != task_id:
                    message = (
                        f"Lock exists in REDIS, but for a different task than {task_id}: "
                        f"publish_context_url={publish_context_url}, task_id_in_lock={value}"
                    )
                    logger.warning(message)
                    raise DedupPublishContextFoundException(message)
                else:
                    logger.info(
                        f"Lock already exists in REDIS for this task, {task_id}: "
                        f"publish_context_url={publish_context_url}, task_id_in_lock={value}. Re-acquiring lock."
                    )
                    # The lock being acquired already exists and the task_ids match. So, return True
                    self.lock_status = self.redis_client.set(
                        publish_context_url,
                        task_id,
                        # ex=app.conf.PUBLISH_WAIT_STATUS_EXPIRES,
                        ex=1200,
                        nx=False
                    )

        return self.lock_status


    @backoff.on_exception(
        backoff.expo, RedisError, max_tries=app.conf.BACKOFF_MAX_TRIES, max_value=app.conf.BACKOFF_MAX_VALUE
    )
    def release(self, publish_context_url, task_id):
        """
        Release the lock

        :param publish_context_url: The publish context url
        :param task_id: The task id
        :return: A tuple (number of records deleted, record value)
        """
        lock_task_id = self.redis_client.get(publish_context_url)
        lock_task_id = lock_task_id.decode() if hasattr(lock_task_id, "decode") else lock_task_id
        if lock_task_id == task_id:
            return self.redis_client.delete(publish_context_url), lock_task_id
        else:
            return 0, lock_task_id
