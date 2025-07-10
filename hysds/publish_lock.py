import backoff
import redis

from redis import BlockingConnectionPool, StrictRedis, RedisError

from pottery import Redlock

from hysds.celery import app

from hysds.log_utils import logger


def publish_wait_backoff_max_time():
    """Return max time for backoff."""
    return app.conf.get("PUBLISH_WAIT_BACKOFF_MAX_TIME", 300)


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
        self.publish_lock = None

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
    def acquire_lock(self, publish_context_url):
        self.publish_lock = Redlock(
            key=publish_context_url,
            masters={self.redis_client},
            auto_release_time=app.conf.get("PUBLISH_WAIT_STATUS_EXPIRES", 600)
        )
        logger.info(f"Acquiring lock for {publish_context_url}")
        self.lock_status = self.publish_lock.acquire(timeout=publish_wait_backoff_max_time())
        if self.lock_status is False:
            raise DedupPublishContextFoundException(
                f"Could not successfully acquire lock for {publish_context_url}. "
                f"Lock still exists even after waiting {publish_wait_backoff_max_time()} seconds."
            )

        return self.lock_status


    @backoff.on_exception(
        backoff.expo, RedisError, max_tries=app.conf.BACKOFF_MAX_TRIES, max_value=app.conf.BACKOFF_MAX_VALUE
    )
    def release(self):
        """
        Release the lock

        :return: True if there was a current lock and we successfully released it, False otherwise
        """
        status = False
        if self.publish_lock:
            locked_result = self.publish_lock.locked()
            if locked_result > 0.0:
                self.publish_lock.release()
                status = True
        return status
