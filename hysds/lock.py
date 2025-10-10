from redis import BlockingConnectionPool, StrictRedis

from pottery import Redlock

from hysds.celery import app

class LockNotAcquiredException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)

class Lock:
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
        self.locker = None

    def acquire_lock(
            self,
            key,
            expire_time=app.conf.get("PUBLISH_LOCK_EXPIRE_TIME", 600),
            wait_time=app.conf.get("PUBLISH_LOCK_WAIT_TIME", 540)
    ):
        self.locker = Redlock(
            key=key,
            masters={self.redis_client},
            auto_release_time=expire_time
        )
        self.lock_status = self.locker.acquire(timeout=wait_time)

        return self.lock_status

    def release(self):
        status = False
        if self.locker:
            locked_result = self.locker.locked()
            if locked_result > 0.0:
                self.locker.release()
                status = True
        return status
