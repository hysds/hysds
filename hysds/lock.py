from redis import BlockingConnectionPool, StrictRedis
import threading
import time
import json
import traceback
import backoff
import celery.states
from pottery import Redlock

from hysds.celery import app
from hysds.log_utils import logger


class LockNotAcquiredException(Exception):
    """Exception raised when a job lock cannot be acquired."""
    def __init__(self, message):
        self.message = message
        super().__init__(message)


class TaskStateIndeterminateException(Exception):
    """Exception raised when task state is still PENDING/unknown during stale lock check."""
    def __init__(self, message, task_id, state):
        self.message = message
        self.task_id = task_id
        self.state = state
        super().__init__(message)


def giveup_task_state_check(details):
    """Called when we give up checking task state."""
    logger.warning(
        f"Gave up checking task state after {details.get('elapsed')}s. "
        f"Args: {details.get('args')}"
    )


class JobLock:
    """
    Enhanced distributed lock for job deduplication with metadata storage,
    heartbeat renewal, and stale lock detection.
    
    This lock prevents duplicate jobs from running by using a payload_id-based
    distributed lock with automatic renewal (heartbeat) and stale lock detection.
    """
    _connection_pool = None
    
    # Redis key templates
    LOCK_KEY_TMPL = "job-lock-{payload_id}"
    LOCK_METADATA_KEY_TMPL = "job-lock-metadata-{payload_id}"

    @classmethod
    def _get_connection_pool(cls):
        if cls._connection_pool is None:
            cls._connection_pool = BlockingConnectionPool.from_url(
                app.conf.REDIS_JOB_STATUS_URL,
                ssl_cert_reqs="none",
            )
            logger.info(f"Created Redis connection pool for JobLock: {app.conf.REDIS_JOB_STATUS_URL}")
        return cls._connection_pool

    def __init__(self, payload_id, task_id, worker_hostname):
        """
        Initialize a JobLock instance.
        
        :param payload_id: The job's payload ID (used as lock key)
        :param task_id: The Celery task ID
        :param worker_hostname: The hostname of the worker running the job
        """
        self.payload_id = payload_id
        self.task_id = task_id
        self.worker_hostname = worker_hostname
        self.lock_key = self.LOCK_KEY_TMPL.format(payload_id=payload_id)
        self.metadata_key = self.LOCK_METADATA_KEY_TMPL.format(payload_id=payload_id)
        
        self.redis_client = StrictRedis(
            connection_pool=self._get_connection_pool(),
            ssl_ciphers=app.conf.get("broker_use_ssl", {}).get("ciphers")
        )
        self.locker = None
        self.heartbeat_thread = None
        self.heartbeat_stop_event = None

    def acquire(self, expire_time=None, wait_time=10):
        """
        Try to acquire the job lock.
        
        :param expire_time: Lock expiration time in seconds (default from config)
        :param wait_time: How long to wait for lock (0 = no wait, fail immediately)
        :return: True if acquired, False otherwise
        """
        if expire_time is None:
            expire_time = app.conf.get("JOB_LOCK_EXPIRE_TIME", 600)
        
        logger.info(
            f"Attempting to acquire lock: key={self.lock_key}, "
            f"task_id={self.task_id}, expire_time={expire_time}s"
        )
        
        self.locker = Redlock(
            key=self.lock_key,
            masters={self.redis_client},
            auto_release_time=expire_time
        )
        
        timeout_param = wait_time
        
        logger.info(f"Calling locker.acquire(timeout={timeout_param})")
        
        try:
            acquired = self.locker.acquire(timeout=timeout_param)
            logger.info(f"locker.acquire() returned: {acquired}")
        except Exception as e:
            logger.error(f"Exception during lock acquisition: {type(e).__name__}: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Re-raise to propagate the error
            raise
        
        logger.info(f"Lock acquisition result: {acquired}")
        
        if acquired:
            # Store metadata about who owns the lock
            metadata = {
                "task_id": self.task_id,
                "worker": self.worker_hostname,
                "acquired_at": time.time(),
                "last_renewed_at": time.time()
            }
            self.redis_client.setex(
                self.metadata_key,
                expire_time,
                json.dumps(metadata)
            )
            logger.info(f"Acquired job lock for payload {self.payload_id} (task {self.task_id})")
        
        return acquired

    def release(self):
        """Release the job lock and clean up metadata."""
        status = False
        
        # Stop heartbeat if running
        self.stop_heartbeat()
        
        # Release the lock
        if self.locker:
            locked_result = self.locker.locked()
            if locked_result > 0.0:
                self.locker.release()
                status = True
                logger.info(f"Released job lock for payload {self.payload_id}")
        
        # Clean up metadata
        try:
            self.redis_client.delete(self.metadata_key)
        except Exception as e:
            logger.warning(f"Failed to delete lock metadata: {e}")
        
        return status

    def extend(self, additional_time=None):
        """
        Extend the lock expiration time.
        
        :param additional_time: Additional time in seconds (default from config)
        :return: True if extended, False otherwise
        """
        if additional_time is None:
            additional_time = app.conf.get("JOB_LOCK_EXPIRE_TIME", 600)
        
        if self.locker:
            try:
                # Extend the Redlock
                self.locker.extend(additional_time=additional_time)
                
                # Update metadata renewal timestamp
                metadata = self.get_lock_metadata()
                if metadata:
                    metadata["last_renewed_at"] = time.time()
                    self.redis_client.setex(
                        self.metadata_key,
                        additional_time,
                        json.dumps(metadata)
                    )
                
                logger.info(f"Extended job lock for payload {self.payload_id}")
                return True
            except Exception as e:
                logger.error(f"Failed to extend lock: {e}")
                return False
        return False

    def start_heartbeat(self, interval=None):
        """
        Start a background thread that renews the lock periodically.
        
        :param interval: Renewal interval in seconds (default from config)
        """
        if interval is None:
            interval = app.conf.get("JOB_LOCK_HEARTBEAT_INTERVAL", 300)
        
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            logger.warning("Heartbeat already running")
            return
        
        self.heartbeat_stop_event = threading.Event()
        self.heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(interval,),
            daemon=True,
            name=f"JobLockHeartbeat-{self.payload_id}"
        )
        self.heartbeat_thread.start()
        logger.info(f"Started heartbeat for job lock {self.payload_id} (interval: {interval}s)")

    def stop_heartbeat(self):
        """Stop the heartbeat renewal thread."""
        if self.heartbeat_stop_event:
            self.heartbeat_stop_event.set()
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=5)
            logger.info(f"Stopped heartbeat for job lock {self.payload_id}")

    def _heartbeat_loop(self, interval):
        """Internal heartbeat loop that runs in a background thread."""
        expire_time = app.conf.get("JOB_LOCK_EXPIRE_TIME", 600)
        
        while not self.heartbeat_stop_event.wait(interval):
            try:
                success = self.extend(additional_time=expire_time)
                if not success:
                    logger.error(f"Failed to renew lock for {self.payload_id}, stopping heartbeat")
                    break
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
                # Continue trying despite errors

    def get_lock_metadata(self):
        """
        Get metadata about the current lock holder.
        
        :return: Dictionary with lock metadata, or None if no lock exists
        """
        try:
            metadata_json = self.redis_client.get(self.metadata_key)
            if metadata_json:
                return json.loads(metadata_json)
        except Exception as e:
            logger.error(f"Failed to get lock metadata: {e}")
        return None

    @backoff.on_exception(
        backoff.expo,
        (ConnectionError, TimeoutError),
        max_tries=8,
        max_value=16,
    )
    @backoff.on_exception(
        backoff.expo,
        TaskStateIndeterminateException,
        max_time=lambda: app.conf.get("JOB_LOCK_STALE_CHECK_TIMEOUT", 60),
        max_value=16,
        on_giveup=giveup_task_state_check,
    )
    def _check_task_state(self, task_id):
        """
        Check task state using Celery's result backend.
        Raises TaskStateIndeterminateException if state is still PENDING/unknown.
        
        :param task_id: The Celery task ID to check
        :return: Tuple of (is_finished, state)
        :raises TaskStateIndeterminateException: If task state is indeterminate
        """
        try:
            task = app.AsyncResult(task_id)
            state = task.state
            
            logger.info(f"Checking task state: task_id={task_id}, state={state}")
            
            # If task is in terminal state, return immediately
            if state in celery.states.READY_STATES:
                logger.info(f"Task {task_id} is in terminal state: {state}")
                return True, state  # Task finished
            
            # If task is actively running, return immediately
            if state in (celery.states.STARTED, celery.states.RETRY):
                logger.info(f"Task {task_id} is active: {state}")
                return False, state  # Task is running
            
            # Task is PENDING or unknown - raise exception to trigger retry
            raise TaskStateIndeterminateException(
                f"Task {task_id} state is indeterminate: {state}",
                task_id=task_id,
                state=state
            )
            
        except TaskStateIndeterminateException:
            # Re-raise to trigger backoff retry
            raise
        except Exception as e:
            # For other exceptions, log and raise TaskStateIndeterminateException
            logger.warning(f"Error checking task state for {task_id}: {e}")
            raise TaskStateIndeterminateException(
                f"Failed to check task state: {e}",
                task_id=task_id,
                state=None
            )

    def is_lock_stale(self):
        """
        Check if lock is stale by examining the Celery state of the task holding the lock.
        Uses exponential backoff to account for status update latency.
        
        :return: True if lock is stale, False otherwise
        """
        metadata = self.get_lock_metadata()
        if not metadata:
            return False
        
        lock_holder_task_id = metadata.get("task_id")
        last_renewed = metadata.get("last_renewed_at", 0)
        acquired_at = metadata.get("acquired_at", 0)
        lock_age = time.time() - last_renewed
        lock_total_age = time.time() - acquired_at
        
        logger.info(
            f"Checking if lock for payload {self.payload_id} is stale. "
            f"Holder: {lock_holder_task_id}, Lock age: {lock_total_age:.0f}s, "
            f"Last renewed: {lock_age:.0f}s ago"
        )
        
        # Fast path: If lock is very old (> 10 min) and not being renewed, it's stale
        if lock_age > 600:
            logger.warning(
                f"Lock not renewed for {lock_age:.0f}s (> 10 min), considering stale"
            )
            return True
        
        # Check task state with backoff retry
        try:
            is_finished, state = self._check_task_state(lock_holder_task_id)
            
            if is_finished:
                logger.warning(
                    f"Lock holder task {lock_holder_task_id} is in terminal state {state}"
                )
                return True
            else:
                logger.info(
                    f"Lock holder task {lock_holder_task_id} is running (state: {state})"
                )
                return False
                
        except TaskStateIndeterminateException as e:
            # Gave up after retries - task state is still indeterminate
            # Fall back to lock age heuristics
            logger.warning(
                f"Could not determine task state for {lock_holder_task_id} "
                f"after retries (state: {e.state}). Falling back to lock age heuristics."
            )
            
            # If lock is very new (< 2 min), give benefit of doubt
            if lock_total_age < 120:
                logger.info("Lock is new, assuming valid despite indeterminate task state")
                return False
            
            # If lock is old with no renewal, probably stale
            if lock_age > 600:
                logger.warning("Old lock with no renewal, considering stale")
                return True
            
            # Check if heartbeat is working (recent renewal = probably alive)
            if lock_age < 360:  # < 6 minutes (heartbeat + buffer)
                logger.info("Recent heartbeat, assuming lock is valid")
                return False
            else:
                logger.warning("No recent heartbeat, considering stale")
                return True

    def force_release(self):
        """
        Force-release a lock (use with caution, typically for stale locks).
        
        :return: True if released, False otherwise
        """
        try:
            # Delete the Redlock key directly
            deleted = self.redis_client.delete(self.lock_key)
            
            # Delete metadata
            self.redis_client.delete(self.metadata_key)
            
            if deleted:
                logger.warning(f"Force-released lock for payload {self.payload_id}")
                return True
        except Exception as e:
            logger.error(f"Failed to force-release lock: {e}")
        
        return False

    @classmethod
    def check_and_break_stale_lock(cls, payload_id, current_task_id=None):
        """
        Check if a lock exists for the payload_id, and if it's stale, break it.
        
        :param payload_id: The payload ID to check
        :param current_task_id: The task_id of the current job attempting to acquire the lock
        :return: True if lock was stale and broken, False otherwise
        """
        # Create a temporary instance just for checking
        temp_lock = cls(payload_id, task_id="check", worker_hostname="checker")
        
        # Check if lock metadata exists
        metadata = temp_lock.get_lock_metadata()
        if not metadata:
            # No lock metadata exists
            logger.info(f"No lock metadata found for payload {payload_id}")
            
            # But check if a Redlock key exists without metadata (orphaned lock)
            lock_key = temp_lock.LOCK_KEY_TMPL.format(payload_id=payload_id)
            lock_exists = temp_lock.redis_client.exists(lock_key)
            logger.info(f"Checking for orphaned lock key: {lock_key}, exists={lock_exists}")
            
            if lock_exists:
                logger.warning(
                    f"Found orphaned lock key without metadata for payload {payload_id}. "
                    f"Breaking orphaned lock."
                )
                return temp_lock.force_release()
            
            logger.info(f"No lock or metadata found for payload {payload_id}, proceeding")
            return False
        
        lock_holder_task_id = metadata.get('task_id')
        logger.info(
            f"Found existing lock for payload {payload_id}: "
            f"task_id={lock_holder_task_id}, worker={metadata.get('worker')}"
        )
        
        # Check if the lock holder is the same as the current task
        # This happens when a job crashed after acquiring the lock but before completing,
        # and is now being re-executed with the same task_id
        if current_task_id and lock_holder_task_id == current_task_id:
            logger.warning(
                f"Lock for payload {payload_id} is held by the current task {current_task_id}. "
                f"This indicates a stale lock from a previous execution attempt. Breaking lock."
            )
            return temp_lock.force_release()
        
        if temp_lock.is_lock_stale():
            # Lock is stale, break it
            logger.warning(
                f"Lock for payload {payload_id} is stale (task {lock_holder_task_id}). "
                f"Breaking stale lock."
            )
            return temp_lock.force_release()
        
        logger.info(f"Lock for payload {payload_id} is valid, not breaking")
        return False
