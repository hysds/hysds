from redis import BlockingConnectionPool, StrictRedis
import threading
import time
import json
import logging
import traceback
import backoff
import celery.states
from pottery import Redlock

from hysds.celery import app
from hysds.log_utils import logger

# Standard logger for thread-safe logging (doesn't require Celery task context)
thread_logger = logging.getLogger(__name__)


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


class LockNotRenewedException(Exception):
    """Exception raised when lock has not been renewed yet during polling."""
    def __init__(self, message, payload_id, initial_timestamp, current_timestamp):
        self.message = message
        self.payload_id = payload_id
        self.initial_timestamp = initial_timestamp
        self.current_timestamp = current_timestamp
        super().__init__(message)


def giveup_task_state_check(details):
    """Called when we give up checking task state."""
    logger.warning(
        f"Gave up checking task state after {details.get('elapsed')}s. "
        f"Args: {details.get('args')}"
    )


def giveup_lock_renewal_check(details):
    """Called when we give up waiting for lock renewal."""
    logger.warning(
        f"Gave up waiting for lock renewal after {details.get('elapsed'):.1f}s. "
        f"Lock was not renewed - original worker is dead."
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
        
        # Get max_extensions from config
        # Default: 288 extensions = 1 day with 5-minute heartbeat interval
        max_extensions = app.conf.get("JOB_LOCK_MAX_EXTENSIONS", 288)
        
        self.locker = Redlock(
            key=self.lock_key,
            masters={self.redis_client},
            auto_release_time=expire_time,
            num_extensions=max_extensions
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
                # Extend the Redlock (extends by original auto_release_time)
                self.locker.extend()
                
                # Update metadata renewal timestamp
                # Keep metadata TTL in sync with lock TTL
                metadata = self.get_lock_metadata()
                if metadata:
                    metadata["last_renewed_at"] = time.time()
                    self.redis_client.setex(
                        self.metadata_key,
                        additional_time,
                        json.dumps(metadata)
                    )
                
                thread_logger.info(f"Extended job lock for payload {self.payload_id} (task {self.task_id})")
                return True
            except Exception as e:
                thread_logger.error(f"Failed to extend lock for payload {self.payload_id} (task {self.task_id}): {e}")
                return False
        return False

    def start_heartbeat(self, interval=None):
        """
        Start a background thread that renews the lock periodically.
        
        :param interval: Renewal interval in seconds (default from config)
        """
        if interval is None:
            interval = app.conf.get("JOB_LOCK_HEARTBEAT_INTERVAL", 300)
        
        # Check if heartbeat is already running
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            logger.warning(
                f"Heartbeat already running for {self.payload_id}. "
                f"Not starting a new thread."
            )
            return
        
        # Clean up any stopped thread references before starting new one
        if self.heartbeat_thread and not self.heartbeat_thread.is_alive():
            logger.info(f"Cleaning up stopped heartbeat thread for {self.payload_id}")
            self.heartbeat_thread = None
            self.heartbeat_stop_event = None
        
        # Create and start new heartbeat thread
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
        if not self.heartbeat_stop_event:
            # No heartbeat running
            return
        
        # Signal thread to stop
        self.heartbeat_stop_event.set()
        
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            logger.info(f"Stopping heartbeat for job lock {self.payload_id}")
            self.heartbeat_thread.join(timeout=5)
            
            # Verify thread actually stopped
            if self.heartbeat_thread.is_alive():
                logger.error(
                    f"Heartbeat thread for {self.payload_id} did not stop within timeout. "
                    f"Thread will continue as daemon but lock is released."
                )
            else:
                logger.info(f"Stopped heartbeat for job lock {self.payload_id}")
        
        # Clean up references to help garbage collection
        self.heartbeat_thread = None
        self.heartbeat_stop_event = None

    def _heartbeat_loop(self, interval):
        """Internal heartbeat loop that runs in a background thread."""
        expire_time = app.conf.get("JOB_LOCK_EXPIRE_TIME", 600)
        consecutive_failures = 0
        max_failures = 3
        
        thread_logger.info(f"Heartbeat loop started for payload {self.payload_id} (task {self.task_id})")
        
        try:
            while not self.heartbeat_stop_event.wait(interval):
                try:
                    success = self.extend(additional_time=expire_time)
                    if not success:
                        consecutive_failures += 1
                        thread_logger.error(
                            f"Failed to renew lock for payload {self.payload_id} (task {self.task_id}) "
                            f"({consecutive_failures}/{max_failures})"
                        )
                        if consecutive_failures >= max_failures:
                            thread_logger.error(
                                f"Max consecutive failures reached for payload {self.payload_id} (task {self.task_id}), "
                                f"stopping heartbeat"
                            )
                            break
                    else:
                        # Reset failure counter on success
                        consecutive_failures = 0
                except Exception as e:
                    consecutive_failures += 1
                    thread_logger.error(
                        f"Error in heartbeat loop for payload {self.payload_id} (task {self.task_id}): {e} "
                        f"({consecutive_failures}/{max_failures})"
                    )
                    if consecutive_failures >= max_failures:
                        thread_logger.error(
                            f"Max consecutive errors reached for payload {self.payload_id} (task {self.task_id}), "
                            f"stopping heartbeat"
                        )
                        break
                    # Continue trying despite errors
        finally:
            thread_logger.info(f"Heartbeat loop exiting for payload {self.payload_id} (task {self.task_id})")

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
    
    def _wait_for_lock_renewal(self, initial_last_renewed, max_wait_time=60):
        """
        Wait and poll to see if the lock gets renewed by the original worker.
        Uses exponential backoff (via decorator) to check if last_renewed_at changes.
        
        :param initial_last_renewed: The initial last_renewed_at timestamp
        :param max_wait_time: Maximum time to wait in seconds
        :return: True if lock was renewed (worker is alive), False if not renewed (worker is dead)
        """
        logger.info(
            f"Waiting up to {max_wait_time:.0f}s to see if lock for {self.payload_id} gets renewed "
            f"(initial last_renewed: {initial_last_renewed})"
        )
        
        # Create a decorated check function with the specific max_wait_time
        @backoff.on_exception(
            backoff.expo,
            LockNotRenewedException,
            max_time=max_wait_time,
            max_value=8,  # Cap backoff at 8 seconds
            on_giveup=giveup_lock_renewal_check,
            logger=None,  # Disable backoff's built-in retry logging to avoid log spam
        )
        def check_renewal():
            """Inner function to check lock renewal with backoff."""
            # Check if lock metadata was renewed
            metadata = self.get_lock_metadata()
            if not metadata:
                # Lock was released, treat as stale (worker is dead)
                logger.warning(f"Lock metadata disappeared during wait, lock released")
                return False
            
            current_last_renewed = metadata.get('last_renewed_at', metadata.get('acquired_at', 0))
            
            if current_last_renewed > initial_last_renewed:
                # Lock was renewed! Worker is alive
                logger.info(
                    f"Lock for {self.payload_id} was renewed "
                    f"(timestamp: {initial_last_renewed} → {current_last_renewed}). "
                    f"Original worker is alive."
                )
                return True
            
            # Lock not renewed yet, raise exception to trigger retry with backoff
            raise LockNotRenewedException(
                f"Lock for {self.payload_id} not renewed yet",
                payload_id=self.payload_id,
                initial_timestamp=initial_last_renewed,
                current_timestamp=current_last_renewed
            )
        
        try:
            # This will retry with exponential backoff until max_wait_time
            return check_renewal()
        except LockNotRenewedException:
            # Gave up after max_wait_time - lock was not renewed
            logger.warning(
                f"Lock for {self.payload_id} was NOT renewed within {max_wait_time:.0f}s. "
                f"Original worker is considered dead."
            )
            return False

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
        
        # Get config values
        expire_time = app.conf.get("JOB_LOCK_EXPIRE_TIME", 600)
        grace_period = app.conf.get("JOB_LOCK_NEW_GRACE_PERIOD", 120)
        heartbeat_interval = app.conf.get("JOB_LOCK_HEARTBEAT_INTERVAL", 300)
        
        # Fast path: If lock is very old and not being renewed, it's stale
        if lock_age > expire_time:
            logger.warning(
                f"Lock not renewed for {lock_age:.0f}s (> {expire_time}s), considering stale"
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
            
            # If lock is very new, give benefit of doubt
            if lock_total_age < grace_period:
                logger.info(
                    f"Lock is new ({lock_total_age:.0f}s < {grace_period}s grace period), "
                    f"assuming valid despite indeterminate task state"
                )
                return False
            
            # If lock is old with no renewal, probably stale
            if lock_age > expire_time:
                logger.warning(
                    f"Old lock with no renewal ({lock_age:.0f}s > {expire_time}s), "
                    f"considering stale"
                )
                return True
            
            # Check if heartbeat is working (recent renewal = probably alive)
            # Use heartbeat interval + 20% buffer to account for timing variations
            heartbeat_threshold = heartbeat_interval * 1.2
            if lock_age < heartbeat_threshold:
                logger.info(
                    f"Recent heartbeat ({lock_age:.0f}s < {heartbeat_threshold:.0f}s threshold), "
                    f"assuming lock is valid"
                )
                return False
            else:
                logger.warning(
                    f"No recent heartbeat ({lock_age:.0f}s >= {heartbeat_threshold:.0f}s threshold), "
                    f"considering stale"
                )
                return True

    def force_release(self):
        """
        Force-release a lock (use with caution, typically for stale locks).
        
        :return: True if released, False otherwise
        """
        try:
            # Pottery's Redlock adds a "redlock:" prefix to the key
            redlock_key = f"redlock:{self.lock_key}"
            
            logger.info(f"Attempting to force-release lock: key={redlock_key}, metadata_key={self.metadata_key}")
            
            # Check if keys exist before deleting
            lock_exists = self.redis_client.exists(redlock_key)
            metadata_exists = self.redis_client.exists(self.metadata_key)
            logger.info(f"Before deletion - lock_exists={lock_exists}, metadata_exists={metadata_exists}")
            
            # Delete the Redlock key directly (with the redlock: prefix)
            deleted_lock = self.redis_client.delete(redlock_key)
            logger.info(f"Deleted lock key: {deleted_lock} key(s) removed")
            
            # Delete metadata
            deleted_metadata = self.redis_client.delete(self.metadata_key)
            logger.info(f"Deleted metadata key: {deleted_metadata} key(s) removed")
            
            if deleted_lock > 0:
                logger.warning(f"Force-released lock for payload {self.payload_id}")
                return True
            else:
                logger.warning(f"Force-release failed: lock key {redlock_key} did not exist or could not be deleted")
                return False
        except Exception as e:
            logger.error(f"Failed to force-release lock: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
        
        return False

    @classmethod
    def check_and_break_stale_lock(cls, payload_id, current_task_id=None, current_hostname=None):
        """
        Check if a lock exists for the payload_id, and if it's stale, break it.
        
        :param payload_id: The payload ID to check
        :param current_task_id: The task_id of the current job attempting to acquire the lock
        :param current_hostname: The hostname of the current worker attempting to acquire the lock
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
            # Pottery's Redlock adds a "redlock:" prefix to the key
            lock_key = temp_lock.LOCK_KEY_TMPL.format(payload_id=payload_id)
            redlock_key = f"redlock:{lock_key}"
            lock_exists = temp_lock.redis_client.exists(redlock_key)
            logger.info(f"Checking for orphaned lock key: {redlock_key}, exists={lock_exists}")
            
            if lock_exists:
                logger.warning(
                    f"Found orphaned lock key without metadata for payload {payload_id}. "
                    f"Breaking orphaned lock."
                )
                return temp_lock.force_release()
            
            logger.info(f"No lock or metadata found for payload {payload_id}, proceeding")
            return False
        
        lock_holder_task_id = metadata.get('task_id')
        lock_holder_hostname = metadata.get('worker')
        logger.info(
            f"Found existing lock for payload {payload_id}: "
            f"task_id={lock_holder_task_id}, worker={lock_holder_hostname}"
        )
        
        # Check if the lock holder has the same task_id as the current task
        # This happens in two scenarios:
        # 1. Job crashed and is restarting on the SAME worker (hostname matches)
        # 2. RabbitMQ redelivered job to a DIFFERENT worker (hostname differs)
        if current_task_id and lock_holder_task_id == current_task_id:
            lock_age = time.time() - metadata.get('last_renewed_at', metadata.get('acquired_at', 0))
            heartbeat_interval = app.conf.get("JOB_LOCK_HEARTBEAT_INTERVAL", 300)
            
            # CRITICAL: Check if hostnames differ
            # Different hostname = RabbitMQ requeued to different worker
            if current_hostname and lock_holder_hostname != current_hostname:
                logger.warning(
                    f"Lock for payload {payload_id} held by task {current_task_id} on "
                    f"DIFFERENT worker (holder: {lock_holder_hostname}, current: {current_hostname}). "
                    f"This indicates RabbitMQ redelivery to a different worker. "
                    f"Lock age: {lock_age:.0f}s, last renewed {lock_age:.0f}s ago."
                )
                
                # IMPORTANT: Cannot rely on task state check here because both workers
                # share the same task_id, and the current worker's STARTED state will
                # overwrite the previous worker's state in Celery's result backend.
                # Use lock age and heartbeat as the sole indicator of staleness.
                
                # If lock was renewed very recently (within heartbeat interval), we're in an
                # ambiguous state: the original worker might be alive (heartbeat pending) or
                # dead (crashed before heartbeat). Wait and observe if lock gets renewed.
                if lock_age < heartbeat_interval:
                    logger.warning(
                        f"Lock was renewed {lock_age:.0f}s ago (< {heartbeat_interval}s heartbeat interval). "
                        f"Ambiguous state: original worker may be alive or dead. Waiting to observe..."
                    )
                    
                    # Calculate how long to wait: time until next expected heartbeat + small buffer
                    time_until_heartbeat = heartbeat_interval - lock_age
                    buffer = 10  # Grace period after expected heartbeat
                    
                    # Wait for the full time until heartbeat is expected, plus buffer
                    # This is critical: if workers started simultaneously, we need to wait the full interval
                    wait_time = time_until_heartbeat + buffer
                    
                    # Respect absolute maximum from config (safety cap)
                    max_wait = app.conf.get("JOB_LOCK_REDELIVERY_WAIT_MAX", None)
                    if max_wait is not None:
                        wait_time = min(wait_time, max_wait)
                        if wait_time < time_until_heartbeat:
                            logger.warning(
                                f"JOB_LOCK_REDELIVERY_WAIT_MAX ({max_wait}s) is less than time until "
                                f"expected heartbeat ({time_until_heartbeat:.0f}s). May incorrectly "
                                f"break lock of active worker!"
                            )
                    
                    logger.info(
                        f"Waiting {wait_time:.0f}s to see if lock gets renewed "
                        f"(next heartbeat expected in {time_until_heartbeat:.0f}s, buffer: {buffer}s)"
                    )
                    
                    initial_last_renewed = metadata.get('last_renewed_at', metadata.get('acquired_at', 0))
                    lock_was_renewed = temp_lock._wait_for_lock_renewal(initial_last_renewed, max_wait_time=wait_time)
                    
                    if lock_was_renewed:
                        logger.info(
                            f"Lock for {payload_id} was renewed by original worker {lock_holder_hostname}. "
                            f"Original worker is alive. NOT breaking lock."
                        )
                        return False  # Worker is alive, don't break lock
                    else:
                        logger.warning(
                            f"Lock for {payload_id} was NOT renewed after waiting {wait_time:.0f}s. "
                            f"Original worker {lock_holder_hostname} is dead. Breaking stale lock."
                        )
                        result = temp_lock.force_release()
                        logger.info(f"force_release() returned: {result}")
                        return result
                
                # Lock hasn't been renewed for at least one heartbeat interval
                # Original worker is definitely dead (missed at least one heartbeat)
                logger.warning(
                    f"Lock not renewed for {lock_age:.0f}s (>= {heartbeat_interval}s heartbeat interval). "
                    f"Original worker missed heartbeat and is considered dead. Breaking stale lock immediately."
                )
                result = temp_lock.force_release()
                logger.info(f"force_release() returned: {result}")
                return result
            else:
                # Same hostname OR hostname not provided - extra safety check using heartbeat
                # If lock was renewed very recently, the previous execution might still be alive
                # Give it a buffer of 2x heartbeat interval to account for processing delays
                if lock_age < (heartbeat_interval * 2):
                    logger.warning(
                        f"Lock for payload {payload_id} is held by current task {current_task_id} "
                        f"on same worker {lock_holder_hostname}, "
                        f"and was renewed {lock_age:.0f}s ago (< {heartbeat_interval * 2}s threshold). "
                        f"Previous execution may still be active. NOT breaking lock."
                    )
                    return False
                
                logger.warning(
                    f"Lock for payload {payload_id} is held by the current task {current_task_id} "
                    f"on same worker {lock_holder_hostname}. "
                    f"Last renewed {lock_age:.0f}s ago. "
                    f"This indicates a stale lock from a previous execution attempt. Breaking lock."
                )
                result = temp_lock.force_release()
                logger.info(f"force_release() returned: {result}")
                return result
        
        if temp_lock.is_lock_stale():
            # Lock is stale, break it
            logger.warning(
                f"Lock for payload {payload_id} is stale (task {lock_holder_task_id}). "
                f"Breaking stale lock."
            )
            return temp_lock.force_release()
        
        logger.info(f"Lock for payload {payload_id} is valid, not breaking")
        return False
