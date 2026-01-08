"""
Unit tests for the JobLock distributed locking mechanism.

These tests use mocked Redis and time to run quickly (< 15 seconds total).
"""

import json
import sys
import time
import threading
from unittest import TestCase

try:
    import unittest.mock as umock
except ImportError:
    from unittest import mock as umock

# Mock hysds.celery before importing lock module
sys.modules["hysds.celery"] = umock.MagicMock()
sys.modules["opensearchpy"] = umock.Mock()

import fakeredis
import pytest
from freezegun import freeze_time
from pottery import Redlock
from pottery.exceptions import ExtendUnlockedLock, TooManyExtensions

from hysds.lock import JobLock, LockNotAcquiredException, TaskStateIndeterminateException


class TestJobLockBasicOperations(TestCase):
    """Test basic lock acquire/release/extend operations."""
    
    def setUp(self):
        """Set up test fixtures with fake Redis."""
        self.fake_redis = fakeredis.FakeStrictRedis()
        self.payload_id = "test-payload-123"
        self.task_id = "test-task-456"
        self.hostname = "test-worker-1"
        
        # Patch Redis connection pool to use fake Redis
        self.redis_patcher = umock.patch.object(
            JobLock,
            '_get_connection_pool',
            return_value=self.fake_redis
        )
        self.redis_patcher.start()
        
        # Mock app.conf with production defaults
        from hysds import celery
        celery.app.conf.JOB_LOCK_EXPIRE_TIME = 600
        celery.app.conf.JOB_LOCK_HEARTBEAT_INTERVAL = 30
        celery.app.conf.JOB_LOCK_MAX_EXTENSIONS = 4320
        celery.app.conf.JOB_LOCK_HEARTBEAT_MAX_FAILURES = 3
        celery.app.conf.JOB_LOCK_STALE_CHECK_RETRIES = 3
        
    def tearDown(self):
        """Clean up patches."""
        self.redis_patcher.stop()
        umock.patch.stopall()
    
    def test_acquire_lock_success(self):
        """Test successfully acquiring a lock when no lock exists."""
        lock = JobLock(self.payload_id, self.task_id, self.hostname)
        
        # Should acquire successfully
        result = lock.acquire(wait_time=0)
        self.assertTrue(result)
        
        # Verify Redis keys exist
        redlock_key = f"redlock:job-lock-{self.payload_id}"
        metadata_key = f"job-lock-metadata-{self.payload_id}"
        
        self.assertEqual(self.fake_redis.exists(redlock_key), 1)
        self.assertEqual(self.fake_redis.exists(metadata_key), 1)
        
        # Verify TTL is set correctly
        ttl = self.fake_redis.ttl(redlock_key)
        self.assertGreater(ttl, 590)  # Should be ~600s
        self.assertLessEqual(ttl, 600)
        
        # Verify metadata content
        metadata_json = self.fake_redis.get(metadata_key)
        metadata = json.loads(metadata_json)
        
        self.assertEqual(metadata['task_id'], self.task_id)
        self.assertEqual(metadata['worker'], self.hostname)
        self.assertIn('acquired_at', metadata)
        self.assertIn('last_renewed_at', metadata)
    
    def test_acquire_lock_failure_already_locked(self):
        """Test acquiring lock fails when already locked by another task."""
        # Worker 1 acquires lock
        lock1 = JobLock(self.payload_id, "task-1", "worker-1")
        result1 = lock1.acquire(wait_time=0)
        self.assertTrue(result1)
        
        # Worker 2 tries to acquire same lock (different task)
        lock2 = JobLock(self.payload_id, "task-2", "worker-2")
        result2 = lock2.acquire(wait_time=0)
        self.assertFalse(result2)
        
        # Verify original lock still intact
        metadata = lock1.get_lock_metadata()
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata['task_id'], "task-1")
    
    def test_release_lock_success(self):
        """Test successfully releasing a lock."""
        lock = JobLock(self.payload_id, self.task_id, self.hostname)
        lock.acquire(wait_time=0)
        
        # Release the lock
        result = lock.release()
        self.assertTrue(result)
        
        # Verify Redis keys are deleted
        redlock_key = f"redlock:job-lock-{self.payload_id}"
        metadata_key = f"job-lock-metadata-{self.payload_id}"
        
        self.assertEqual(self.fake_redis.exists(redlock_key), 0)
        self.assertEqual(self.fake_redis.exists(metadata_key), 0)
    
    @freeze_time("2025-01-01 12:00:00")
    def test_extend_lock_success(self):
        """Test successfully extending a lock."""
        lock = JobLock(self.payload_id, self.task_id, self.hostname)
        lock.acquire(wait_time=0)
        
        # Get initial metadata
        initial_metadata = lock.get_lock_metadata()
        initial_renewed = initial_metadata['last_renewed_at']
        
        # Fast-forward time
        with freeze_time("2025-01-01 12:05:00"):  # 5 minutes later
            result = lock.extend()
            self.assertTrue(result)
            
            # Verify TTL reset to ~600s (not ~295s)
            redlock_key = f"redlock:job-lock-{self.payload_id}"
            ttl = self.fake_redis.ttl(redlock_key)
            self.assertGreater(ttl, 590)
            self.assertLessEqual(ttl, 600)
            
            # Verify last_renewed_at updated
            updated_metadata = lock.get_lock_metadata()
            self.assertGreater(
                updated_metadata['last_renewed_at'],
                initial_renewed
            )
    
    def test_lock_expiration(self):
        """Test lock expires after TTL."""
        lock = JobLock(self.payload_id, self.task_id, self.hostname)
        lock.acquire(expire_time=2, wait_time=0)  # 2 second TTL
        
        redlock_key = f"redlock:job-lock-{self.payload_id}"
        self.assertEqual(self.fake_redis.exists(redlock_key), 1)
        
        # Wait for expiration
        time.sleep(3)
        
        # Lock should be gone
        self.assertEqual(self.fake_redis.exists(redlock_key), 0)
    
    def test_get_lock_metadata_no_lock(self):
        """Test getting metadata when no lock exists."""
        lock = JobLock(self.payload_id, self.task_id, self.hostname)
        metadata = lock.get_lock_metadata()
        self.assertIsNone(metadata)
    
    def test_force_release(self):
        """Test force-releasing a lock held by another worker."""
        # Worker 1 acquires lock
        lock1 = JobLock(self.payload_id, "task-1", "worker-1")
        lock1.acquire(wait_time=0)
        
        # Worker 2 force-releases it
        lock2 = JobLock(self.payload_id, "task-2", "worker-2")
        result = lock2.force_release()
        self.assertTrue(result)
        
        # Verify lock is gone
        self.assertIsNone(lock2.get_lock_metadata())


class TestJobLockHeartbeat(TestCase):
    """Test heartbeat functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.fake_redis = fakeredis.FakeStrictRedis()
        self.payload_id = "test-payload-heartbeat"
        self.task_id = "test-task-heartbeat"
        self.hostname = "test-worker-heartbeat"
        
        self.redis_patcher = umock.patch.object(
            JobLock,
            '_get_connection_pool',
            return_value=self.fake_redis
        )
        self.redis_patcher.start()
        
        from hysds import celery
        celery.app.conf.JOB_LOCK_EXPIRE_TIME = 600
        celery.app.conf.JOB_LOCK_HEARTBEAT_INTERVAL = 2  # Short interval for testing (not production 30s)
        celery.app.conf.JOB_LOCK_MAX_EXTENSIONS = 4320
        celery.app.conf.JOB_LOCK_HEARTBEAT_MAX_FAILURES = 3
        celery.app.conf.JOB_LOCK_STALE_CHECK_RETRIES = 3
        
    def tearDown(self):
        """Clean up."""
        self.redis_patcher.stop()
        umock.patch.stopall()
    
    def test_heartbeat_starts_and_extends_lock(self):
        """Test heartbeat starts and extends lock periodically."""
        lock = JobLock(self.payload_id, self.task_id, self.hostname)
        lock.acquire(wait_time=0)
        
        # Get initial last_renewed_at
        initial_metadata = lock.get_lock_metadata()
        initial_renewed = initial_metadata['last_renewed_at']
        
        # Start heartbeat with 1 second interval
        lock.start_heartbeat(interval=1)
        
        # Wait for at least one heartbeat
        time.sleep(2.5)
        
        # Check that lock was extended
        updated_metadata = lock.get_lock_metadata()
        self.assertIsNotNone(updated_metadata)
        self.assertGreater(
            updated_metadata['last_renewed_at'],
            initial_renewed,
            "Lock should have been renewed by heartbeat"
        )
        
        # Clean up
        lock.stop_heartbeat()
        lock.release()
    
    def test_heartbeat_stops_on_release(self):
        """Test heartbeat stops when lock is released."""
        lock = JobLock(self.payload_id, self.task_id, self.hostname)
        lock.acquire(wait_time=0)
        lock.start_heartbeat(interval=1)
        
        # Wait for heartbeat to start
        time.sleep(0.5)
        self.assertTrue(lock.heartbeat_thread.is_alive())
        
        # Release lock
        lock.release()
        
        # Wait a bit for thread to stop
        time.sleep(1.5)
        
        # Heartbeat thread should be stopped
        self.assertFalse(
            lock.heartbeat_thread.is_alive(),
            "Heartbeat thread should stop after release"
        )
    
    @umock.patch('hysds.lock.thread_logger')
    def test_heartbeat_handles_redis_failure(self, mock_logger):
        """Test heartbeat handles Redis failures gracefully."""
        lock = JobLock(self.payload_id, self.task_id, self.hostname)
        lock.acquire(wait_time=0)
        
        # Mock extend to fail
        original_extend = lock.extend
        call_count = [0]
        
        def failing_extend(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] > 1:  # Fail after first call
                return False
            return original_extend(*args, **kwargs)
        
        lock.extend = failing_extend
        
        # Start heartbeat
        lock.start_heartbeat(interval=0.5)
        
        # Wait for failures to accumulate
        time.sleep(2.5)
        
        # Should have logged errors
        error_calls = [
            call for call in mock_logger.error.call_args_list
            if 'Failed to renew lock' in str(call)
        ]
        self.assertGreater(len(error_calls), 0, "Should log errors on extend failure")
        
        # Clean up
        lock.stop_heartbeat()
    
    def test_max_extensions_reached(self):
        """Test that max extensions limit is enforced."""
        from hysds import celery
        celery.app.conf.JOB_LOCK_MAX_EXTENSIONS = 2
        
        lock = JobLock(self.payload_id, self.task_id, self.hostname)
        lock.acquire(wait_time=0)
        
        # Extend twice successfully
        self.assertTrue(lock.extend())
        self.assertTrue(lock.extend())
        
        # Third extend should fail (max reached)
        # Note: pottery raises TooManyExtensions
        with self.assertRaises(TooManyExtensions):
            lock.extend()


class TestJobLockStaleLockDetection(TestCase):
    """Test stale lock detection logic."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.fake_redis = fakeredis.FakeStrictRedis()
        
        self.redis_patcher = umock.patch.object(
            JobLock,
            '_get_connection_pool',
            return_value=self.fake_redis
        )
        self.redis_patcher.start()
        
        from hysds import celery
        celery.app.conf.JOB_LOCK_EXPIRE_TIME = 600
        celery.app.conf.JOB_LOCK_HEARTBEAT_INTERVAL = 30
        celery.app.conf.JOB_LOCK_MAX_EXTENSIONS = 4320
        celery.app.conf.JOB_LOCK_HEARTBEAT_MAX_FAILURES = 3
        celery.app.conf.JOB_LOCK_STALE_CHECK_RETRIES = 3
        celery.app.conf.JOB_LOCK_STALE_CHECK_TIMEOUT = 60
        celery.app.conf.JOB_LOCK_REDELIVERY_BUFFER_TIME = 10
        
    def tearDown(self):
        """Clean up."""
        self.redis_patcher.stop()
        umock.patch.stopall()
    
    def test_no_lock_exists(self):
        """Test check_and_break_stale_lock when no lock exists."""
        payload_id = "no-lock-payload"
        
        result = JobLock.check_and_break_stale_lock(
            payload_id,
            current_task_id="task-1",
            current_hostname="worker-1"
        )
        
        self.assertFalse(result, "Should return False when no lock exists")
    
    def test_orphaned_lock_no_metadata(self):
        """Test detecting and breaking orphaned lock without metadata."""
        payload_id = "orphaned-payload"
        
        # Manually create lock key without metadata
        redlock_key = f"redlock:job-lock-{payload_id}"
        self.fake_redis.setex(redlock_key, 600, "orphaned-uuid")
        
        result = JobLock.check_and_break_stale_lock(
            payload_id,
            current_task_id="task-1",
            current_hostname="worker-1"
        )
        
        self.assertTrue(result, "Should break orphaned lock")
        self.assertEqual(self.fake_redis.exists(redlock_key), 0, "Orphaned lock should be deleted")
    
    @freeze_time("2025-01-01 12:00:00")
    def test_stale_lock_same_task_different_worker_old_renewal(self):
        """Test same task_id, different worker, missed 3+ heartbeats → immediate break."""
        payload_id = "stale-payload"
        task_id = "same-task-123"
        
        # Worker A acquires lock
        lock_a = JobLock(payload_id, task_id, "worker-A")
        lock_a.acquire(wait_time=0)
        
        # Fast-forward past 3x heartbeat interval (30s × 3 = 90s)
        with freeze_time("2025-01-01 12:02:00"):  # 2 minutes later (> 90s = 3 × 30s heartbeat)
            # Worker B receives redelivered job (same task_id, different hostname)
            result = JobLock.check_and_break_stale_lock(
                payload_id,
                current_task_id=task_id,
                current_hostname="worker-B"
            )
            
            self.assertTrue(result, "Should break stale lock immediately (missed 3+ heartbeats)")
            self.assertIsNone(
                lock_a.get_lock_metadata(),
                "Lock should be broken"
            )
    
    @freeze_time("2025-01-01 12:00:00")
    def test_stale_lock_same_task_same_worker_old_renewal(self):
        """Test same task_id, same worker, old renewal → break lock."""
        payload_id = "stale-same-worker"
        task_id = "task-123"
        hostname = "worker-A"
        
        # Worker A acquires lock
        lock_a = JobLock(payload_id, task_id, hostname)
        lock_a.acquire(wait_time=0)
        
        # Fast-forward past 2x heartbeat interval (2 × 30s = 60s)
        with freeze_time("2025-01-01 12:02:00"):  # 2 min (> 2 × 30s = 60s)
            # Same worker receives redelivered job (job crashed and restarted)
            result = JobLock.check_and_break_stale_lock(
                payload_id,
                current_task_id=task_id,
                current_hostname=hostname
            )
            
            self.assertTrue(result, "Should break stale lock (old renewal, same worker)")
    
    @freeze_time("2025-01-01 12:00:00")
    def test_stale_lock_same_task_same_worker_recent_renewal(self):
        """Test same task_id, same worker, recent renewal → don't break."""
        payload_id = "recent-same-worker"
        task_id = "task-123"
        hostname = "worker-A"
        
        # Worker A acquires lock
        lock_a = JobLock(payload_id, task_id, hostname)
        lock_a.acquire(wait_time=0)
        
        # Fast-forward slightly (< 2x heartbeat interval = 60s)
        with freeze_time("2025-01-01 12:00:30"):  # 30 sec (< 2 × 30s = 60s)
            # Same worker receives redelivered job
            result = JobLock.check_and_break_stale_lock(
                payload_id,
                current_task_id=task_id,
                current_hostname=hostname
            )
            
            self.assertFalse(result, "Should NOT break lock (recent renewal, might still be alive)")
            self.assertIsNotNone(lock_a.get_lock_metadata(), "Lock should still exist")


class TestJobLockWaitForRenewal(TestCase):
    """Test wait for renewal logic."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.fake_redis = fakeredis.FakeStrictRedis()
        
        self.redis_patcher = umock.patch.object(
            JobLock,
            '_get_connection_pool',
            return_value=self.fake_redis
        )
        self.redis_patcher.start()
        
        from hysds import celery
        celery.app.conf.JOB_LOCK_EXPIRE_TIME = 600
        celery.app.conf.JOB_LOCK_HEARTBEAT_INTERVAL = 30
        celery.app.conf.JOB_LOCK_MAX_EXTENSIONS = 4320
        celery.app.conf.JOB_LOCK_HEARTBEAT_MAX_FAILURES = 3
        celery.app.conf.JOB_LOCK_STALE_CHECK_RETRIES = 3
        
    def tearDown(self):
        """Clean up."""
        self.redis_patcher.stop()
        umock.patch.stopall()
    
    def test_wait_for_renewal_lock_renewed(self):
        """Test waiting detects when lock is renewed (worker alive)."""
        payload_id = "renewal-test"
        lock = JobLock(payload_id, "task-1", "worker-1")
        lock.acquire(wait_time=0)
        
        initial_metadata = lock.get_lock_metadata()
        initial_renewed = initial_metadata['last_renewed_at']
        
        # Start a thread that will renew the lock after 1 second
        def renew_later():
            time.sleep(1)
            lock.extend()
        
        renew_thread = threading.Thread(target=renew_later, daemon=True)
        renew_thread.start()
        
        # Wait for renewal (max 3 seconds)
        result = lock._wait_for_lock_renewal(initial_renewed, max_wait_time=3)
        
        self.assertTrue(result, "Should detect lock renewal (worker alive)")
        renew_thread.join(timeout=1)
    
    def test_wait_for_renewal_lock_not_renewed(self):
        """Test waiting times out when lock is not renewed (worker dead)."""
        payload_id = "no-renewal-test"
        lock = JobLock(payload_id, "task-1", "worker-1")
        lock.acquire(wait_time=0)
        
        initial_metadata = lock.get_lock_metadata()
        initial_renewed = initial_metadata['last_renewed_at']
        
        # Don't renew lock, just wait
        result = lock._wait_for_lock_renewal(initial_renewed, max_wait_time=2)
        
        self.assertFalse(result, "Should timeout (worker dead, no renewal)")
    
    def test_wait_for_renewal_lock_disappears(self):
        """Test waiting detects when lock disappears (worker crashed)."""
        payload_id = "disappear-test"
        lock = JobLock(payload_id, "task-1", "worker-1")
        lock.acquire(wait_time=0)
        
        initial_metadata = lock.get_lock_metadata()
        initial_renewed = initial_metadata['last_renewed_at']
        
        # Start a thread that will delete the lock after 0.5 seconds
        def delete_later():
            time.sleep(0.5)
            lock.release()
        
        delete_thread = threading.Thread(target=delete_later, daemon=True)
        delete_thread.start()
        
        # Wait for renewal
        result = lock._wait_for_lock_renewal(initial_renewed, max_wait_time=3)
        
        self.assertFalse(result, "Should detect lock disappeared (worker dead)")
        delete_thread.join(timeout=1)


class TestJobLockConcurrency(TestCase):
    """Test concurrent access scenarios."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.fake_redis = fakeredis.FakeStrictRedis()
        
        self.redis_patcher = umock.patch.object(
            JobLock,
            '_get_connection_pool',
            return_value=self.fake_redis
        )
        self.redis_patcher.start()
        
        from hysds import celery
        celery.app.conf.JOB_LOCK_EXPIRE_TIME = 600
        celery.app.conf.JOB_LOCK_HEARTBEAT_INTERVAL = 30
        celery.app.conf.JOB_LOCK_MAX_EXTENSIONS = 4320
        celery.app.conf.JOB_LOCK_HEARTBEAT_MAX_FAILURES = 3
        celery.app.conf.JOB_LOCK_STALE_CHECK_RETRIES = 3
        
    def tearDown(self):
        """Clean up."""
        self.redis_patcher.stop()
        umock.patch.stopall()
    
    def test_concurrent_acquire_same_lock(self):
        """Test multiple workers trying to acquire same lock simultaneously."""
        payload_id = "concurrent-payload"
        results = []
        
        def try_acquire(worker_id):
            lock = JobLock(payload_id, f"task-{worker_id}", f"worker-{worker_id}")
            result = lock.acquire(wait_time=0)
            results.append((worker_id, result))
        
        # Spawn 5 threads trying to acquire same lock
        threads = []
        for i in range(5):
            t = threading.Thread(target=try_acquire, args=(i,))
            threads.append(t)
            t.start()
        
        # Wait for all threads
        for t in threads:
            t.join(timeout=2)
        
        # Only one should succeed
        successes = [r for r in results if r[1] is True]
        self.assertEqual(len(successes), 1, "Only one worker should acquire the lock")
    
    def test_multiple_concurrent_jobs_different_payloads(self):
        """Test multiple jobs with different payload_ids can run concurrently."""
        results = []
        
        def acquire_and_work(payload_id):
            lock = JobLock(payload_id, f"task-{payload_id}", f"worker-{payload_id}")
            result = lock.acquire(wait_time=0)
            results.append((payload_id, result))
            if result:
                time.sleep(0.1)  # Simulate work
                lock.release()
        
        # Spawn 10 threads with different payloads
        threads = []
        for i in range(10):
            t = threading.Thread(target=acquire_and_work, args=(f"payload-{i}",))
            threads.append(t)
            t.start()
        
        # Wait for all threads
        for t in threads:
            t.join(timeout=2)
        
        # All should succeed (different payload_ids)
        successes = [r for r in results if r[1] is True]
        self.assertEqual(len(successes), 10, "All workers should acquire their respective locks")


class TestJobLockConfiguration(TestCase):
    """Test configuration and edge cases."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.fake_redis = fakeredis.FakeStrictRedis()
        
        self.redis_patcher = umock.patch.object(
            JobLock,
            '_get_connection_pool',
            return_value=self.fake_redis
        )
        self.redis_patcher.start()
        
    def tearDown(self):
        """Clean up."""
        self.redis_patcher.stop()
        umock.patch.stopall()
    
    def test_custom_expire_time(self):
        """Test lock with custom expiration time."""
        from hysds import celery
        celery.app.conf.JOB_LOCK_EXPIRE_TIME = 600
        
        lock = JobLock("custom-expire", "task-1", "worker-1")
        lock.acquire(expire_time=300, wait_time=0)  # Custom 5 minutes
        
        redlock_key = f"redlock:job-lock-custom-expire"
        ttl = self.fake_redis.ttl(redlock_key)
        
        self.assertGreater(ttl, 290)
        self.assertLessEqual(ttl, 300)
    
    def test_lock_metadata_format(self):
        """Test lock metadata has correct structure."""
        lock = JobLock("metadata-test", "task-123", "worker-abc")
        lock.acquire(wait_time=0)
        
        metadata = lock.get_lock_metadata()
        
        # Verify required fields
        self.assertIn('task_id', metadata)
        self.assertIn('worker', metadata)
        self.assertIn('acquired_at', metadata)
        self.assertIn('last_renewed_at', metadata)
        
        # Verify values
        self.assertEqual(metadata['task_id'], "task-123")
        self.assertEqual(metadata['worker'], "worker-abc")
        self.assertIsInstance(metadata['acquired_at'], (int, float))
        self.assertIsInstance(metadata['last_renewed_at'], (int, float))
    
    def test_heartbeat_max_failures_configurable(self):
        """Test that heartbeat max failures is configurable."""
        from hysds import celery
        
        # Set custom max failures
        celery.app.conf.JOB_LOCK_HEARTBEAT_MAX_FAILURES = 5
        celery.app.conf.JOB_LOCK_HEARTBEAT_INTERVAL = 1  # 1 second for fast test
        
        lock = JobLock("config-test", "task-1", "worker-1")
        lock.acquire(wait_time=0)
        
        # Mock extend to always fail
        lock.extend = umock.Mock(return_value=False)
        
        # Start heartbeat
        lock.start_heartbeat(interval=0.5)
        
        # Wait for failures to accumulate (5 failures × 0.5s = 2.5s + buffer)
        time.sleep(3)
        
        # Should have called extend at least 5 times
        self.assertGreaterEqual(
            lock.extend.call_count, 
            5,
            "Should attempt extend at least max_failures times"
        )
        
        # Clean up
        lock.stop_heartbeat()
    
    def test_stale_check_retries_configurable(self):
        """Test that stale check retries is configurable."""
        from hysds import celery
        
        # Set custom retry count
        celery.app.conf.JOB_LOCK_STALE_CHECK_RETRIES = 5
        celery.app.conf.JOB_LOCK_HEARTBEAT_INTERVAL = 10
        
        # With 5 retries, stale threshold = 5 × 10s = 50s
        # Create a lock that's 60s old (> threshold)
        with freeze_time("2025-01-01 12:00:00"):
            lock = JobLock("retry-test", "task-1", "worker-1")
            lock.acquire(wait_time=0)
        
        # Fast-forward 60 seconds (> 50s threshold)
        with freeze_time("2025-01-01 12:01:00"):
            result = JobLock.check_and_break_stale_lock(
                "retry-test",
                current_task_id="task-2",
                current_hostname="worker-2"
            )
            
            # Should break immediately (exceeded 5 × 10s = 50s threshold)
            self.assertTrue(result, "Should break stale lock (exceeded 5 retry threshold)")


if __name__ == '__main__':
    import unittest
    unittest.main()
