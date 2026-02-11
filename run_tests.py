#!/usr/bin/env python
"""
Isolated test runner for the Telegram bot project.
Runs tests without triggering bot startup.
"""
import sys
import os
import unittest
import importlib.util
from dataclasses import dataclass
from enum import Enum

# Load models directly without package initialization
def load_module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Get project root
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# Load core.models directly
models = load_module_from_file("models", os.path.join(PROJECT_ROOT, "core", "models.py"))


class TestDownloadTask(unittest.TestCase):
    """Tests for DownloadTask dataclass."""

    def test_create_download_task(self):
        """Test basic DownloadTask creation."""
        task = models.DownloadTask(
            task_id="test-123",
            file_unique_id="unique-456",
            user_id=12345,
            chat_id=12345,
            message_id=100,
            file_id="file-789",
            file_size=1024000,
            file_name="test_file.mp4",
        )
        self.assertEqual(task.task_id, "test-123")
        self.assertEqual(task.file_unique_id, "unique-456")
        self.assertEqual(task.user_id, 12345)
        self.assertEqual(task.file_size, 1024000)
        self.assertEqual(task.status, "PENDING")

    def test_download_task_default_values(self):
        """Test DownloadTask default values are set correctly."""
        task = models.DownloadTask(
            task_id="t1",
            file_unique_id="u1",
            user_id=1,
            chat_id=1,
            message_id=1,
            file_id="f1",
            file_size=100,
            file_name="test.txt",
        )
        self.assertEqual(task.mime_type, "")
        self.assertEqual(task.media_type, "document")
        self.assertEqual(task.retry_count, 0)
        self.assertEqual(task.max_retries, 5)
        self.assertEqual(task.downloaded_bytes, 0)

    def test_download_task_progress_calculation(self):
        """Test progress percentage calculation."""
        task = models.DownloadTask(
            task_id="test-123",
            file_unique_id="unique-456",
            user_id=12345,
            chat_id=12345,
            message_id=100,
            file_id="file-789",
            file_size=1024000,
            file_name="test_file.mp4",
        )
        task.downloaded_bytes = 512000
        progress = (task.downloaded_bytes / task.file_size) * 100
        self.assertEqual(progress, 50.0)


class TestDownloadStatus(unittest.TestCase):
    """Tests for DownloadStatus enum."""

    def test_status_values(self):
        """Test all status values exist."""
        self.assertEqual(models.DownloadStatus.PENDING.value, "PENDING")
        self.assertEqual(models.DownloadStatus.DOWNLOADING.value, "DOWNLOADING")
        self.assertEqual(models.DownloadStatus.UPLOADING.value, "UPLOADING")
        self.assertEqual(models.DownloadStatus.COMPLETED.value, "COMPLETED")
        self.assertEqual(models.DownloadStatus.FAILED.value, "FAILED")
        self.assertEqual(models.DownloadStatus.CANCELED.value, "CANCELED")


class TestProgressEvent(unittest.TestCase):
    """Tests for ProgressEvent dataclass."""

    def test_create_progress_event(self):
        """Test ProgressEvent creation."""
        event = models.ProgressEvent(
            task_id="test-123",
            user_id=12345,
            chat_id=12345,
            message_id=100,
            current_bytes=512000,
            total_bytes=1024000,
        )
        self.assertEqual(event.task_id, "test-123")
        self.assertEqual(event.current_bytes, 512000)
        self.assertEqual(event.total_bytes, 1024000)

    def test_progress_percentage(self):
        """Test progress percentage calculation via percent property."""
        event = models.ProgressEvent(
            task_id="test-123",
            user_id=12345,
            chat_id=12345,
            message_id=100,
            current_bytes=512000,
            total_bytes=1024000,
        )
        self.assertEqual(event.percent, 50.0)


class TestSpeedTracker(unittest.TestCase):
    """Tests for SpeedTracker class."""

    def test_speed_tracker_creation(self):
        """Test SpeedTracker initialization."""
        tracker = models.SpeedTracker()
        self.assertEqual(tracker.get_speed(), 0.0)

    def test_speed_tracker_add_sample(self):
        """Test SpeedTracker add_sample method."""
        import time
        tracker = models.SpeedTracker(max_samples=10)
        tracker.add_sample(1024, time.time())
        tracker.add_sample(2048, time.time() + 1)
        speed = tracker.get_speed()
        self.assertGreaterEqual(speed, 0)


class TestConfig(unittest.TestCase):
    """Tests for configuration values."""

    def test_config_imports(self):
        """Test that config module imports correctly."""
        config = load_module_from_file("config", os.path.join(PROJECT_ROOT, "config.py"))
        self.assertIsNotNone(config.API_ID)
        self.assertIsNotNone(config.API_HASH)
        self.assertIsNotNone(config.BOT_TOKEN)

    def test_api_id_is_integer(self):
        """Test API_ID is an integer."""
        config = load_module_from_file("config", os.path.join(PROJECT_ROOT, "config.py"))
        self.assertIsInstance(config.API_ID, int)

    def test_rate_limit_config(self):
        """Test rate limiting configuration values."""
        config = load_module_from_file("config", os.path.join(PROJECT_ROOT, "config.py"))
        self.assertGreater(config.MAX_CONCURRENT_LOGINS, 0)
        self.assertGreater(config.LOGIN_COOLDOWN_SECONDS, 0)


if __name__ == "__main__":
    print("=" * 60)
    print("Running isolated unit tests for Telegram bot")
    print("=" * 60)
    
    # Run tests
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestDownloadTask))
    suite.addTests(loader.loadTestsFromTestCase(TestDownloadStatus))
    suite.addTests(loader.loadTestsFromTestCase(TestProgressEvent))
    suite.addTests(loader.loadTestsFromTestCase(TestSpeedTracker))
    suite.addTests(loader.loadTestsFromTestCase(TestConfig))
    
    # Run with verbosity
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Exit with proper code
    sys.exit(0 if result.wasSuccessful() else 1)
