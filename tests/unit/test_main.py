import asyncio
import time
import unittest
import unittest.mock
from unittest.mock import Mock, patch

from azul_stats.main import (
    FAIL_VALUE,
    SUCCESS_VALUE,
    TIMEOUT_LABEL,
    UNKNOWN_LABEL,
    StatsCollector,
    stats_status,
)
from azul_stats.settings import StatsSettings


class TestMainCorrectGroups(unittest.IsolatedAsyncioTestCase):
    def test_enable_services(self):
        settings = StatsSettings()
        # Verify Kafka can be enabled.
        settings.kafka = True
        collector = StatsCollector(settings)
        self.assertIn(collector.kafka, collector._thread_stat_scrape_func)
        self.assertEqual(len(collector._async_stat_scrape_func), 0)
        self.assertEqual(len(collector._thread_stat_scrape_func), 1)

        # Verify Opensearch and kafka can be enabled.
        settings.opensearch = True
        collector = StatsCollector(settings)
        self.assertIn(collector.opensearch, collector._async_stat_scrape_func)
        self.assertIn(collector.kafka, collector._thread_stat_scrape_func)
        self.assertEqual(len(collector._async_stat_scrape_func), 1)
        self.assertEqual(len(collector._thread_stat_scrape_func), 1)

        # Just opensearch.
        settings.kafka = False
        collector = StatsCollector(settings)
        self.assertIn(collector.opensearch, collector._async_stat_scrape_func)
        self.assertEqual(len(collector._async_stat_scrape_func), 1)
        self.assertEqual(len(collector._thread_stat_scrape_func), 0)


class TestMainAsync(unittest.IsolatedAsyncioTestCase):

    async def _slow_service(self):
        await asyncio.sleep(1)

    async def _fast_service(self):
        return

    async def _error_service(self):
        # Raise an obscure error
        await asyncio.sleep(0.1)
        raise UnicodeError()

    # ------------------- Test with run_once enabled -----------------------

    async def test_slow_to_scrape_service(self):
        """Test a service that is too slow."""
        stats_status.labels(TIMEOUT_LABEL).set(SUCCESS_VALUE)
        stats_status.labels(UNKNOWN_LABEL).set(SUCCESS_VALUE)
        settings = StatsSettings()
        settings.max_scrape_time = 0.5  # Limit runtime to really small
        stats_collector = StatsCollector(settings)
        stats_collector._running_services = ["slow-service"]
        stats_collector._async_stat_scrape_func = [self._slow_service]
        with self.assertRaises(TimeoutError):
            await stats_collector.run(run_once=True)

        self.assertEqual(stats_status.labels(TIMEOUT_LABEL)._value.get(), FAIL_VALUE)
        self.assertEqual(stats_status.labels(UNKNOWN_LABEL)._value.get(), SUCCESS_VALUE)

    async def test_speedy_service(self):
        """Test a successful run and that the sleep time would be large."""
        stats_status.labels(TIMEOUT_LABEL).set(FAIL_VALUE)
        stats_status.labels(UNKNOWN_LABEL).set(FAIL_VALUE)

        settings = StatsSettings()
        settings.max_scrape_time = 30
        stats_collector = StatsCollector(settings)
        stats_collector._running_services = ["fast-service"]
        stats_collector._async_stat_scrape_func = [self._fast_service]
        time_between = await stats_collector.run(run_once=True)
        # Time between scrapes should be less than or equal to 30 seconds.
        # and the run shouldn't take more than 2 seconds in even the worst case.
        self.assertGreaterEqual(time_between, 28)
        self.assertLessEqual(time_between, 30)

        # Ensure success values are set appropriately.
        self.assertEqual(stats_status.labels(TIMEOUT_LABEL)._value.get(), SUCCESS_VALUE)
        self.assertEqual(stats_status.labels(UNKNOWN_LABEL)._value.get(), SUCCESS_VALUE)

    async def test_error_service(self):
        """Test an ExceptionGroup is generated."""
        stats_status.labels(TIMEOUT_LABEL).set(SUCCESS_VALUE)
        stats_status.labels(UNKNOWN_LABEL).set(SUCCESS_VALUE)
        settings = StatsSettings()
        stats_collector = StatsCollector(settings)
        stats_collector._running_services = ["error-service"]
        stats_collector._async_stat_scrape_func = [self._error_service]
        with self.assertRaises(ExceptionGroup):
            await stats_collector.run(run_once=True)

        self.assertEqual(stats_status.labels(TIMEOUT_LABEL)._value.get(), SUCCESS_VALUE)
        self.assertEqual(stats_status.labels(UNKNOWN_LABEL)._value.get(), FAIL_VALUE)

    # ------------------- Test in infinite loop and just timeout the tests at a fixed time -----------------------
    @patch("time.sleep", return_value=None)
    async def test_slow_to_scrape_service_sleep(self, patched_time_sleep: Mock):
        """Test a service that is too slow."""
        stats_status.labels(TIMEOUT_LABEL).set(SUCCESS_VALUE)
        stats_status.labels(UNKNOWN_LABEL).set(SUCCESS_VALUE)
        settings = StatsSettings()
        settings.max_scrape_time = 0.1  # Make it timeout almost immediately.
        stats_collector = StatsCollector(settings)
        stats_collector._running_services = ["slow-service"]
        stats_collector._async_stat_scrape_func = [self._slow_service]

        try:
            async with asyncio.timeout(1):
                await stats_collector.run()
        except TimeoutError:
            pass

        self.assertEqual(stats_status.labels(TIMEOUT_LABEL)._value.get(), FAIL_VALUE)
        self.assertEqual(stats_status.labels(UNKNOWN_LABEL)._value.get(), SUCCESS_VALUE)

    @patch("time.sleep", return_value=None)
    async def test_speedy_service_sleep(self, patched_time_sleep: Mock):
        stats_status.labels(TIMEOUT_LABEL).set(FAIL_VALUE)
        stats_status.labels(UNKNOWN_LABEL).set(FAIL_VALUE)

        settings = StatsSettings()
        settings.max_scrape_time = 30
        stats_collector = StatsCollector(settings)
        stats_collector._running_services = ["fast-service"]
        stats_collector._async_stat_scrape_func = [self._fast_service]
        # Just ensure it gets to run for one second and everything is as expected.
        try:
            async with asyncio.timeout(0.2):
                await stats_collector.run()
        except TimeoutError:
            pass
        patched_time_sleep.assert_called()

        # Ensure success values are set appropriately.
        self.assertEqual(stats_status.labels(TIMEOUT_LABEL)._value.get(), SUCCESS_VALUE)
        self.assertEqual(stats_status.labels(UNKNOWN_LABEL)._value.get(), SUCCESS_VALUE)

    @patch("time.sleep", return_value=None)
    async def test_error_service_sleep(self, patched_time_sleep: Mock):
        """Test an ExceptionGroup is generated."""
        stats_status.labels(TIMEOUT_LABEL).set(SUCCESS_VALUE)
        stats_status.labels(UNKNOWN_LABEL).set(SUCCESS_VALUE)

        settings = StatsSettings()
        settings.max_scrape_time = 1
        stats_collector = StatsCollector(settings)
        stats_collector._running_services = ["error-service"]
        stats_collector._async_stat_scrape_func = [self._error_service]
        # Just ensure it gets to run for one second and everything is as expected.
        try:
            async with asyncio.timeout(0.2):
                await stats_collector.run()
        except asyncio.exceptions.CancelledError:
            pass

        patched_time_sleep.assert_called()

        self.assertEqual(stats_status.labels(TIMEOUT_LABEL)._value.get(), SUCCESS_VALUE)
        self.assertEqual(stats_status.labels(UNKNOWN_LABEL)._value.get(), FAIL_VALUE)


class TestMainThreaded(unittest.IsolatedAsyncioTestCase):

    def _slow_service(self, timeout_sec: float):
        time.sleep(2)

    def _fast_service(self, timeout_sec: float):
        return

    def _error_service(self, timeout_sec: float):
        # Raise an obscure error
        raise UnicodeError()

    # ------------------- Test with run_once enabled -----------------------

    async def test_slow_to_scrape_service(self):
        """Test a service that is too slow."""
        stats_status.labels(TIMEOUT_LABEL).set(SUCCESS_VALUE)
        stats_status.labels(UNKNOWN_LABEL).set(SUCCESS_VALUE)
        settings = StatsSettings()
        settings.max_scrape_time = 0.5  # Limit runtime to really small
        stats_collector = StatsCollector(settings)
        stats_collector._running_services = ["timeout-service"]
        stats_collector._thread_stat_scrape_func = [self._slow_service]
        with self.assertRaises(TimeoutError):
            await stats_collector.run(run_once=True)

        self.assertEqual(stats_status.labels(TIMEOUT_LABEL)._value.get(), FAIL_VALUE)
        self.assertEqual(stats_status.labels(UNKNOWN_LABEL)._value.get(), SUCCESS_VALUE)

    async def test_speedy_service(self):
        """Test a successful run and that the sleep time would be large."""
        stats_status.labels(TIMEOUT_LABEL).set(FAIL_VALUE)
        stats_status.labels(UNKNOWN_LABEL).set(FAIL_VALUE)

        settings = StatsSettings()
        settings.max_scrape_time = 30
        stats_collector = StatsCollector(settings)
        stats_collector._running_services = ["fast-service"]
        stats_collector._thread_stat_scrape_func = [self._fast_service]
        time_between = await stats_collector.run(run_once=True)
        # Time between scrapes should be less than or equal to 30 seconds.
        # and the run shouldn't take more than 2 seconds in even the worst case.
        self.assertGreaterEqual(time_between, 28)
        self.assertLessEqual(time_between, 30)

        # Ensure success values are set appropriately.
        self.assertEqual(stats_status.labels(TIMEOUT_LABEL)._value.get(), SUCCESS_VALUE)
        self.assertEqual(stats_status.labels(UNKNOWN_LABEL)._value.get(), SUCCESS_VALUE)

    async def test_error_service(self):
        """Test an Exception simply kills the child thread and the parent ignores it."""
        stats_status.labels(TIMEOUT_LABEL).set(SUCCESS_VALUE)
        stats_status.labels(UNKNOWN_LABEL).set(FAIL_VALUE)
        settings = StatsSettings()
        stats_collector = StatsCollector(settings)
        stats_collector._running_services = ["error-service"]
        stats_collector._thread_stat_scrape_func = [self._error_service]
        with self.assertRaises(UnicodeError):
            await stats_collector.run(run_once=True)

        self.assertEqual(stats_status.labels(TIMEOUT_LABEL)._value.get(), SUCCESS_VALUE)
        self.assertEqual(stats_status.labels(UNKNOWN_LABEL)._value.get(), FAIL_VALUE)
