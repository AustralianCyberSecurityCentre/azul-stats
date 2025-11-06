import time

from azul_stats.main import (
    FAIL_VALUE,
    REDIS_AUTH,
    REDIS_GET_KEY,
    REDIS_SET_KEY,
    SUCCESS_VALUE,
    StatsCollector,
    azul_health_check_timing,
)
from azul_stats.redis_wrapper import RedisWrapper
from azul_stats.settings import RedisSettings, StatsSettings, StatTargets
from tests.integration.base_test import BaseTest


class TestRedisMain(BaseTest):
    """Test all the redis scraping against main stages."""

    def setUp(self):
        self.cfg = RedisSettings()
        self.system = StatTargets.redis
        # Alter environment before creating stats collection
        setup_result = super().setUp()
        self.stats_settings = StatsSettings()
        self.stats_settings.redis = True
        self.stats_settings.max_scrape_time = 30
        self.stats_collector = StatsCollector(self.stats_settings)
        self.start_time = time.time()
        self.timeout_sec = 10
        return setup_result

    def _set_all_health_to(self, value: int):
        """Set all the redis health values to the provided value."""
        self.set_health_value(REDIS_AUTH, value)
        self.set_health_value(REDIS_SET_KEY, value)
        self.set_health_value(REDIS_GET_KEY, value)

    def test_all_stages_succeed(self):
        """Run all Redis stages and verify they are all successful."""
        self._set_all_health_to(FAIL_VALUE)
        collect_redis = self.stats_collector.redis_setup(self.cfg)
        collect_redis(self.timeout_sec)
        # Ensure everything succeeded.
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(REDIS_AUTH))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(REDIS_SET_KEY))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(REDIS_GET_KEY))
        # Ensure timing stats are captured
        self.assertGreater(azul_health_check_timing.labels(system=self.system, action=REDIS_AUTH)._value.get(), 0)
        self.assertGreater(
            azul_health_check_timing.labels(system=self.system, action=REDIS_SET_KEY)._value.get(),
            0,
        )
        self.assertGreater(azul_health_check_timing.labels(system=self.system, action=REDIS_GET_KEY)._value.get(), 0)

    async def test_fail_timeout(self):
        """Verify timeout of stage and whole stats module behave correctly."""
        collect_redis = self.stats_collector.redis_setup(self.cfg)
        # Timeout running stage
        with self.assertRaises(TimeoutError):
            collect_redis(0.0)

        # Timeout running stats collector
        self.set_stats_timeout_value(SUCCESS_VALUE)
        self.stats_settings.max_scrape_time = 0.0
        stats_collector = StatsCollector(self.stats_settings)
        with self.assertRaises(TimeoutError):
            await stats_collector.run(run_once=True)

        # Ensure timeout is set on the states.
        self.assertEqual(FAIL_VALUE, self.get_stats_timeout_value())

    def test_fail_on_bad_auth(self):
        """Verify auth stages fail on a bad auth."""
        self.cfg.password = "bad-password"
        rw = RedisWrapper(self.cfg)
        self._set_all_health_to(SUCCESS_VALUE)
        self.stats_collector._run_threaded_stage(
            REDIS_AUTH, self.system, rw.connect, self.start_time, self.timeout_sec
        )
        self.assertEqual(FAIL_VALUE, self.get_health_value(REDIS_AUTH))

    def test_get_and_set_key_stage_fails(self):
        """Verify all stages fail on a bad auth."""
        self.cfg.password = "bad-password"
        rw = RedisWrapper(self.cfg)
        self._set_all_health_to(SUCCESS_VALUE)
        self.stats_collector._run_threaded_stage(
            REDIS_AUTH, self.system, rw.connect, self.start_time, self.timeout_sec
        )
        self.stats_collector._run_threaded_stage(
            REDIS_SET_KEY, self.system, rw.set_key, self.start_time, self.timeout_sec
        )
        self.stats_collector._run_threaded_stage(
            REDIS_GET_KEY, self.system, rw.get_key, self.start_time, self.timeout_sec
        )
        self.assertEqual(FAIL_VALUE, self.get_health_value(REDIS_AUTH))
        self.assertEqual(FAIL_VALUE, self.get_health_value(REDIS_SET_KEY))
        self.assertEqual(FAIL_VALUE, self.get_health_value(REDIS_GET_KEY))

    def test_fail_get_and_set_key_full(self):
        """Verify all stages fail when auth is bad full collector version."""
        self._set_all_health_to(SUCCESS_VALUE)
        self.cfg.password = "bad-password"
        collect_redis = self.stats_collector.redis_setup(self.cfg)
        collect_redis(self.timeout_sec)
        # Ensure everything succeeded.
        self.assertEqual(FAIL_VALUE, self.get_health_value(REDIS_AUTH))
        self.assertEqual(FAIL_VALUE, self.get_health_value(REDIS_SET_KEY))
        self.assertEqual(FAIL_VALUE, self.get_health_value(REDIS_GET_KEY))
