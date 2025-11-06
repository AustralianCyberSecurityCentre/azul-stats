import time

from azul_stats.auth_wrapper import AsyncAuthWrapper
from azul_stats.main import (
    AUTH_GET_TOKEN,
    AUTH_VALIDATE_TOKEN,
    FAIL_VALUE,
    SUCCESS_VALUE,
    StatsCollector,
    azul_health_check_timing,
)
from azul_stats.settings import AuthSettings, StatsSettings, StatTargets
from tests.integration.base_test import BaseTest


class TestAuthMain(BaseTest):
    """Test all the auth scraping against main stages."""

    def setUp(self):
        self.cfg = AuthSettings()
        self.system = StatTargets.auth
        # Alter environment before creating stats collection
        setup_result = super().setUp()
        self.stats_settings = StatsSettings()
        self.stats_settings.auth = True
        self.stats_settings.max_scrape_time = 30
        self.stats_collector = StatsCollector(self.stats_settings)
        return setup_result

    def _set_all_health_to(self, value: int):
        """Set all the auth health values to the provided value."""
        self.set_health_value(AUTH_GET_TOKEN, value)
        self.set_health_value(AUTH_VALIDATE_TOKEN, value)

    async def test_all_stages_succeed(self):
        """Run all Auth stages and verify they are all successful."""
        self._set_all_health_to(FAIL_VALUE)

        collect_auth = self.stats_collector.auth_setup(self.cfg)
        # Run auth collection
        await collect_auth()
        # Ensure everything succeeded.
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(AUTH_GET_TOKEN))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(AUTH_VALIDATE_TOKEN))
        # Ensure timing stats are captured
        self.assertGreater(azul_health_check_timing.labels(system=self.system, action=AUTH_GET_TOKEN)._value.get(), 0)
        self.assertGreater(
            azul_health_check_timing.labels(system=self.system, action=AUTH_VALIDATE_TOKEN)._value.get(),
            0,
        )

    async def test_fail_with_bad_settings_username(self):
        """Fail with bad username"""
        self._set_all_health_to(SUCCESS_VALUE)
        self.cfg.client_id = "bad-username"
        collect_auth = self.stats_collector.auth_setup(self.cfg)
        await collect_auth()
        # Ensure stats are correct
        self.assertEqual(FAIL_VALUE, self.get_health_value(AUTH_GET_TOKEN))
        self.assertEqual(FAIL_VALUE, self.get_health_value(AUTH_VALIDATE_TOKEN))

    async def test_fail_with_bad_settings_well_known(self):
        """Fail with bad hostname"""
        self._set_all_health_to(SUCCESS_VALUE)
        self.cfg.well_known_endpoint = "bad-well-known-endpoint"
        collect_auth = self.stats_collector.auth_setup(self.cfg)
        await collect_auth()
        # Ensure stats are correct
        self.assertEqual(FAIL_VALUE, self.get_health_value(AUTH_GET_TOKEN))
        self.assertEqual(FAIL_VALUE, self.get_health_value(AUTH_VALIDATE_TOKEN))

    async def test_fail_with_bad_settings_secret(self):
        """Fail with bad secret"""
        self._set_all_health_to(SUCCESS_VALUE)
        self.cfg.client_secret = "bad-secret"
        collect_auth = self.stats_collector.auth_setup(self.cfg)
        await collect_auth()
        # Ensure stats are correct
        self.assertEqual(FAIL_VALUE, self.get_health_value(AUTH_GET_TOKEN))
        self.assertEqual(FAIL_VALUE, self.get_health_value(AUTH_VALIDATE_TOKEN))

    async def test_stats_fail_with_timeout(self):
        """Run azure blob section of test when given no time."""
        self.set_stats_timeout_value(SUCCESS_VALUE)
        self.stats_settings.max_scrape_time = 0.0
        stats_collector = StatsCollector(self.stats_settings)
        with self.assertRaises(TimeoutError):
            await stats_collector.run(run_once=True)

        # Ensure status stats are captured
        self.assertEqual(FAIL_VALUE, self.get_stats_timeout_value())

    async def test_fail_invalid_token_audience(self):
        """Successfully auth but find an invalid token due to audience not being set."""
        self._set_all_health_to(SUCCESS_VALUE)
        self.cfg.expected_audience = "bad-aud"

        collect_auth = self.stats_collector.auth_setup(self.cfg)
        await collect_auth()

        # verify result
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(AUTH_GET_TOKEN))
        self.assertEqual(FAIL_VALUE, self.get_health_value(AUTH_VALIDATE_TOKEN))

    async def test_fail_invalid_token_roles(self):
        """Successfully auth but find an invalid token due to bad roles."""
        self._set_all_health_to(SUCCESS_VALUE)
        self.cfg.expected_roles = ["bad-role"]

        collect_auth = self.stats_collector.auth_setup(self.cfg)
        await collect_auth()

        # verify result
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(AUTH_GET_TOKEN))
        self.assertEqual(FAIL_VALUE, self.get_health_value(AUTH_VALIDATE_TOKEN))
