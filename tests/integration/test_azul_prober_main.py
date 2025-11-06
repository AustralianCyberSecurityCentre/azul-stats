from azul_stats.main import (
    AZUL_PROBE_DOCS,
    AZUL_PROBE_RESTAPI,
    AZUL_PROBE_RESTAPI_CONFIGJSON,
    AZUL_PROBE_UI,
    AZUL_PROBE_UI_STATIC,
    FAIL_VALUE,
    SUCCESS_VALUE,
    StatsCollector,
    azul_health_check_timing,
)
from azul_stats.settings import (
    AzulProbeSettings,
    StatsSettings,
    StatTargets,
)

from .base_test import BaseTest


class TestAzulProber(BaseTest):
    """Test the azure prober can probe azul and sets the appropriate stats."""

    def setUp(self) -> None:
        self.cfg = AzulProbeSettings()
        self.system = StatTargets.azul
        # Alter environment before creating stats collection.
        setup_result = super().setUp()
        self.stats_settings = StatsSettings()
        self.stats_settings.azul = True
        self.stats_settings.max_scrape_time = 30
        self.stats_collector = StatsCollector(self.stats_settings)
        return setup_result

    def _set_all_health_to(self, value: int):
        self.set_health_value(AZUL_PROBE_UI, value)
        self.set_health_value(AZUL_PROBE_UI_STATIC, value)
        self.set_health_value(AZUL_PROBE_DOCS, value)
        self.set_health_value(AZUL_PROBE_RESTAPI, value)
        self.set_health_value(AZUL_PROBE_RESTAPI_CONFIGJSON, value)

    async def test_all_stages_succeed(self):
        """Run the azul probe section of the stats to verify it works."""
        self._set_all_health_to(FAIL_VALUE)
        collect_probe = self.stats_collector.azul_probe_setup(self.cfg)
        # Run collection
        await collect_probe()
        # Ensure status stats are captured
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(AZUL_PROBE_UI))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(AZUL_PROBE_UI_STATIC))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(AZUL_PROBE_DOCS))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(AZUL_PROBE_RESTAPI))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(AZUL_PROBE_RESTAPI_CONFIGJSON))
        # Ensure timing stats are captured
        self.assertGreater(azul_health_check_timing.labels(system=self.system, action=AZUL_PROBE_UI)._value.get(), 0)
        self.assertGreater(
            azul_health_check_timing.labels(system=self.system, action=AZUL_PROBE_UI_STATIC)._value.get(),
            0,
        )

    async def test_all_fail_on_bad_azul_url(self):
        """Run prober with an invalid azul URL."""
        self._set_all_health_to(SUCCESS_VALUE)

        self.cfg.url = "https://notvaliddddddd.internal"
        collect_probe = self.stats_collector.azul_probe_setup(self.cfg)
        # Run collection
        await collect_probe()
        # Ensure status stats are captured
        self.assertEqual(FAIL_VALUE, self.get_health_value(AZUL_PROBE_UI))
        self.assertEqual(FAIL_VALUE, self.get_health_value(AZUL_PROBE_UI_STATIC))
        self.assertEqual(FAIL_VALUE, self.get_health_value(AZUL_PROBE_DOCS))
        self.assertEqual(FAIL_VALUE, self.get_health_value(AZUL_PROBE_RESTAPI))
        self.assertEqual(FAIL_VALUE, self.get_health_value(AZUL_PROBE_RESTAPI_CONFIGJSON))

    async def test_stats_fail_with_timeout(self):
        """Run azul prober section of test when given no time."""
        self.set_stats_timeout_value(SUCCESS_VALUE)
        self.stats_settings.max_scrape_time = 0.0
        stats_collector = StatsCollector(self.stats_settings)
        with self.assertRaises(TimeoutError):
            await stats_collector.run(run_once=True)

        # Ensure status stats are captured
        self.assertEqual(FAIL_VALUE, self.get_stats_timeout_value())
