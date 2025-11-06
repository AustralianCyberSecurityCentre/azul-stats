from azul_stats.azul_probe import AsyncAzulProbe, AsyncAzulProbeManager
from azul_stats.settings import AzulProbeSettings
from tests.integration.base_test import BaseTest


class TestAzureWrapper(BaseTest):
    async def test_probe_webui(self):
        """Verify the azul webui can be probed."""
        client = AsyncAzulProbe(AzulProbeSettings())
        async with AsyncAzulProbeManager(client):
            self.assertTrue(await client.probe_ui())
            self.assertTrue(await client.probe_ui_static())

    async def test_probe_fails_with_no_slash(self):
        """Verify the azul webui fails to be probed when the UI path has no trailing slash."""
        cur_settings = AzulProbeSettings()
        cur_settings.ui_path = cur_settings.ui_path.rstrip("/")
        client = AsyncAzulProbe(cur_settings)
        async with AsyncAzulProbeManager(client):
            self.assertFalse(await client.probe_ui())

    async def test_probe_restapi(self):
        """Verify the azul webui can be probed."""
        client = AsyncAzulProbe(AzulProbeSettings())
        async with AsyncAzulProbeManager(client):
            self.assertTrue(await client.probe_ui())
            self.assertTrue(await client.probe_ui_static())

    async def test_probe_docs(self):
        """Verify the azul webui can be probed."""
        client = AsyncAzulProbe(AzulProbeSettings())
        async with AsyncAzulProbeManager(client):
            self.assertTrue(await client.probe_ui())
            self.assertTrue(await client.probe_ui_static())
