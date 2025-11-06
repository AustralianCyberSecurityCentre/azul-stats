import time

from azul_stats.azure_wrapper import AzureContainerWrapperAsync
from azul_stats.main import (
    FAIL_VALUE,
    FILESTORE_AUTH,
    FILESTORE_CAN_DOWNLOAD,
    FILESTORE_CAN_UPLOAD,
    FILESTORE_CONTAINER_OR_BUCKET_EXISTS,
    SUCCESS_VALUE,
    StatsCollector,
    azul_health_check_timing,
)
from azul_stats.settings import (
    AzureBlobSettings,
    BackupAzureBlobSettings,
    StatsSettings,
    StatTargets,
)
from azul_stats.utils import random_word

from .base_test import BaseTest


class TestAzureBlobMainFilestore(BaseTest):
    """Test all the azure blob metrics stages including the whole azure blob collection stage."""

    def setUp(self) -> None:
        self.cfg = AzureBlobSettings()
        self.system = StatTargets.storage
        # Alter environment before creating stats collection.
        setup_result = super().setUp()
        self.stats_settings = StatsSettings()
        self.stats_settings.azure_blob = True
        self.stats_settings.max_scrape_time = 30
        self.stats_collector = StatsCollector(self.stats_settings)
        self.dummy_data = random_word(50).encode()
        return setup_result

    def _set_all_health_to(self, value: int):
        self.set_health_value(FILESTORE_AUTH, value)
        self.set_health_value(FILESTORE_CONTAINER_OR_BUCKET_EXISTS, value)
        self.set_health_value(FILESTORE_CAN_UPLOAD, value)
        self.set_health_value(FILESTORE_CAN_DOWNLOAD, value)

    async def test_all_stages_succeed(self):
        """Run just the azure section of the stats tests."""
        self._set_all_health_to(FAIL_VALUE)
        collect_azure = self.stats_collector.azure_blob_setup(self.system, self.cfg)
        # Run collection
        await collect_azure()
        # Ensure status stats are captured
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(FILESTORE_AUTH))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(FILESTORE_CONTAINER_OR_BUCKET_EXISTS))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(FILESTORE_CAN_UPLOAD))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(FILESTORE_CAN_DOWNLOAD))
        # Ensure timing stats are captured
        self.assertGreater(azul_health_check_timing.labels(system=self.system, action=FILESTORE_AUTH)._value.get(), 0)
        self.assertGreater(
            azul_health_check_timing.labels(
                system=self.system, action=FILESTORE_CONTAINER_OR_BUCKET_EXISTS
            )._value.get(),
            0,
        )
        self.assertGreater(
            azul_health_check_timing.labels(system=self.system, action=FILESTORE_CAN_UPLOAD)._value.get(), 0
        )
        self.assertGreater(
            azul_health_check_timing.labels(system=self.system, action=FILESTORE_CAN_DOWNLOAD)._value.get(), 0
        )

    async def test_all_fail_on_bad_azure_creds(self):
        """Run azure blob section of test with bad access key."""
        self._set_all_health_to(SUCCESS_VALUE)

        self.cfg.storage_access_key = "bad-accesskey"
        collect_azure = self.stats_collector.azure_blob_setup(self.system, self.cfg)
        await collect_azure()
        # Ensure status stats are captured
        self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_AUTH))
        self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CONTAINER_OR_BUCKET_EXISTS))
        self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CAN_UPLOAD))
        self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CAN_DOWNLOAD))

    async def test_stats_fail_with_timeout(self):
        """Run azure blob section of test when given no time."""
        self.set_stats_timeout_value(SUCCESS_VALUE)
        self.stats_settings.max_scrape_time = 0.0
        stats_collector = StatsCollector(self.stats_settings)
        with self.assertRaises(TimeoutError):
            await stats_collector.run(run_once=True)

        # Ensure status stats are captured
        self.assertEqual(FAIL_VALUE, self.get_stats_timeout_value())

    async def test_auth_stage_fails(self):
        """Test that connect fails when bootstrap server is set to an invalid value."""

        self.cfg.storage_access_key = "bad-accesskey"
        azw = AzureContainerWrapperAsync(self.cfg)
        async with azw:
            self._set_all_health_to(SUCCESS_VALUE)
            await self.stats_collector._run_async_stage(FILESTORE_AUTH, self.system, azw.auth)
            self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_AUTH))

    async def test_parent_exists_stage_fails(self):
        """Test that parent exists stage fails when access key is invalid value."""
        self._set_all_health_to(SUCCESS_VALUE)
        self.cfg.storage_access_key = "bad-accesskey"
        azw = AzureContainerWrapperAsync(self.cfg)
        async with azw:
            self.set_health_value(FILESTORE_CONTAINER_OR_BUCKET_EXISTS, SUCCESS_VALUE)
            await self.stats_collector._run_async_stage(
                FILESTORE_CONTAINER_OR_BUCKET_EXISTS, self.system, azw.container_exists
            )
            self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CONTAINER_OR_BUCKET_EXISTS))

    async def test_upload_blob_to_bucket(self):
        """Test that uploads a blob to a bucket."""
        # Fail when bucket doesn't exist
        self._set_all_health_to(SUCCESS_VALUE)
        self.cfg.container_name = "imaginary-container-that-does-not-exist"
        self.set_health_value(FILESTORE_CAN_UPLOAD, SUCCESS_VALUE)

        azw = AzureContainerWrapperAsync(self.cfg)
        async with azw:
            await self.stats_collector._run_async_stage(
                FILESTORE_CAN_UPLOAD,
                self.system,
                azw.upload_data_to_blob,
                data=self.dummy_data,
            )
            self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CAN_UPLOAD))

        # Fail with bad auth
        self._set_all_health_to(SUCCESS_VALUE)
        self.cfg.storage_access_key = "bad-accesskey"

        self.set_health_value(FILESTORE_CAN_UPLOAD, SUCCESS_VALUE)
        azw = AzureContainerWrapperAsync(self.cfg)
        async with azw:
            await self.stats_collector._run_async_stage(
                FILESTORE_CAN_UPLOAD,
                self.system,
                azw.upload_data_to_blob,
                data=self.dummy_data,
            )
            self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CAN_UPLOAD))

    async def test_download_blob_from_blob(self):
        """Test that downloading a blob from a blob fails under various conditions."""
        # Fail when blob doesn't exist
        self._set_all_health_to(SUCCESS_VALUE)
        azw = AzureContainerWrapperAsync(self.cfg)
        async with azw:
            self.assertTrue(await azw.delete_blob())
            self.set_health_value(FILESTORE_CAN_DOWNLOAD, SUCCESS_VALUE)
            await self.stats_collector._run_async_stage(
                FILESTORE_CAN_DOWNLOAD,
                self.system,
                azw.download_blob_and_verify_data,
                data=self.dummy_data,
            )
            self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CAN_DOWNLOAD))

        # Fail when bucket doesn't exist
        self._set_all_health_to(SUCCESS_VALUE)
        original_container_name = self.cfg.container_name
        self.cfg.container_name = "imaginary-container-that-does-not-exist"
        azw = AzureContainerWrapperAsync(self.cfg)
        async with azw:
            await self.stats_collector._run_async_stage(
                FILESTORE_CAN_DOWNLOAD,
                self.system,
                azw.download_blob_and_verify_data,
                data=self.dummy_data,
            )
            self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CAN_DOWNLOAD))
            self.cfg.container_name = original_container_name

        # Fail with bad auth
        self.cfg.storage_access_key = "bad-accesskey"
        azw = AzureContainerWrapperAsync(self.cfg)
        async with azw:
            self._set_all_health_to(SUCCESS_VALUE)
            await self.stats_collector._run_async_stage(
                FILESTORE_CAN_DOWNLOAD,
                self.system,
                azw.download_blob_and_verify_data,
                data=self.dummy_data,
            )
            self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CAN_DOWNLOAD))


class TestBackupAzureBlobMainFilestore(TestAzureBlobMainFilestore):
    """Test all the backup azure blob metrics stages including the whole backup azure blob collection stage."""

    def setUp(self) -> None:
        self.cfg = BackupAzureBlobSettings()
        self.system = StatTargets.backup
        # Alter environment before creating stats collection.
        setup_result = super().setUp()
        self.stats_settings = StatsSettings()
        self.stats_settings.azure_blob = False
        self.stats_settings.backup_azure_blob = True
        self.stats_settings.max_scrape_time = 30
        self.stats_collector = StatsCollector(self.stats_settings)
        self.dummy_data = random_word(50).encode()
        return setup_result
