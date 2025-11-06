import time

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
from azul_stats.minio_wrapper import MinioWrapper
from azul_stats.settings import (
    BackupMinioSettings,
    MinioSettings,
    StatsSettings,
    StatTargets,
)
from azul_stats.utils import random_word

from .base_test import BaseTest


class TestMinioMainFilestore(BaseTest):
    """Test all the Minio metrics stages including the whole minio collection stage."""

    def setUp(self) -> None:
        self.cfg = MinioSettings()
        self.system = StatTargets.storage
        # Alter environment before creating stats collection.
        setup_result = super().setUp()
        self.stats_settings = StatsSettings()
        self.stats_settings.minio = True
        self.stats_settings.max_scrape_time = 30
        self.stats_collector = StatsCollector(self.stats_settings)
        self.start_time = time.time()
        self.timeout_sec = 10
        self.dummy_data = random_word(50).encode()
        return setup_result

    def _set_all_health_to(self, value: int):
        self.set_health_value(FILESTORE_AUTH, value)
        self.set_health_value(FILESTORE_CONTAINER_OR_BUCKET_EXISTS, value)
        self.set_health_value(FILESTORE_CAN_UPLOAD, value)
        self.set_health_value(FILESTORE_CAN_DOWNLOAD, value)

    def test_all_stages_succeed(self):
        """Run just the minio section of the stats tests."""
        self._set_all_health_to(FAIL_VALUE)
        collect_minio = self.stats_collector.minio_setup(self.system, self.cfg)
        # Run collection
        collect_minio(self.timeout_sec)
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

    def test_all_fail_on_bad_minio_access_key(self):
        """Run minio section of test with bad access key."""
        self._set_all_health_to(SUCCESS_VALUE)

        self.cfg.accesskey = "bad-minio-accesskey"
        collect_minio = self.stats_collector.minio_setup(self.system, self.cfg)
        collect_minio(self.timeout_sec)
        # Ensure status stats are captured
        self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_AUTH))
        self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CONTAINER_OR_BUCKET_EXISTS))
        self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CAN_UPLOAD))
        self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CAN_DOWNLOAD))

    async def test_stats_fail_with_timeout(self):
        """Run minio section of test with bad minio url."""
        # Timeout running stage
        collect_minio = self.stats_collector.minio_setup(self.system, self.cfg)
        with self.assertRaises(TimeoutError):
            collect_minio(0.0)

        # Timeout running stats collector
        self.set_stats_timeout_value(SUCCESS_VALUE)
        self.stats_settings.max_scrape_time = 0.0
        stats_collector = StatsCollector(self.stats_settings)
        with self.assertRaises(TimeoutError):
            await stats_collector.run(run_once=True)

        # Ensure status stats are captured
        self.assertEqual(FAIL_VALUE, self.get_stats_timeout_value())

    def test_auth_stage_fails(self):
        """Test that connect fails when bootstrap server is set to an invalid value."""

        self.cfg.accesskey = "bad-minio-accesskey"
        mw = MinioWrapper(self.cfg)

        self._set_all_health_to(SUCCESS_VALUE)
        self.stats_collector._run_threaded_stage(
            FILESTORE_AUTH, self.system, mw.auth, self.start_time, self.timeout_sec
        )
        self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_AUTH))

    def test_parent_exists_stage_fails(self):
        """Test that parent exists stage fails when access key is invalid value."""
        self._set_all_health_to(SUCCESS_VALUE)
        self.cfg.accesskey = "bad-minio-accesskey"
        mw = MinioWrapper(self.cfg)

        self.set_health_value(FILESTORE_CONTAINER_OR_BUCKET_EXISTS, SUCCESS_VALUE)
        self.stats_collector._run_threaded_stage(
            FILESTORE_CONTAINER_OR_BUCKET_EXISTS, self.system, mw.exists, self.start_time, self.timeout_sec
        )
        self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CONTAINER_OR_BUCKET_EXISTS))

    def test_upload_blob_to_bucket(self):
        """Test that upload a blob to a bucket."""
        # Fail when bucket doesn't exist
        self._set_all_health_to(SUCCESS_VALUE)
        self.cfg.bucket_name = "imaginary-bucket-that-does-not-exist"
        mw = MinioWrapper(self.cfg)
        self.set_health_value(FILESTORE_CAN_UPLOAD, SUCCESS_VALUE)
        self.stats_collector._run_threaded_stage(
            FILESTORE_CAN_UPLOAD,
            self.system,
            mw.upload_data_to_blob,
            self.start_time,
            self.timeout_sec,
            data=self.dummy_data,
        )
        self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CAN_UPLOAD))

        # Fail with bad auth
        self._set_all_health_to(SUCCESS_VALUE)
        self.cfg.accesskey = "bad-minio-accesskey"
        mw = MinioWrapper(self.cfg)
        self.set_health_value(FILESTORE_CAN_UPLOAD, SUCCESS_VALUE)
        self.stats_collector._run_threaded_stage(
            FILESTORE_CAN_UPLOAD,
            self.system,
            mw.upload_data_to_blob,
            self.start_time,
            self.timeout_sec,
            data=self.dummy_data,
        )
        self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CAN_UPLOAD))

    def test_download_blob_from_blob(self):
        """Test that downloading a blob from a blob fails under various conditions."""
        # Fail when blob doesn't exist
        self._set_all_health_to(SUCCESS_VALUE)
        mw = MinioWrapper(self.cfg)
        self.assertTrue(mw.delete_blob())
        self.set_health_value(FILESTORE_CAN_DOWNLOAD, SUCCESS_VALUE)
        self.stats_collector._run_threaded_stage(
            FILESTORE_CAN_DOWNLOAD,
            self.system,
            mw.download_blob_and_verify_data,
            self.start_time,
            self.timeout_sec,
            data=self.dummy_data,
        )
        self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CAN_DOWNLOAD))

        # Fail when bucket doesn't exist
        self._set_all_health_to(SUCCESS_VALUE)
        original_bucket_name = self.cfg.bucket_name
        self.cfg.bucket_name = "imaginary-bucket-that-does-not-exist"
        mw = MinioWrapper(self.cfg)
        self.stats_collector._run_threaded_stage(
            FILESTORE_CAN_DOWNLOAD,
            self.system,
            mw.download_blob_and_verify_data,
            self.start_time,
            self.timeout_sec,
            data=self.dummy_data,
        )
        self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CAN_DOWNLOAD))
        self.cfg.bucket_name = original_bucket_name

        # Fail with bad auth
        self.cfg.accesskey = "bad-minio-accesskey"
        mw = MinioWrapper(self.cfg)
        self._set_all_health_to(SUCCESS_VALUE)
        self.stats_collector._run_threaded_stage(
            FILESTORE_CAN_DOWNLOAD,
            self.system,
            mw.download_blob_and_verify_data,
            self.start_time,
            self.timeout_sec,
            data=self.dummy_data,
        )
        self.assertEqual(FAIL_VALUE, self.get_health_value(FILESTORE_CAN_DOWNLOAD))


class TestMinioMainBackup(TestMinioMainFilestore):
    """Same tests for backup"""

    def setUp(self) -> None:
        self.cfg = BackupMinioSettings()
        self.system = StatTargets.backup
        # Alter environment before creating stats collection.
        setup_result = super().setUp()
        self.stats_settings = StatsSettings()
        self.stats_settings.backup_minio = True
        self.stats_settings.minio = False
        self.stats_settings.max_scrape_time = 30
        self.stats_collector = StatsCollector(self.stats_settings)
        self.stats_collector.minio_setup(self.system, self.cfg)
        self.start_time = time.time()
        self.timeout_sec = 10
        self.dummy_data = random_word(50).encode()
        return setup_result
