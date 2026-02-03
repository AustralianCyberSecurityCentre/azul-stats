"""Test cases for plugin output."""

import os
import traceback
from unittest import IsolatedAsyncioTestCase

from azul_stats.main import TIMEOUT_LABEL, azul_health_check_status, stats_status
from azul_stats.minio_wrapper import MinioWrapper
from azul_stats.settings import MinioSettings


def sius(key, data):
    if not os.environ.get(key):
        if os.environ.get(key.upper()):
            os.environ[key] = os.environ.get(key.upper())
        else:
            os.environ[key] = data
    return os.environ[key]


class BaseTest(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.alter_env()
        # Ensure minio has it's test bucket and create it if it doesn't
        try:
            cfg = MinioSettings()
            client = MinioWrapper(cfg)
            if not client.exists():
                client._client.make_bucket(cfg.bucket_name)
        except Exception:
            print(
                f"WARNING - error occurred when attempting to create minio bucket, stacktrace {traceback.format_exc()}"
            )
        # Expected to be set by child if using health check helpers.
        cls.system = ""

    @classmethod
    def alter_env(self):
        # Opensearch
        sius("opensearch_username", "azul_writer")
        sius("opensearch_password", "dummyPassword!")
        sius("opensearch_host", "https://localhost:9204")
        sius("opensearch_certificate_verification", "false")
        # Kafka
        sius("kafka_bootstrap_server", "localhost:9092")
        # Redis
        sius("redis_host", "localhost")
        sius("redis_port", "6379")
        sius("redis_username", "default")
        sius("redis_password", "password")
        # Azure - not all set for local testing.
        sius("azure_host", "")  # TEST - needs to be set but is private
        sius("azure_container_name", "")  # TEST - needs to be set but is private

        # Option A auth settings - storage access key
        sius("azure_storage_access_key", "")  # TEST - needs to be set but is private
        # Option B auth settings
        sius("azure_client_id", "")  # TEST - needs to be set but is private
        sius("azure_tenant_id", "")  # TEST - needs to be set but is private
        sius("azure_client_secret", "")  # TEST - needs to be set but is private

        # Azure backup - not set for local testing.
        sius("azure_bkup_host", "")  # TEST - needs to be set but is private
        sius("azure_bkup_container_name", "")  # TEST - needs to be set but is private
        # Option A auth settings - storage access key
        sius(
            "azure_bkup_storage_access_key",
            # Secret can't be left in because it's a real secret
            "",
        )
        # Prevent race condition where the non-backup azure can delete the blob while backup azure is checking for existence
        sius("azure_bkup_test_blob", "test-blob-backup")

        # Minio
        sius("minio_host", "localhost:9000")
        sius("minio_accesskey", "dummyS3accessKEY")
        sius("minio_secretkey", "dummyS3secretKEY")
        sius("minio_bucket_name", "test-bucket")
        sius("minio_certificate_verification", "false")

        # backup Minio
        sius("minio_bkup_host", "localhost:9000")
        sius("minio_bkup_accesskey", "dummyS3accessKEY")
        sius("minio_bkup_secretkey", "dummyS3secretKEY")
        sius("minio_bkup_bucket_name", "test-bucket")
        sius("minio_bkup_certificate_verification", "false")
        # Prevent race condition where the non-backup minio can delete the blob while backup minio is checking for existence
        sius("minio_bkup_test_blob", "test-blob-backup")

        # Auth
        sius("auth_well_known_endpoint", "")  # TEST - needs to be set but is private
        sius("auth_client_id", "")  # TEST - needs to be set but is private
        sius("auth_client_secret", "")  # TEST - needs to be set but is private
        sius("auth_scopes", "openid profile email offline_access")  # TEST - may need modification for your environment
        sius("auth_expected_audience", "")  # TEST - needs to be set but is private
        sius("auth_expected_roles", '["azul_read"]')

    def get_stats_timeout_value(self) -> int:
        return stats_status.labels(reason=TIMEOUT_LABEL)._value.get()

    def set_stats_timeout_value(self, value: int) -> int:
        return stats_status.labels(reason=TIMEOUT_LABEL).set(value)

    def get_health_value(self, action: str) -> int:
        if self.system == "":
            raise Exception("Need to set 'system' variable by overriding setUpClass in Test class.")
        return azul_health_check_status.labels(system=self.system, action=action)._value.get()

    def set_health_value(self, action: str, value: int):
        if self.system == "":
            raise Exception("Need to set 'system' variable by overriding setUpClass in Test class.")
        azul_health_check_status.labels(system=self.system, action=action).set(value)

    def __revert_env(self, key: str):
        """Set the env back to the original value after a test run."""
        old_value = os.environ.get(key)

        def inner_revert():
            if old_value is not None:
                os.environ[key] = old_value
            else:
                del os.environ[key]

        return inner_revert

    def set_bad_env(self, key: str, value: str):
        if not os.environ.get(key) and os.environ.get(key.upper()):
            key = key.upper()

        self.addCleanup(self.__revert_env(key))
        os.environ[key] = value
