from azul_stats.minio_wrapper import MinioWrapper
from azul_stats.settings import MinioSettings
from azul_stats.utils import random_word
from tests.integration.base_test import BaseTest


class TestMinioWrapper(BaseTest):
    @classmethod
    def setUpClass(cls):
        val = super().setUpClass()
        cls.cfg = MinioSettings()
        cls.client = MinioWrapper(cls.cfg)
        return val

    def test_can_connect_and_bucket_exists(self):
        """Test you can verify the minio bucket exists"""
        self.assertTrue(self.client.auth)
        self.assertTrue(self.client.exists())

    def test_verify_upload_and_download_of_blob(self):
        """Check upload and download verification works as expected."""
        self.assertTrue(self.client.auth())
        # Delete the blob if it exists
        self.client.delete_blob()

        self.assertTrue(self.client.exists())
        test_data = random_word(30).encode()
        self.assertTrue(self.client.upload_data_to_blob(test_data))
        self.assertTrue(self.client.download_blob_and_verify_data(test_data))
        self.assertTrue(self.client.delete_blob())

    def test_verify_fail_case(self):
        """Check upload and download verification works as expected."""
        cfg = MinioSettings()
        cfg.bucket_name = "bucket-that-should-not-exist"
        client = MinioWrapper(cfg)
        self.assertTrue(client.auth())
        self.assertFalse(client.exists())
        test_data = random_word(30).encode()
        self.assertFalse(client.upload_data_to_blob(test_data))
        self.assertFalse(client.download_blob_and_verify_data(test_data))
        self.assertTrue(client.delete_blob())

    def test_verify_fail_case_no_auth(self):
        """Check upload and download verification works as expected."""
        cfg = MinioSettings()
        cfg.secretkey = "what-this-key-is-bad?"
        client = MinioWrapper(cfg)
        self.assertFalse(client.auth())
        self.assertFalse(client.exists())
        test_data = random_word(30).encode()
        self.assertFalse(client.upload_data_to_blob(test_data))
        self.assertFalse(client.download_blob_and_verify_data(test_data))
        self.assertTrue(client.delete_blob())
