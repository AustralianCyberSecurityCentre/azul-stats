from azul_stats.azure_wrapper import AzureContainerWrapperAsync
from azul_stats.settings import AzureBlobSettings
from azul_stats.utils import random_word
from tests.integration.base_test import BaseTest


class TestAzureWrapper(BaseTest):
    async def test_can_connect_and_container_exists(self):
        """Credentials are working and we can connect to azure blob storage and verify the container exists."""
        client = AzureContainerWrapperAsync(AzureBlobSettings())
        async with client:
            self.assertTrue(await client.container_exists())

    async def test_verify_sas_auth_occurs_first(self):
        """Credentials are working and we can connect to azure blob storage and verify the container exists."""
        cfg = AzureBlobSettings()
        cfg.client_id = ""
        cfg.client_secret = ""
        cfg.tenant_id = ""
        client = AzureContainerWrapperAsync(cfg)

        async with client:
            self.assertTrue(await client.container_exists())

    async def test_verify_upload_and_download_of_blob(self):
        """Upload a blob download a blob and verify it's as expected then delete the blob."""
        client = AzureContainerWrapperAsync(AzureBlobSettings())
        async with client:
            self.assertTrue(await client.auth())
            self.assertTrue(await client.container_exists())
            test_data = random_word(30).encode()
            self.assertTrue(await client.upload_data_to_blob(test_data))
            self.assertTrue(await client.download_blob_and_verify_data(test_data))
            self.assertTrue(await client.delete_blob())

    async def test_fail_to_delete_and_upload_with_bad_client(self):
        """Upload a blob download a blob and verify it's as expected then delete the blob."""
        cfg = AzureBlobSettings()
        cfg.container_name = "random-bad-container-name"
        client = AzureContainerWrapperAsync(cfg)
        async with client:
            self.assertTrue(await client.auth())
            self.assertFalse(await client.container_exists())
            test_data = random_word(30).encode()
            self.assertFalse(await client.upload_data_to_blob(test_data))
            self.assertFalse(await client.download_blob_and_verify_data(test_data))
            self.assertFalse(await client.delete_blob())

    async def test_fail_to_delete_and_upload_with_bad_credentials(self):
        """Upload a blob download a blob and verify it's as expected then delete the blob."""
        cfg = AzureBlobSettings()
        cfg.storage_access_key = "random-bad-secret"
        cfg.client_id = ""
        client = AzureContainerWrapperAsync(cfg)
        async with client:
            self.assertFalse(await client.auth())
            self.assertFalse(await client.container_exists())
            test_data = random_word(30).encode()
            self.assertFalse(await client.upload_data_to_blob(test_data))
            self.assertFalse(await client.download_blob_and_verify_data(test_data))
            self.assertFalse(await client.delete_blob())

    async def test_verify_upload_and_download_of_blob_with_service_principal_auth(self):
        """Upload a blob download a blob and verify it's as expected then delete the blob when using service principal authentication method."""
        cfg = AzureBlobSettings()
        cfg.storage_access_key = ""
        client = AzureContainerWrapperAsync(cfg)
        async with client:
            self.assertTrue(await client.auth())
            self.assertTrue(await client.container_exists())
            test_data = random_word(30).encode()
            self.assertTrue(await client.upload_data_to_blob(test_data))
            self.assertTrue(await client.download_blob_and_verify_data(test_data))
            self.assertTrue(await client.delete_blob())
            # Call delete to verify re-deleting doesn't break anything
            self.assertTrue(await client.delete_blob())
            self.assertTrue(await client.delete_blob())
