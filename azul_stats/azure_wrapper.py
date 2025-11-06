"""Simplified client to enable interacting with Azure container/blob storage."""

import logging
import os
import traceback

from azure.core.exceptions import ClientAuthenticationError
from azure.identity import DefaultAzureCredential
from azure.storage.blob.aio import ContainerClient

from azul_stats.settings import AzureBlobSettings

logger = logging.getLogger(__name__)


class AzureContainerWrapperAsync:
    """Simplification of handling connections to an Azure container."""

    def __init__(self, cfg: AzureBlobSettings):
        self.cfg = cfg
        self._client = None
        self._exists = None

    async def __aenter__(self):
        """Context manager method to open Azure container connection."""
        self.open()
        return self

    async def __aexit__(self, *args):
        """Context manager method to close Azure container connection."""
        await self.close()

    def open(self):
        """Open the connection to the Azure container."""
        cred = self.cfg.storage_access_key
        # If there is no access key use Service Principal with Secret method.
        # More detail in this documentation
        # https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python
        if not cred:
            if self.cfg.client_id and self.cfg.client_secret and self.cfg.tenant_id:
                os.environ["AZURE_CLIENT_ID"] = self.cfg.client_id
                os.environ["AZURE_CLIENT_SECRET"] = self.cfg.client_secret
                os.environ["AZURE_TENANT_ID"] = self.cfg.tenant_id
                cred = DefaultAzureCredential()
            else:
                logger.error(
                    "No authentication configured either provide a SAS credential or"
                    + " set the AZURE_CLIENT_ID, AZURE_CLIENT_SECRET and AZURE_TENANT_ID for 'service principal auth'"
                )

        # This works with a SAS key (accesskey) - found on the azure storage account
        self._client = ContainerClient(
            account_url=self.cfg.host, container_name=self.cfg.container_name, credential=cred
        )

    async def close(self):
        """Close the connection to the Azure container."""
        await self._client.close()

    async def auth(self) -> bool:
        """Verify if the client can authenticate or not."""
        try:
            await self._client.exists()
            # Can auth container might not exist.
            return True
        except ClientAuthenticationError:
            # Can't auth
            logger.warning("Can't authenticate to Azure.")
            return False

    async def container_exists(self) -> bool:
        """Verify that the azure container exists."""
        try:
            if self._exists is None:
                self._exists = await self._client.exists()
            return self._exists
        except Exception as e:
            logger.warning(f"Container existence couldn't be determined with error {e}")
            return False

    async def upload_data_to_blob(self, data: bytes) -> bool:
        """Upload the provided data to the test blob."""
        try:
            # Container must exist for this to work.
            if not await self.container_exists():
                return False
            async with self._client.get_blob_client(self.cfg.test_blob) as blob_client:
                await blob_client.upload_blob(data, overwrite=True)
                return True
        except Exception as e:
            logger.warning(f"Could not upload an azure blob with error {e}")
            return False

    async def download_blob_and_verify_data(self, data: bytes) -> bool:
        """Get an azure blob and verify it's data value is the same as the expected value."""
        try:
            # Container must exist for this to work.
            if not await self.container_exists():
                return False
            async with self._client.get_blob_client(self.cfg.test_blob) as blob_client:
                # If the blob doesn't exist can't download the blob.
                if not await blob_client.exists():
                    return False
                blob_stream = await blob_client.download_blob()
                downloaded_data = await blob_stream.readall()
                return data == downloaded_data
        except Exception as e:
            logger.warning(f"Could not download and compare an azure blob with error {e}")
            return False

    async def delete_blob(self) -> bool:
        """Delete the test blob and return True if successful."""
        try:
            # Container must exist for this to work.
            if not await self.container_exists():
                return False

            async with self._client.get_blob_client(self.cfg.test_blob) as blob_client:
                if not await blob_client.exists():
                    return True

                await blob_client.delete_blob(delete_snapshots="include")
                return True
        except Exception:
            logger.warning(f"Couldn't delete Azure blob with error {traceback.format_exc()}")
            return False
