"""Simplified client to enable interacting with Azure container/blob storage."""

import io
import logging
import traceback

from minio import Minio, S3Error
from minio.credentials import IamAwsProvider

from azul_stats.settings import MinioSettings

logger = logging.getLogger(__name__)


class MinioWrapper:
    """Simplification of handling connections to Minio container."""

    def __init__(self, cfg: MinioSettings):
        self.cfg = cfg
        self._client = None
        self._exists = None

        params = {"endpoint": self.cfg.host, "secure": self.cfg.certificate_verification}

        if self.cfg.accesskey.strip() == "":
            # If no access key is specified, use an environmental role instead
            params["credentials"] = IamAwsProvider()
        else:
            params["access_key"] = self.cfg.accesskey
            params["secret_key"] = self.cfg.secretkey

        self._client = Minio(**params)

    def auth(self) -> bool:
        """Verify that the minio client can auth to the minio server."""
        try:
            self._client.bucket_exists(self.cfg.bucket_name)
        except S3Error as e:
            logger.error(
                f"Failed to authenticate to minio with the S3 error code {e.code} and status {e.response.status}"
            )
            if e.response.status == 403:
                logger.error("Failed to authenticate to azure because credentials are bad or invalid.")
            elif e.response.status == 401:
                logger.error("Failure reason because credentials aren't authorized to view the test bucket.")
            return False
        except Exception as e:
            logger.error(f"Failed to authenticate with unexpected error {e}")
            return False
        return True

    def exists(self) -> bool:
        """Verify that the minio bucket exists."""
        try:
            return self._client.bucket_exists(self.cfg.bucket_name)
        except Exception as e:
            logger.warning(f"Failed to verify bucket exists with error {e}")
            return False

    def upload_data_to_blob(self, data: bytes):
        """Upload the provided data to the test blob."""
        try:
            self._client.put_object(self.cfg.bucket_name, self.cfg.test_blob, io.BytesIO(data), len(data))
        except Exception as e:
            logger.warning(f"Failed to upload blob to minio with exception {e}")
            return False
        return True

    def download_blob_and_verify_data(self, data: bytes) -> bool:
        """Download a blob and verify it's data is the same as the expected data value."""
        try:
            response = self._client.get_object(self.cfg.bucket_name, self.cfg.test_blob)
            return response.data == data
        except Exception as e:
            logger.warning(f"Failed to download blob from minio with exception {e}")
            return False

    def _check_blob_exists(self) -> bool:
        """Check if the test blob exists."""
        try:
            self._client.stat_object(self.cfg.bucket_name, self.cfg.test_blob)
        except S3Error:
            return False
        return True

    def delete_blob(self) -> bool:
        """Delete the test blob and return True if successful."""
        try:
            if self._check_blob_exists():
                self._client.remove_object(self.cfg.bucket_name, self.cfg.test_blob)
            return True
        except Exception:
            logger.warning(f"Couldn't delete object from minio for reason {traceback.format_exc()}")
            return False
