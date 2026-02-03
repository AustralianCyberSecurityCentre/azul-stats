"""Settings for the Stats module."""

from enum import StrEnum
from typing import Annotated

from pydantic import AfterValidator
from pydantic_settings import BaseSettings, SettingsConfigDict


class StatTargets(StrEnum):
    """Enum of possible services to target when collecting stats."""

    opensearch = "opensearch"
    kafka = "kafka"
    storage = "storage"
    backup = "backup"
    redis = "redis"
    auth = "auth"
    azul = "azul"


class StatsSettings(BaseSettings):
    """Settings for the azul stats module."""

    model_config = SettingsConfigDict(env_prefix="stats_")

    # Port that prometheus will scrape from.
    prometheus_port: int = 8900
    max_scrape_time: float = 60

    # Enable different scrape options
    azul: bool = False
    opensearch: bool = False
    kafka: bool = False
    redis: bool = False
    auth: bool = False
    # Primary version of storage
    minio: bool = False
    azure_blob: bool = False
    # Backup version of storage
    backup_minio: bool = False
    backup_azure_blob: bool = False


class OpensearchSettings(BaseSettings):
    """Opensearch specific settings."""

    model_config = SettingsConfigDict(env_prefix="opensearch_")
    # Auth settings (username and password or jwt).
    username: str | None = None
    password: str | None = None
    jwt: str | None = None

    # Opensearch datastore cluster url.
    host: str
    # Scrape health and shard stats
    scrape_stats: bool = True
    # Expression used to determine what index to scrape for health and shard stats
    scrape_index_pattern: str = "azul*"

    # Test indices and aliases
    test_alias: str = "azul.test.alias"
    test_index: str = "azul.test.index"
    test_index_pattern: str = "azul.test.*"

    # Certificate verification enabled or not (disable for testing primarily.)
    certificate_verification: bool = True


class KafkaSettings(BaseSettings):
    """Kafka specific settings."""

    model_config = SettingsConfigDict(env_prefix="kafka_")
    # URL for kafka bootstrap server.
    bootstrap_server: str
    # Max timeout for all kafka operations.
    max_timeout_ms: int = 30000  # 2000ms in the library, willing to wait though.
    test_topic: str = "test-kafka-up-topic"


class AzureBlobSettings(BaseSettings):
    """Azure blob specific settings."""

    model_config = SettingsConfigDict(env_prefix="azure_")
    # URL for Azure blob storage (storage account name address).
    host: str
    # Option A - Storage account Access key. (SAS key)
    storage_access_key: str = ""

    # Option B - Note, these environment variables are defined as per the documentation found here
    # https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python
    # For Service Principal with Secret auth.
    client_id: str = ""  # MUST map to AZURE_CLIENT_ID
    tenant_id: str = ""  # MUST map to AZURE_TENANT_ID
    client_secret: str = ""  # MUST map to AZURE_CLIENT_SECRET

    # Container to connect to
    container_name: str
    # Blob to create in the bucket.
    test_blob: str = "test_blob"


class BackupAzureBlobSettings(AzureBlobSettings):
    """Azure blob storage backup settings."""

    model_config = SettingsConfigDict(env_prefix="azure_bkup_")


class MinioSettings(BaseSettings):
    """Minio specific settings."""

    model_config = SettingsConfigDict(env_prefix="minio_")
    # URL for minio
    host: str
    # Username/password for authenticating to minio.
    accesskey: str = ""
    secretkey: str = ""

    # Bucket to connect to
    bucket_name: str
    # Blob to create in the bucket.
    test_blob: str = "test_blob"

    # Enable SSL and certificate verification
    certificate_verification: bool = True


class BackupMinioSettings(MinioSettings):
    """Minio backup settings."""

    model_config = SettingsConfigDict(env_prefix="minio_bkup_")


class RedisSettings(BaseSettings):
    """Settings specific to contacting redis."""

    model_config = SettingsConfigDict(env_prefix="redis_")

    host: str
    username: str = "default"
    password: str
    port: int = 6379
    db: int = 0

    test_key: str = "testKey"
    test_value: str = "testvalue"


class OAuthMethodEnum(StrEnum):
    """Supported oauth methods (tested and all work for Keycloak and Azure)."""

    client_secret_post = "client_secret_post"  # noqa S105
    client_secret_basic = "client_secret_basic"  # noqa S105


class AuthSettings(BaseSettings):
    """Settings specific to contacting auth."""

    model_config = SettingsConfigDict(env_prefix="auth_")

    well_known_endpoint: str
    client_id: str
    client_secret: str

    oauth_method: str = OAuthMethodEnum.client_secret_basic.value
    scopes: str = "openid profile email offline_access"

    expected_audience: str = ""
    expected_roles: list[str] = []


class AzulProbeSettings(BaseSettings):
    """Settings specific to contacting and verifying Azul is up and running."""

    model_config = SettingsConfigDict(env_prefix="azul_")

    # Number of seconds before a timeout occurs on a request.
    timeout_seconds: int = 5

    # Url that points to the base of Azul e.g https://azul.internal and sub urls would be:
    url: Annotated[str, AfterValidator(lambda u: u.rstrip("/"))]

    # These are the sub urls, note - these shouldn't need to be changed.
    restapi_path: Annotated[str, AfterValidator(lambda url: url.rstrip("/"))] = "/api"
    restapi_openapi_path: Annotated[str, AfterValidator(lambda url: url.rstrip("/"))] = "/api/openapi.json"

    docs_path: str = "/docs/"

    ui_path: str = "/ui/"
    ui_static_file_path: Annotated[str, AfterValidator(lambda url: url.rstrip("/"))] = "/ui/assets/config.json"

    # Perform additional checks with credentials (taken from authSettings if they are set). FUTURE
    # with_auth: bool = True
