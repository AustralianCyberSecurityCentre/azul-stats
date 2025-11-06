from azul_stats.main import (
    AUTH_GET_TOKEN,
    AUTH_VALIDATE_TOKEN,
    AZUL_PROBE_DOCS,
    AZUL_PROBE_RESTAPI,
    AZUL_PROBE_RESTAPI_CONFIGJSON,
    AZUL_PROBE_UI,
    AZUL_PROBE_UI_STATIC,
    FAIL_VALUE,
    FILESTORE_AUTH,
    FILESTORE_CAN_DOWNLOAD,
    FILESTORE_CAN_UPLOAD,
    FILESTORE_CONTAINER_OR_BUCKET_EXISTS,
    KAFKA_CONNECT,
    KAFKA_CONSUME_TOPIC,
    KAFKA_CREATE_TOPIC,
    KAFKA_PRODUCE_TOPIC,
    OPENSEARCH_AUTH_LABEL,
    OPENSEARCH_CREATE_INDEX_LABEL,
    OPENSEARCH_GET_AGG_LABEL,
    OPENSEARCH_GET_DOC_LABEL,
    OPENSEARCH_INDEX_DOCS_LABEL,
    REDIS_AUTH,
    REDIS_GET_KEY,
    REDIS_SET_KEY,
    SUCCESS_VALUE,
    StatsCollector,
)
from azul_stats.settings import StatsSettings, StatTargets
from tests.integration.base_test import BaseTest


class TestKafkaStatsMethod(BaseTest):
    """Test all the Opensearch metrics stages including the whole opensearch collection stage."""

    def setUp(self) -> None:
        # Alter environment before creating stats collection.
        setup_result = super().setUp()
        self.settings = StatsSettings()
        self.settings.azul = True
        self.settings.opensearch = True
        self.settings.kafka = True
        self.settings.redis = True
        self.settings.auth = True
        self.settings.minio = True
        self.settings.backup_minio = True
        self.stats_collector = StatsCollector(self.settings)
        # Ensure all values start as fails.
        # Azul Proober
        self.system = StatTargets.azul
        self.set_health_value(AZUL_PROBE_UI, FAIL_VALUE)
        self.set_health_value(AZUL_PROBE_UI_STATIC, FAIL_VALUE)
        self.set_health_value(AZUL_PROBE_DOCS, FAIL_VALUE)
        self.set_health_value(AZUL_PROBE_RESTAPI, FAIL_VALUE)
        self.set_health_value(AZUL_PROBE_RESTAPI_CONFIGJSON, FAIL_VALUE)
        # Opensearch
        self.system = StatTargets.opensearch
        self.set_health_value(OPENSEARCH_AUTH_LABEL, FAIL_VALUE)
        self.set_health_value(OPENSEARCH_CREATE_INDEX_LABEL, FAIL_VALUE)
        self.set_health_value(OPENSEARCH_INDEX_DOCS_LABEL, FAIL_VALUE)
        self.set_health_value(OPENSEARCH_GET_DOC_LABEL, FAIL_VALUE)
        self.set_health_value(OPENSEARCH_GET_AGG_LABEL, FAIL_VALUE)
        # Kafka
        self.system = StatTargets.kafka
        self.set_health_value(KAFKA_CONNECT, FAIL_VALUE)
        self.set_health_value(KAFKA_CREATE_TOPIC, FAIL_VALUE)
        self.set_health_value(KAFKA_PRODUCE_TOPIC, FAIL_VALUE)
        self.set_health_value(KAFKA_CONSUME_TOPIC, FAIL_VALUE)
        # Redis
        self.set_health_value(REDIS_AUTH, FAIL_VALUE)
        self.set_health_value(REDIS_SET_KEY, FAIL_VALUE)
        self.set_health_value(REDIS_GET_KEY, FAIL_VALUE)
        # Auth
        self.set_health_value(AUTH_GET_TOKEN, FAIL_VALUE)
        self.set_health_value(AUTH_VALIDATE_TOKEN, FAIL_VALUE)
        # For backup/storage minio/azure blob set all the health values.
        self.system = StatTargets.backup
        for system in [StatTargets.backup, StatTargets.storage]:
            self.system = system
            self.set_health_value(FILESTORE_AUTH, FAIL_VALUE)
            self.set_health_value(FILESTORE_CONTAINER_OR_BUCKET_EXISTS, FAIL_VALUE)
            self.set_health_value(FILESTORE_CAN_UPLOAD, FAIL_VALUE)
            self.set_health_value(FILESTORE_CAN_DOWNLOAD, FAIL_VALUE)
        return setup_result

    def _assertions(self):
        # Azul Prober is working
        self.system = StatTargets.azul
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(AZUL_PROBE_UI))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(AZUL_PROBE_UI_STATIC))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(AZUL_PROBE_DOCS))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(AZUL_PROBE_RESTAPI))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(AZUL_PROBE_RESTAPI_CONFIGJSON))
        # Opensearch is working.
        self.system = StatTargets.opensearch
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_AUTH_LABEL))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_CREATE_INDEX_LABEL))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_INDEX_DOCS_LABEL))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_GET_DOC_LABEL))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_GET_AGG_LABEL))
        # kafka is working
        self.system = StatTargets.kafka
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(KAFKA_CONNECT))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(KAFKA_CREATE_TOPIC))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(KAFKA_PRODUCE_TOPIC))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(KAFKA_CONSUME_TOPIC))
        # Redis
        self.system = StatTargets.redis
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(REDIS_AUTH))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(REDIS_SET_KEY))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(REDIS_GET_KEY))
        # Auth
        self.system = StatTargets.auth
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(AUTH_GET_TOKEN))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(AUTH_VALIDATE_TOKEN))
        # Minio is working
        self.system = StatTargets.storage
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(FILESTORE_AUTH))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(FILESTORE_CONTAINER_OR_BUCKET_EXISTS))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(FILESTORE_CAN_UPLOAD))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(FILESTORE_CAN_DOWNLOAD))
        # Minio backup is working
        self.system = StatTargets.backup
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(FILESTORE_AUTH))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(FILESTORE_CONTAINER_OR_BUCKET_EXISTS))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(FILESTORE_CAN_UPLOAD))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(FILESTORE_CAN_DOWNLOAD))

    async def test_all_stages_succeed(self):
        """Run all test stages using minio for storage."""
        await self.stats_collector.run(run_once=True)
        self._assertions()

    async def test_all_stages_alt_succeed(self):
        """Run all test stages using azure for storage."""
        self.settings.minio = False
        self.settings.backup_minio = False
        self.settings.azure_blob = True
        self.settings.backup_azure_blob = True
        self.stats_collector = StatsCollector(self.settings)
        await self.stats_collector.run(run_once=True)
        self._assertions()
