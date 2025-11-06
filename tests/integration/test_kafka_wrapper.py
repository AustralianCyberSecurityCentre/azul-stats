import time
import unittest

from azul_stats.kafka_wrapper import KafkaWrapper
from azul_stats.settings import KafkaSettings
from azul_stats.utils import random_word
from tests.integration.base_test import BaseTest


class TestKafkaWrapper(BaseTest):

    def setUp(self) -> None:
        self.cfg = KafkaSettings()
        self.kw = KafkaWrapper()
        self.kw.init()
        return super().setUp()

    def tearDown(self) -> None:
        self.kw.close()
        return super().tearDown()

    def _check_test_topic_exists(self, exists: bool = True):
        """Check if a topic exists or doesn't exist with a retry to give kafka time to complete the operation."""
        for _ in range(6):
            topics = self.kw.admin_client.list_topics()
            if exists and self.cfg.test_topic in topics:
                return True
            elif not exists and self.cfg.test_topic not in topics:
                return True
            time.sleep(0.5)

    def test_can_connect(self):
        self.assertTrue(self.kw.can_connect())

    def test_create_and_delete_topic(self):
        # Delete test topic if it exists and verify it doesn't exist
        self.kw.delete_test_topic()
        self.kw.delete_test_topic()
        self.kw.delete_test_topic()
        self.kw.delete_test_topic()
        self.assertTrue(self._check_test_topic_exists(False), "Test topic couldn't be deleted to test creation.")
        # Create test Topic and verify it exists.
        self.assertTrue(self.kw.create_test_topic(), "Initial topic creation failed")
        self.assertTrue(self._check_test_topic_exists(), "Test topic didn't create successfully.")
        # Verify calling create when topic already exists causes no harm
        self.assertTrue(self.kw.create_test_topic(), "2nd topic creation failed")
        self.assertTrue(self.kw.create_test_topic(), "3rd topic creation failed")
        self.assertTrue(self.kw.create_test_topic(), "4th topic creation failed")
        self.assertTrue(self._check_test_topic_exists(), "Test topic didn't create successfully.")
        # Delete test topic verifying delete was successful.
        self.kw.delete_test_topic()
        self.assertTrue(self._check_test_topic_exists(False), "Test topic couldn't be deleted after creation.")

    def test_produce_and_consume_from_topic(self):
        """Verify that when a consumer topic is setup and then data produced data can be found."""
        self.kw.create_test_topic()
        self.assertTrue(self.kw.set_consumer_offset_latest())
        key = random_word(10).encode()
        body = random_word(50).encode()
        self.assertTrue(self.kw.produce_to_topic(key, body))
        key = random_word(10).encode()
        body = random_word(50).encode()
        self.assertTrue(self.kw.produce_to_topic(key, body))
        key = random_word(10).encode()
        body = random_word(50).encode()
        self.assertTrue(self.kw.produce_to_topic(key, body))
        self.assertTrue(self.kw.consume_from_topic_and_check_has_key_value(key, body))

    def test_produce_and_consume_initially_not_found(self):
        """Verify that is the consumer is setup with consume_from_topic it won't find data initially."""
        self.kw.create_test_topic()
        key = random_word(10).encode()
        body = random_word(50).encode()
        # Initially because the offset isn't set on consumer it won't find topic data.
        self.assertTrue(self.kw.produce_to_topic(key, body))
        self.assertFalse(self.kw.consume_from_topic_and_check_has_key_value(key, body))
        # When we produce content to the topic consumer, it will now find the data.
        self.assertTrue(self.kw.produce_to_topic(key, body))
        self.assertTrue(self.kw.consume_from_topic_and_check_has_key_value(key, body))
