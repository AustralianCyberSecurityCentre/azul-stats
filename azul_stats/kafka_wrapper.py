"""Wrapper classes for producing and consuming to Kafka."""

import logging
import traceback

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import ConfigResource, NewTopic
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

from azul_stats.settings import KafkaSettings

logger = logging.getLogger(__name__)


class KafkaWrapper:
    """Wrapper for kafka client to make it simpler to use."""

    def __init__(self):
        """Create the kafka wrapper ready to execute tasks."""
        self.cfg = KafkaSettings()

    def init(self):
        """Initialise all of the kafka clients (should close after)."""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.cfg.bootstrap_server,
            )
            self.consumer_client = KafkaConsumer(
                self.cfg.test_topic,
                bootstrap_servers=self.cfg.bootstrap_server,
                auto_offset_reset="latest",
            )
            self.producer_client = KafkaProducer(
                bootstrap_servers=self.cfg.bootstrap_server,
                retries=3,
                api_version_auto_timeout_ms=self.cfg.max_timeout_ms,
            )
        except Exception as e:
            logger.warning(f"Could not connect to kafka broker with error {e} and details {traceback.format_exc()}")
            self.admin_client = None
            self.consumer_client = None
            self.producer_client = None

    def __enter__(self):
        """Initialise all of the clients with a context manager."""
        self.init()
        return self

    def __exit__(self, *args, **kwargs):
        """Close all the consumers with the context manager."""
        self.close()

    def can_connect(self) -> bool:
        """Return True if the client can connect to the kafka bootstrap server."""
        # Verify connection to broker was valid.
        if self.admin_client is None:
            return False
        return (
            self.consumer_client.bootstrap_connected() is True and self.producer_client.bootstrap_connected() is True
        )

    def create_test_topic(self) -> bool:
        """Create and configure the test kafka topic."""
        # Verify connection to broker was valid.
        if self.admin_client is None:
            return False
        try:
            topics = self.admin_client.list_topics()
            if self.cfg.test_topic not in topics:
                self.admin_client.create_topics(
                    [NewTopic(name=self.cfg.test_topic, num_partitions=1, replication_factor=1)],
                    timeout_ms=self.cfg.max_timeout_ms,
                )
                self.admin_client.alter_configs(
                    [
                        ConfigResource(
                            resource_type="TOPIC", name=self.cfg.test_topic, configs={"retention.ms": "10000"}
                        )
                    ]
                )
        except TopicAlreadyExistsError:
            # Topic already exists and can't be created so no problem.
            return True
        except Exception as e:
            logger.info(f"Failed to create test topic with error {e}")
            return False
        return True

    def delete_test_topic(self) -> bool:
        """Delete the test kafka topic."""
        # Verify connection to broker was valid.
        if self.admin_client is None:
            return False
        try:
            topics = self.admin_client.list_topics()
            if self.cfg.test_topic in topics:
                self.admin_client.delete_topics([self.cfg.test_topic], timeout_ms=self.cfg.max_timeout_ms)
        except UnknownTopicOrPartitionError:
            # Topic doesn't exist so can't be deleted
            return True
        except Exception as e:
            logger.info(f"Failed to delete test topic with error {e}")
            return False
        return True

    def produce_to_topic(self, key: bytes | str, data: bytes | str) -> bool:
        """Create data in Kafka topic."""
        # Verify connection to broker was valid.
        if self.admin_client is None:
            return False
        if isinstance(data, str):
            data = data.encode()
        if isinstance(key, str):
            key = key.encode()
        try:
            # If topic doesn't exist can't produce to topic
            topics = self.admin_client.list_topics()
            if self.cfg.test_topic not in topics:
                return False
            self.producer_client.send(topic=self.cfg.test_topic, value=data, key=key)
        except Exception as e:
            logger.info(f"Failed to produce to test topic with error {e}")
            return False
        return True

    def set_consumer_offset_latest(self, max_attempts=5) -> bool:
        """Ensure the consumer has partitions and is set to the latest point in the topic."""
        # Verify connection to broker was valid.
        if self.admin_client is None:
            return False
        attempt = 0
        while len(self.consumer_client.assignment()) == 0 and attempt < max_attempts:
            attempt += 1
            self.consumer_client.poll(timeout_ms=500)
        return len(self.consumer_client.assignment()) > 0

    def consume_from_topic_and_check_has_key_value(self, key: bytes | str, data: bytes | str, max_attempts=5) -> bool:
        """Return true if data could be consumed from the test topic."""
        # Verify connection to broker was valid.
        if self.admin_client is None:
            return False
        if isinstance(data, str):
            data = data.encode()
        if isinstance(key, str):
            key = key.encode()

        for _ in range(max_attempts):
            value = self.consumer_client.poll(timeout_ms=500)
            # No value found wait.
            if value == {}:
                continue
            all_records: list[list[ConsumerRecord]] = list(value.values())
            for list_of_records in all_records:
                for c_record in list_of_records:
                    if c_record.key == key and c_record.value == data:
                        return True
        return False

    def close(self):
        """Close all the Kafka clients."""
        if self.admin_client:
            self.admin_client.close()
        if self.consumer_client:
            self.consumer_client.close()
        if self.producer_client:
            self.producer_client.close()
