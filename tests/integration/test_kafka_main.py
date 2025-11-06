import time

from azul_stats.kafka_wrapper import KafkaWrapper
from azul_stats.main import (
    FAIL_VALUE,
    KAFKA_CONNECT,
    KAFKA_CONSUME_TOPIC,
    KAFKA_CREATE_TOPIC,
    KAFKA_PRODUCE_TOPIC,
    SUCCESS_VALUE,
    TIMEOUT_LABEL,
    StatsCollector,
    azul_health_check_timing,
    stats_status,
)
from azul_stats.settings import StatsSettings, StatTargets
from azul_stats.utils import random_word

from .base_test import BaseTest


class TestKafkaStatsMethod(BaseTest):
    """Test all the Kafka metrics stages including the whole kafka collection stage."""

    def setUp(self) -> None:
        self._init_kafka_wrapper()
        self.system = StatTargets.kafka
        # Alter environment before creating stats collection.
        setup_result = super().setUp()
        self.stats_settings = StatsSettings()
        self.stats_settings.kafka = True
        self.stats_settings.max_scrape_time = 30
        self.stats_collector = StatsCollector(self.stats_settings)
        self.start_time = time.time()
        self.timeout_sec = 10
        self.dummy_key = random_word(10).encode()
        self.dummy_data = random_word(50).encode()
        return setup_result

    def _set_all_health_to(self, value: int):
        self.set_health_value(KAFKA_CONNECT, value)
        self.set_health_value(KAFKA_CREATE_TOPIC, value)
        self.set_health_value(KAFKA_PRODUCE_TOPIC, value)
        self.set_health_value(KAFKA_CONSUME_TOPIC, value)

    def tearDown(self) -> None:
        self.kw.close()
        return super().tearDown()

    def _init_kafka_wrapper(self):
        self.kw = KafkaWrapper()
        self.kw.init()

    async def test_all_stages_succeed(self):
        """Run just the kafka section of the stats tests."""
        self._set_all_health_to(FAIL_VALUE)

        await self.stats_collector.run(run_once=True)
        # Ensure status stats are captured
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(KAFKA_CONNECT))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(KAFKA_CREATE_TOPIC))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(KAFKA_PRODUCE_TOPIC))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(KAFKA_CONSUME_TOPIC))
        # Ensure timing stats are captured
        self.assertGreater(azul_health_check_timing.labels(system=self.system, action=KAFKA_CONNECT)._value.get(), 0)
        self.assertGreater(
            azul_health_check_timing.labels(system=self.system, action=KAFKA_CREATE_TOPIC)._value.get(), 0
        )
        self.assertGreater(
            azul_health_check_timing.labels(system=self.system, action=KAFKA_PRODUCE_TOPIC)._value.get(), 0
        )
        self.assertGreater(
            azul_health_check_timing.labels(system=self.system, action=KAFKA_CONSUME_TOPIC)._value.get(), 0
        )

    def test_all_fail_on_bad_kafka_url(self):
        """Run kafka section of test with bad kafka url."""
        self._set_all_health_to(SUCCESS_VALUE)
        # Not the kafka bootstrap server.
        self.set_bad_env("kafka_bootstrap_server", "localhost:10002")
        self._init_kafka_wrapper()
        self.stats_collector.kafka(20)
        # Ensure status stats are captured
        self.assertEqual(FAIL_VALUE, self.get_health_value(KAFKA_CONNECT))
        self.assertEqual(FAIL_VALUE, self.get_health_value(KAFKA_CREATE_TOPIC))
        self.assertEqual(FAIL_VALUE, self.get_health_value(KAFKA_PRODUCE_TOPIC))
        self.assertEqual(FAIL_VALUE, self.get_health_value(KAFKA_CONSUME_TOPIC))

    async def test_stats_fail_with_timeout(self):
        """Run kafka section of test with bad kafka url."""
        # Timeout running stage
        self._set_all_health_to(SUCCESS_VALUE)
        # Not the kafka bootstrap server.
        with self.assertRaises(TimeoutError):
            self.stats_collector.kafka(0)

        # Timeout running stats collector
        self.set_stats_timeout_value(SUCCESS_VALUE)
        self.stats_settings.max_scrape_time = 0
        stats_collector = StatsCollector(self.stats_settings)
        with self.assertRaises(TimeoutError):
            await stats_collector.run(run_once=True)
        # Ensure status stats are captured
        self.assertEqual(FAIL_VALUE, self.get_stats_timeout_value())

    def test_connect_stage_fails(self):
        """Test that connect fails when bootstrap server is set to an invalid value."""
        self.set_bad_env("kafka_bootstrap_server", "localhost:10002")
        self._init_kafka_wrapper()
        self._set_all_health_to(SUCCESS_VALUE)
        self.stats_collector._run_threaded_stage(
            KAFKA_CONNECT, StatTargets.kafka, self.kw.can_connect, self.start_time, self.timeout_sec
        )
        self.assertEqual(FAIL_VALUE, self.get_health_value(KAFKA_CONNECT))

    def test_create_topic_stage_fails(self):
        """Test that create topic stage fails when bootstrap server is set to an invalid value."""
        self.set_bad_env("kafka_bootstrap_server", "localhost:10002")
        self._init_kafka_wrapper()
        self._set_all_health_to(SUCCESS_VALUE)
        self.stats_collector._run_threaded_stage(
            KAFKA_CREATE_TOPIC, StatTargets.kafka, self.kw.create_test_topic, self.start_time, self.timeout_sec
        )
        self.assertEqual(FAIL_VALUE, self.get_health_value(KAFKA_CREATE_TOPIC))

    def test_produce_topic_stage_fails_bad_topic(self):
        """Test that produce topic stage fails when the topic doesn't exist."""
        # Fail when topic doesn't exist
        self.set_bad_env("kafka_test_topic", "non-existent-kafka-topic-zz")
        self._init_kafka_wrapper()
        self.assertTrue(self.kw.delete_test_topic())
        self._set_all_health_to(SUCCESS_VALUE)
        self.stats_collector._run_threaded_stage(
            KAFKA_PRODUCE_TOPIC,
            StatTargets.kafka,
            self.kw.produce_to_topic,
            self.start_time,
            self.timeout_sec,
            key=self.dummy_key,
            data=self.dummy_data,
        )
        self.assertEqual(FAIL_VALUE, self.get_health_value(KAFKA_PRODUCE_TOPIC))

    def test_produce_topic_stage_fails_bad_server_uri(self):
        """Test that produce topic stage fails when bootstrap server is set to an invalid value."""
        # Fail with bad bootstrap server
        self.set_bad_env("kafka_bootstrap_server", "localhost:10002")
        self._init_kafka_wrapper()
        self._set_all_health_to(SUCCESS_VALUE)
        self.stats_collector._run_threaded_stage(
            KAFKA_PRODUCE_TOPIC,
            StatTargets.kafka,
            self.kw.produce_to_topic,
            self.start_time,
            self.timeout_sec,
            key=self.dummy_key,
            data=self.dummy_data,
        )
        self.assertEqual(FAIL_VALUE, self.get_health_value(KAFKA_PRODUCE_TOPIC))

    def test_consume_topic_stage_fails(self):
        """Test that create topic stage fails when bootstrap server is set to an invalid value."""
        # Topic doesn't exist
        self.assertTrue(self.kw.delete_test_topic())
        self._set_all_health_to(SUCCESS_VALUE)
        self.stats_collector._run_threaded_stage(
            KAFKA_CONSUME_TOPIC,
            StatTargets.kafka,
            self.kw.consume_from_topic_and_check_has_key_value,
            self.start_time,
            self.timeout_sec,
            key=self.dummy_key,
            data=self.dummy_data,
        )
        self.assertEqual(FAIL_VALUE, self.get_health_value(KAFKA_CONSUME_TOPIC))
        # Topic is empty
        self.assertTrue(self.kw.create_test_topic())
        self._set_all_health_to(SUCCESS_VALUE)
        self.stats_collector._run_threaded_stage(
            KAFKA_CONSUME_TOPIC,
            StatTargets.kafka,
            self.kw.consume_from_topic_and_check_has_key_value,
            self.start_time,
            self.timeout_sec,
            key=self.dummy_key,
            data=self.dummy_data,
        )
        self.assertEqual(FAIL_VALUE, self.get_health_value(KAFKA_CONSUME_TOPIC))

        # Test bad bootstrap server.
        self.set_bad_env("kafka_bootstrap_server", "localhost:10002")
        self._init_kafka_wrapper()
        self._set_all_health_to(SUCCESS_VALUE)
        self.stats_collector._run_threaded_stage(
            KAFKA_CONSUME_TOPIC,
            StatTargets.kafka,
            self.kw.consume_from_topic_and_check_has_key_value,
            self.start_time,
            self.timeout_sec,
            key=self.dummy_key,
            data=self.dummy_data,
        )
        self.assertEqual(FAIL_VALUE, self.get_health_value(KAFKA_CONSUME_TOPIC))
