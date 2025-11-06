from azul_stats.redis_wrapper import RedisWrapper
from azul_stats.settings import RedisSettings
from tests.integration.base_test import BaseTest


class TestRedisWrapper(BaseTest):

    def setUp(self):
        parent_setup = super().setUp()
        self.cfg = RedisSettings()
        self.client = RedisWrapper(self.cfg)
        return parent_setup

    def test_can_connect_to_redis(self):
        """Check redis client can be created successfully."""
        self.assertTrue(self.client.connect())

    def test_cant_connect_to_redis_hostname(self):
        """Check redis client fails under various conditions"""
        self.cfg.host = "reallybadhostname"
        self.client = RedisWrapper(self.cfg)
        self.assertFalse(self.client.connect())

    def test_cant_connect_to_redis_password(self):
        """Check redis client fails under various conditions"""
        self.cfg.password = "incorrect-password"
        self.client = RedisWrapper(self.cfg)
        self.assertFalse(self.client.connect())

    def test_can_delete_set_and_get(self):
        """Delete and then get/set a redis key."""
        self.assertTrue(self.client.connect())
        # Delete key if present
        self.assertTrue(self.client.delete_key())
        # Get key that isn't there and fail
        self.assertFalse(self.client.get_key())
        # Set key and then verify it's there with a get.
        self.assertTrue(self.client.set_key())
        self.assertTrue(self.client.get_key())
        # Repeat calls to ensure it still works if the old key wasn't cleared first.
        self.assertTrue(self.client.set_key())
        self.assertTrue(self.client.get_key())

        # Delete the key twice to make sure multiple delete calls don't break anything.
        self.assertTrue(self.client.delete_key())
        self.assertTrue(self.client.delete_key())
