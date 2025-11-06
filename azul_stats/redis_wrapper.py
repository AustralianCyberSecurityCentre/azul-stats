"""Simplified client to enable interacting with redis."""

import logging

import redis

from azul_stats.settings import RedisSettings

logger = logging.getLogger(__name__)


class RedisWrapper:
    """Simplification of handling of connections to and from redis to make status checks easier."""

    def __init__(self, cfg: RedisSettings):
        self.cfg = cfg
        self._client = None

    def connect(self) -> bool:
        """Connect to the redis server and return true if successful."""
        try:
            self._client = redis.Redis(
                host=self.cfg.host,
                port=self.cfg.port,
                db=self.cfg.db,
                password=self.cfg.password,
                username=self.cfg.username,
            )
            # Verify the connection is working with a ping
            self._client.ping()
        except Exception as e:
            logger.warning(f"Unable to connect to redis with error {e}")
            return False
        return True

    def set_key(self) -> bool:
        """Return true if key can be successfully set."""
        if self._client is None:
            return False
        try:
            self._client.set(self.cfg.test_key, self.cfg.test_value)
        except Exception:
            return False
        return True

    def get_key(self):
        """Get the value for the test key from the redis cache."""
        if self._client is None:
            return False
        try:
            return self._client.get(self.cfg.test_key) == self.cfg.test_value.encode()
        except Exception:
            return False

    def delete_key(self):
        """Delete the test key from the redis cache."""
        if self._client is None:
            return False
        try:
            if self._client.exists(self.cfg.test_key):
                self._client.delete(self.cfg.test_key)
        except Exception as e:
            logger.warning(f"Failed to delete redis key with error {e}")
            return False
        return True
