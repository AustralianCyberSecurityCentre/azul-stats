from azul_stats.auth_wrapper import AsyncAuthWrapper
from azul_stats.settings import AuthSettings, OAuthMethodEnum
from tests.integration.base_test import BaseTest


class TestAuthWrapper(BaseTest):
    def setUp(self):
        parent_setup = super().setUp()
        self.cfg = AuthSettings()
        return parent_setup

    async def test_can_connect_to_auth(self):
        """Check the auth wrapper can connect to the auth server."""
        aw = AsyncAuthWrapper(self.cfg)
        # Ensure the expected roles/audience are set to at least a value.
        self.assertGreater(len(self.cfg.expected_audience), 0)
        self.assertGreater(len(self.cfg.expected_roles), 0)

        self.assertTrue(await aw.get_token())
        self.assertTrue(await aw.is_token_valid())

    async def test_can_connect_to_auth_method_2(self):
        """Check the auth wrapper can connect to the auth server with a different method."""
        self.cfg.oauth_method = OAuthMethodEnum.client_secret_post.value
        aw = AsyncAuthWrapper(self.cfg)
        self.assertTrue(await aw.get_token())
        self.assertTrue(await aw.is_token_valid())

    async def test_cant_connect_to_auth_well_known(self):
        """Check auth wrapper fails under various incorrect hostname conditions."""
        self.cfg.well_known_endpoint = "reallybadhostname"
        aw = AsyncAuthWrapper(self.cfg)
        self.assertFalse(await aw.get_token())

    async def test_cant_connect_to_auth_secret(self):
        """Check auth wrapper fails under various incorrect password conditions."""
        self.cfg.client_secret = "not-a-real-secret :("
        aw = AsyncAuthWrapper(self.cfg)
        self.assertFalse(await aw.get_token())

    async def test_cant_connect_to_auth_username(self):
        """Check auth wrapper fails under various incorrect username conditions."""
        self.cfg.client_id = "bad username"
        aw = AsyncAuthWrapper(self.cfg)
        self.assertFalse(await aw.get_token())

    async def test_can_connect_and_succeed_with_no_expectations(self):
        """Test that an empty expected role and audience allows a normal success."""
        self.cfg.expected_audience = ""
        self.cfg.expected_roles = []
        aw = AsyncAuthWrapper(self.cfg)
        self.assertTrue(await aw.get_token())
        self.assertTrue(await aw.is_token_valid())

    async def test_can_connect_but_bad_token(self):
        """Test that a bad expected audience allows a connect to succeed but the token validation to fail."""
        self.cfg.expected_audience = "aud-that-does-not-exist"
        aw = AsyncAuthWrapper(self.cfg)
        self.assertTrue(await aw.get_token())
        self.assertFalse(await aw.is_token_valid())

    async def test_can_connect_but_bad_token(self):
        """Test that a bad expected role allows a connect to succeed but the token validation to fail."""
        self.cfg.expected_roles = ["bad-role"]
        aw = AsyncAuthWrapper(self.cfg)
        self.assertTrue(await aw.get_token())
        self.assertFalse(await aw.is_token_valid())
