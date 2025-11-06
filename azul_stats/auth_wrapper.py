"""Simplified client to enable interacting with auth."""

import logging

import asyncache
import cachetools
import httpx
from authlib.integrations.httpx_client import AsyncOAuth2Client
from authlib.jose import jwt

from azul_stats.settings import AuthSettings

logger = logging.getLogger(__name__)


class AsyncAuthWrapper:
    """Simplification of handling of connections to and from auth to make status checks easier."""

    def __init__(self, cfg: AuthSettings):
        self.cfg = cfg
        self._client = AsyncOAuth2Client(
            self.cfg.client_id,
            self.cfg.client_secret,
            token_endpoint_auth_method=self.cfg.oauth_method,
            scope=self.cfg.scopes,
        )
        self._httpx_async_client = httpx.AsyncClient(
            mounts={
                "https://": httpx.AsyncHTTPTransport(retries=3),
                "http://": httpx.AsyncHTTPTransport(retries=3),
            },
            timeout=5.0,
        )
        self._access_token = None

    @asyncache.cached(cache=cachetools.TTLCache(maxsize=1, ttl=600))
    async def _get_well_known(self) -> dict:
        """Get auth details from well-known config."""
        try:
            resp = await self._httpx_async_client.get(self.cfg.well_known_endpoint)
            json = resp.json()
        except ValueError as e:
            raise Exception("unable to discover IdP auth server well known details") from e
        return json

    async def _get_token_endpoint(self) -> str:
        """Get the token url endpoint."""
        try:
            json_data = await self._get_well_known()
            return json_data["token_endpoint"]
        except Exception as e:
            logger.error(
                "Couldn't find token endpoint, this often means "
                + f"the well-known endpoint is incorrect. error was {e}"
            )
            raise

    @asyncache.cached(cache=cachetools.TTLCache(maxsize=1, ttl=600))
    async def _get_jwks(self):
        """Get the jwks keys to allow for validation of the jwt token."""
        try:
            jwks_uri = (await self._get_well_known()).get("jwks_uri")
            resp = await self._httpx_async_client.get(jwks_uri)
            return resp.json()
        except Exception as e:
            logger.error(f"Couldn't get jwks due to error {e}")
            raise

    def _is_expected_in_actual(
        self, expected: list[str] | str, actual: list[str] | str | None, fail_msg_prefix=""
    ) -> bool:
        """Verify all the elements in expected are in actual."""
        # If expected is an empty string or empty list everything is always ok.
        if not expected:
            return True

        # Convert all strings to lists
        if isinstance(expected, str):
            expected = [expected]
        if isinstance(actual, str):
            actual = [actual]

        # Ensure if actual is None it still gets an empty list.
        if not actual:
            actual = []

        missing_keys = []
        for e in expected:
            if e not in actual:
                missing_keys.append(e)

        if len(missing_keys) > 0:
            logger.warning(f"{fail_msg_prefix} (missing keys) '{missing_keys}' != '{actual}' (actual)")
            return False
        return True

    async def get_token(self) -> bool:
        """Get a token from the auth server."""
        try:
            token = await self._client.fetch_token(await self._get_token_endpoint())
            access_token = token.get("access_token")
            if not access_token:
                logger.warning("Failed to get access token from token endpoint.")
                return False

            self._access_token = access_token
        except Exception as e:
            logger.warning(f"Unable to connect to auth with error {e}")
            return False
        return True

    async def is_token_valid(self) -> bool:
        """Is the token valid."""
        # No access token so it can't be valid.
        if not self._access_token:
            return False

        jwk_keys = await self._get_jwks()
        claims = jwt.decode(self._access_token, jwk_keys)

        if not self._is_expected_in_actual(
            self.cfg.expected_audience, claims.get("aud"), "Failed to auth because the audience was incorrect"
        ):
            return False
        elif not self._is_expected_in_actual(
            self.cfg.expected_roles, claims.get("roles"), "Failed to verify roles because at least one is missing"
        ):
            return False
        return True
