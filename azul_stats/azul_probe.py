"""Prober for checking Azul's various components are up and working."""

import logging

import httpx

from azul_stats.settings import AzulProbeSettings

logger = logging.getLogger(__name__)


class AsyncAzulProbe:
    """Prober for checking Azul's various components are up and working."""

    def __init__(self, cfg: AzulProbeSettings):
        self.cfg = cfg
        self._client = None

    def init_client(self):
        """Initialise the underlying httpx client for communicating with Azul."""
        self._client = httpx.AsyncClient(base_url=self.cfg.url, timeout=self.cfg.timeout_seconds)

    async def aclose(self):
        """Close the underlying httpx client."""
        # No client present so nothing to close.
        if not self._client:
            return
        await self._client.aclose()

    async def _probe_endpoint(self, endpoint: str) -> bool:
        """Probe an endpoint for a 200 status and return true if a 200 was received."""
        # Verify client is initialised
        if not self._client:
            raise Exception("Azul Probe client is not initialised you need to call init_client.")
        resp = await self._client.get(endpoint)

        if resp.is_success:
            return True

        # Attempt to append a "/" to the request and retry the query.
        new_url = str(resp.request.url)
        if resp.is_redirect:
            if new_url.endswith("/"):
                new_url = new_url.rstrip("/")
            else:
                new_url = new_url + "/"
            resp = await self._client.get(new_url)

        if resp.is_success:
            logger.warning(
                f"Prober failed due to redirect original URL was '{resp.request.url}' "
                + f" the issue is a trailing slash, the correct URL is '{new_url}' modify the prober path settings."
            )
        else:
            logger.warning(f"Failed to probe Azul endpoint with '{resp.request.url}' status code {resp.status_code}")
        return False

    # --- WebUI ---

    async def probe_ui(self) -> bool:
        """Probe the Azul WebUI and verify it is up."""
        return await self._probe_endpoint(self.cfg.ui_path)

    async def probe_ui_static(self) -> bool:
        """Probe the Azul WebUI and verify static resources are loading."""
        return await self._probe_endpoint(self.cfg.ui_static_file_path)

    # --- Docs ---

    async def probe_docs(self) -> bool:
        """Probe the Azul Docs and verify it is up."""
        return await self._probe_endpoint(self.cfg.docs_path)

    # --- Restapi ---

    async def probe_restapi(self) -> bool:
        """Probe the Azul Restapi and verify it is up."""
        return await self._probe_endpoint(self.cfg.restapi_path)

    async def probe_restapi_openapi_file(self) -> bool:
        """Probe the Azul Restapi static resources load."""
        return await self._probe_endpoint(self.cfg.restapi_openapi_path)


class AsyncAzulProbeManager:
    """Manage the AzulProbe asynchronously and ensure to close the client when work is done."""

    def __init__(self, s: AsyncAzulProbe):
        """Get the search probe to manage."""
        self.s = s

    async def __aenter__(self):
        """Login to the search wrapper."""
        self.s.init_client()

    async def __aexit__(self, *args, **kwargs):
        """Cleanly close the search wrapper."""
        await self.s.aclose()
