"""Collects statistics from azul infrastructure and hosts them to be scraped by prometheus."""

import asyncio
import logging
import time
import traceback
from asyncio import Future
from threading import Thread
from typing import Any, Awaitable, Callable, Coroutine

import opensearchpy
from prometheus_client import Gauge, Info, start_http_server

from azul_stats import settings
from azul_stats.auth_wrapper import AsyncAuthWrapper
from azul_stats.azul_probe import AsyncAzulProbe, AsyncAzulProbeManager
from azul_stats.azure_wrapper import AzureContainerWrapperAsync
from azul_stats.kafka_wrapper import KafkaWrapper
from azul_stats.minio_wrapper import MinioWrapper
from azul_stats.opensearch_wrapper import AsyncSearchWrapperManager, SearchWrapper
from azul_stats.redis_wrapper import RedisWrapper
from azul_stats.settings import (
    AuthSettings,
    AzulProbeSettings,
    AzureBlobSettings,
    BackupAzureBlobSettings,
    BackupMinioSettings,
    MinioSettings,
    RedisSettings,
    StatsSettings,
    StatTargets,
)
from azul_stats.utils import SysExitThread, has_timed_out, random_word

logger = logging.getLogger(__name__)

stats_status = Gauge(
    "status_status",
    "Sets value to 0 if stats gathering is going well and 100 if there is a problem.",
    labelnames=["reason"],
)

azul_health_check_timing = Gauge(
    "azul_health_check_timing", "Time to complete a specific health check", labelnames=["system", "action"]
)
azul_health_check_status = Gauge(
    "azul_health_check_status",
    "label on whether the health check succeeded or failed.",
    labelnames=["system", "action"],
)


opensearch_cluster_shard_size = Gauge(
    "opensearch_shard_size",
    "The shard sizes in an opensearch cluster in bytes.",
    labelnames=["index", "shard", "node"],
)

opensearch_health_info = Info(
    name="opensearch_health_info_summary", documentation="Summary health information about opensearch"
)


SUCCESS_VALUE = 0
FAIL_VALUE = 100

TIMEOUT_LABEL = "timeout"
UNKNOWN_LABEL = "unknown"

OPENSEARCH_AUTH_LABEL = "auth"
OPENSEARCH_CREATE_INDEX_LABEL = "create_index"
OPENSEARCH_INDEX_DOCS_LABEL = "index_docs"
OPENSEARCH_GET_DOC_LABEL = "get_doc"
OPENSEARCH_GET_AGG_LABEL = "get_docs_agg"
OPENSEARCH_COLLECT_STATS_LABEL = "collect_stats"

KAFKA_CONNECT = "connect"
KAFKA_CREATE_TOPIC = "create_topic"
KAFKA_PRODUCE_TOPIC = "produce_topic"
KAFKA_CONSUME_TOPIC = "consume_topic"

FILESTORE_AUTH = "auth"
FILESTORE_CONTAINER_OR_BUCKET_EXISTS = "parent_exists"
FILESTORE_CAN_UPLOAD = "blob_upload"
FILESTORE_CAN_DOWNLOAD = "blob_download"

REDIS_AUTH = "auth"
REDIS_SET_KEY = "set"
REDIS_GET_KEY = "get"

AUTH_GET_TOKEN = "get_token"  # nosec B105
AUTH_VALIDATE_TOKEN = "valid_token"  # nosec B105

AZUL_PROBE_UI = "webui"
AZUL_PROBE_UI_STATIC = "webui_static"
AZUL_PROBE_DOCS = "docs"
AZUL_PROBE_RESTAPI = "restapi"
AZUL_PROBE_RESTAPI_CONFIGJSON = "restapi_config"


class StatsCollector:
    """Collects all of the different stats from infrastructure in azul managed by the settings."""

    def __init__(self, settings: StatsSettings):
        self.s = settings
        self._running_services: list[str] = []
        self._async_stat_scrape_func: list[Callable[[], Future]] = []
        # Function that accepts number of seconds to timeout thread.
        self._thread_stat_scrape_func: list[Callable[[int], Thread]] = []

        # Enable Azul probing
        if self.s.azul:
            self._async_stat_scrape_func.append(self.azul_probe_setup(AzulProbeSettings()))
            self._running_services.append(StatTargets.azul)

        # Enable Opensearch scraper
        if self.s.opensearch:
            self._async_stat_scrape_func.append(self.opensearch)
            self._running_services.append(StatTargets.opensearch)

        # Enable Kafka scraper
        if self.s.kafka:
            self._thread_stat_scrape_func.append(self.kafka)
            self._running_services.append(StatTargets.kafka)

        # Enable Redis scraper
        if self.s.redis:
            self._thread_stat_scrape_func.append(self.redis_setup(RedisSettings()))
            self._running_services.append(StatTargets.redis)

        # Enable Auth scraper
        if self.s.auth:
            self._async_stat_scrape_func.append(self.auth_setup(AuthSettings()))
            self._running_services.append(StatTargets.auth)

        # Enable filestore scraping (azure or minio)
        if self.s.azure_blob:
            self._async_stat_scrape_func.append(self.azure_blob_setup(StatTargets.storage, AzureBlobSettings()))
            self._running_services.append(StatTargets.storage)
        elif self.s.minio:
            self._thread_stat_scrape_func.append(self.minio_setup(StatTargets.storage, MinioSettings()))
            self._running_services.append(StatTargets.storage)

        # Enable backup filestore scraping (minio only)
        if self.s.backup_minio:
            self._thread_stat_scrape_func.append(self.minio_setup(StatTargets.backup, BackupMinioSettings()))
            self._running_services.append(StatTargets.backup)
        elif self.s.backup_azure_blob:
            self._async_stat_scrape_func.append(self.azure_blob_setup(StatTargets.backup, BackupAzureBlobSettings()))
            self._running_services.append(StatTargets.backup)

    @staticmethod
    async def _run_opensearch_stage(
        stage: str, func: Callable[[SearchWrapper, Any], Awaitable[Any]], sw: SearchWrapper, **kwargs
    ):
        """Standard method for running an opensearch stage and collecting stats for it."""
        try:
            with azul_health_check_timing.labels(system=StatTargets.opensearch, action=stage).time():
                await func(sw, **kwargs)
            azul_health_check_status.labels(system=StatTargets.opensearch, action=stage).set(SUCCESS_VALUE)
            azul_health_check_status.labels(system=StatTargets.opensearch, action=OPENSEARCH_AUTH_LABEL).set(
                SUCCESS_VALUE
            )
        except opensearchpy.AuthenticationException:
            # Current stage and auth both fail, due to auth.
            azul_health_check_status.labels(system=StatTargets.opensearch, action=OPENSEARCH_AUTH_LABEL).set(
                FAIL_VALUE
            )
            azul_health_check_status.labels(system=StatTargets.opensearch, action=stage).set(FAIL_VALUE)
            return
        except Exception as e:
            logger.error(
                f"Error occurred while gathering opensearch stats {e} with stack track {traceback.format_exc()}"
            )
            azul_health_check_status.labels(system=StatTargets.opensearch, action=stage).set(FAIL_VALUE)

    @staticmethod
    async def _create_index_stage(sw: SearchWrapper):
        """Create the index and necessary template."""
        await sw.init_index_and_template()

    @staticmethod
    async def _index_docs_stage(sw: SearchWrapper, docs: list[dict]):
        """Index a list of provided opensearch documents."""
        for doc in docs:
            created = await sw.index_doc(doc)
            if not created:
                raise Exception("Failed to index document into index.")

    @staticmethod
    async def _opensearch_get_doc_stage(sw: SearchWrapper, doc: dict):
        """Get a single Opensearch document."""
        result = await sw.get_doc(doc.get("id"))
        if len(result.get("hits", {}).get("hits", [])) != 1:
            raise Exception("Failed to get valid search result.")

    @staticmethod
    async def _get_docs_agg_stage(sw: SearchWrapper):
        """Get opensearch documents via an aggregation."""
        agg_result = await sw.get_docs_with_aggregation()
        if len(agg_result.get("aggregations", {}).get("event_ints", {}).get("buckets", [])) == 0:
            raise Exception("Failed to get valid aggregation result.")

    @staticmethod
    async def _collect_stats(sw: SearchWrapper):
        """Get opensearch stats."""
        try:
            culled_health_info, culled_shard_info = await sw.collect_stats()
            stringy_health_info = dict()
            for k, v in culled_health_info.items():
                stringy_health_info[str(k)] = str(v)
            opensearch_health_info.info(stringy_health_info)

            # Collect the nested shard info:
            for index_name, index_shards in culled_shard_info.get("indices", {}).items():
                for shard_number, shard_info in index_shards.get("shards", {}).items():
                    for primary_or_replica_shard in shard_info:
                        # Find out how many bytes the primary and secondary's are taking up.
                        node_name = primary_or_replica_shard.get("routing", {}).get("node", "")
                        if primary_or_replica_shard.get("routing", {}).get("primary"):
                            node_name = "primary-" + node_name
                        else:
                            node_name = "secondary-" + node_name

                        opensearch_cluster_shard_size.labels(index=index_name, shard=shard_number, node=node_name).set(
                            primary_or_replica_shard.get("store", {}).get("size_in_bytes", 0)
                        )
        except Exception as e:
            logger.warning(f"Failed to collect opensearch stats with error {e}")
            raise

    async def opensearch(self):
        """Collect stats from Opensearch."""
        sw = SearchWrapper()
        async with AsyncSearchWrapperManager(sw):
            test_docs: list[dict] = []
            for _ in range(10):
                test_docs.append(sw.generate_test_doc())

            await self._run_opensearch_stage(OPENSEARCH_CREATE_INDEX_LABEL, self._create_index_stage, sw)
            await self._run_opensearch_stage(OPENSEARCH_INDEX_DOCS_LABEL, self._index_docs_stage, sw, docs=test_docs)
            await self._run_opensearch_stage(
                OPENSEARCH_GET_DOC_LABEL, self._opensearch_get_doc_stage, sw, doc=test_docs[0]
            )
            await self._run_opensearch_stage(OPENSEARCH_GET_AGG_LABEL, self._get_docs_agg_stage, sw)
            await self._run_opensearch_stage(OPENSEARCH_COLLECT_STATS_LABEL, self._collect_stats, sw)
            # Delete the index to ensure it doesn't get too big.
            try:
                await sw.delete_index()
            except Exception as e:
                logger.error(
                    f"Error occurred when deleting opensearch index {e} with stack track {traceback.format_exc()}"
                )

    def azul_probe_setup(self, cfg: AzulProbeSettings) -> Callable[..., Coroutine]:
        """Setup and return a function that can probe Azul and record stats in prometheus about the results."""

        async def azul_probe():
            """Probe Azul and verify it is up and running."""
            system = StatTargets.azul
            prober = AsyncAzulProbe(cfg)
            async with AsyncAzulProbeManager(prober):
                await self._run_async_stage(AZUL_PROBE_UI, system, prober.probe_ui)
                await self._run_async_stage(AZUL_PROBE_UI_STATIC, system, prober.probe_ui_static)
                await self._run_async_stage(AZUL_PROBE_DOCS, system, prober.probe_docs)
                await self._run_async_stage(AZUL_PROBE_RESTAPI, system, prober.probe_restapi)
                await self._run_async_stage(AZUL_PROBE_RESTAPI_CONFIGJSON, system, prober.probe_restapi_openapi_file)

        return azul_probe

    def kafka(self, timeout_sec: float):
        """Collect stats for Kafka."""
        start_time = time.time()
        try:
            with KafkaWrapper() as kw:
                self._run_threaded_stage(KAFKA_CONNECT, StatTargets.kafka, kw.can_connect, start_time, timeout_sec)
                self._run_threaded_stage(
                    KAFKA_CREATE_TOPIC, StatTargets.kafka, kw.create_test_topic, start_time, timeout_sec
                )
                try:
                    kw.set_consumer_offset_latest()
                except Exception as e:
                    logger.warning(f"error when attempting to set the consumer offset to latest {e}.")
                # Create random data to produce and then consume.
                rand_key = random_word(10).encode()
                rand_data = random_word(50).encode()
                self._run_threaded_stage(
                    KAFKA_PRODUCE_TOPIC,
                    StatTargets.kafka,
                    kw.produce_to_topic,
                    start_time,
                    timeout_sec,
                    key=rand_key,
                    data=rand_data,
                )
                self._run_threaded_stage(
                    KAFKA_CONSUME_TOPIC,
                    StatTargets.kafka,
                    kw.consume_from_topic_and_check_has_key_value,
                    start_time,
                    timeout_sec,
                    key=rand_key,
                    data=rand_data,
                )
                try:
                    kw.delete_test_topic()
                except Exception as e:
                    logger.warning(f"Failed to delete kafka topic with error {e} and message {traceback.format_exc()}")
        except TimeoutError:
            logger.warning("A kafka stats thread ran out of time to collect stats.")
            raise
        except Exception as e:
            logger.warning(
                "An unhandled exception occurred at some point when handling kafka stats"
                + f", the exception was {e} and message {traceback.format_exc()}"
            )
            raise

    def minio_setup(self, system: StatTargets, cfg: MinioSettings) -> Callable[[float], None]:
        """Return a function that accepts the seconds until timeout and scrapes minio stats."""

        def _internal_minio(timeout_sec: float):
            """Collect stats for minio."""
            start_time = time.time()
            try:
                mw = MinioWrapper(cfg)
                self._run_threaded_stage(FILESTORE_AUTH, system, mw.auth, start_time, timeout_sec)
                self._run_threaded_stage(
                    FILESTORE_CONTAINER_OR_BUCKET_EXISTS, system, mw.exists, start_time, timeout_sec
                )
                data = random_word(50).encode()
                self._run_threaded_stage(
                    FILESTORE_CAN_UPLOAD, system, mw.upload_data_to_blob, start_time, timeout_sec, data=data
                )
                self._run_threaded_stage(
                    FILESTORE_CAN_DOWNLOAD,
                    system,
                    mw.download_blob_and_verify_data,
                    start_time,
                    timeout_sec,
                    data=data,
                )
                try:
                    mw.delete_blob()
                except Exception as e:
                    logger.warning(
                        f"Failed to delete minio blob on {system} with error {e} and message {traceback.format_exc()}"
                    )
            except TimeoutError:
                logger.warning("A minio stats thread ran out of time to collect stats.")
                raise
            except Exception as e:
                logger.warning(
                    "An unhandled exception occurred at some point when handling minio stats"
                    + f", the exception was {e} and message {traceback.format_exc()}"
                )
                raise

        return _internal_minio

    def redis_setup(self, cfg: RedisSettings) -> Callable[[float], None]:
        """Return a function that accepts the seconds until timeout and scrapes redis stats."""

        def _redis_internal(timeout_sec: float):
            """Threaded scrape of redis server."""
            start_time = time.time()
            try:
                rw = RedisWrapper(cfg)
                self._run_threaded_stage(REDIS_AUTH, StatTargets.redis, rw.connect, start_time, timeout_sec)
                self._run_threaded_stage(REDIS_SET_KEY, StatTargets.redis, rw.connect, start_time, timeout_sec)
                self._run_threaded_stage(REDIS_GET_KEY, StatTargets.redis, rw.connect, start_time, timeout_sec)
                try:
                    rw.delete_key()
                except Exception as e:
                    logger.warning(
                        f"Failed to delete redis key on {StatTargets.redis} with error {e}"
                        + f" and message {traceback.format_exc()}"
                    )
            except TimeoutError:
                logger.warning("A redis stats thread ran out of time to collect stats.")
                raise
            except Exception as e:
                logger.warning(
                    "An unhandled exception occurred at some point when handling redis stats"
                    + f", the exception was {e} and message {traceback.format_exc()}"
                )
                raise

        return _redis_internal

    def azure_blob_setup(self, system: StatTargets, cfg: AzureBlobSettings) -> Callable[..., Coroutine]:
        """Return a function that collects stats from azure blob storage using the provided configuration."""

        async def azure_blob():
            """Collect stats from azure blob storage."""
            client = AzureContainerWrapperAsync(cfg)
            async with client:
                await self._run_async_stage(FILESTORE_AUTH, system, client.auth)
                await self._run_async_stage(FILESTORE_CONTAINER_OR_BUCKET_EXISTS, system, client.container_exists)
                data = random_word(50).encode()
                await self._run_async_stage(FILESTORE_CAN_UPLOAD, system, client.upload_data_to_blob, data=data)
                await self._run_async_stage(
                    FILESTORE_CAN_DOWNLOAD, system, client.download_blob_and_verify_data, data=data
                )
                # Remove old blob.
                try:
                    await client.delete_blob()
                except Exception as e:
                    logger.error(
                        f"Error '{e}' occurred when deleting blob for"
                        + f" {system} with stack track {traceback.format_exc()}"
                    )

        return azure_blob

    def auth_setup(self, cfg: AuthSettings) -> Callable[..., Coroutine]:
        """Return a function that collects stats from auth using the provided configuration."""
        # Initalised here to allow caching of jwks and token_url to make fewer requests to auth server.
        async_client = AsyncAuthWrapper(cfg)

        async def auth():
            """Collect stats from auth."""
            await self._run_async_stage(AUTH_GET_TOKEN, StatTargets.auth, async_client.get_token)
            await self._run_async_stage(AUTH_VALIDATE_TOKEN, StatTargets.auth, async_client.is_token_valid)

        return auth

    def _run_threaded_stage(
        self,
        stage: StatTargets,
        system: str,
        func: Callable[[Any], bool],
        start_time: float,
        timeout_sec: float,
        **kwargs,
    ):
        if has_timed_out(start_time, timeout_sec):
            raise TimeoutError(f"{system} failed to scrape in time and has timed out at stage {stage}")
        try:
            with azul_health_check_timing.labels(system=system, action=stage).time():
                result = func(**kwargs)
                health_value = FAIL_VALUE
                # If the function ran successfully report success value.
                if result:
                    health_value = SUCCESS_VALUE
                azul_health_check_status.labels(system=system, action=stage).set(health_value)
        except Exception as e:
            logger.error(
                f"Error occurred while gathering {system} stats {e} with stack track {traceback.format_exc()}"
            )
            azul_health_check_status.labels(system=system, action=stage).set(FAIL_VALUE)

    @staticmethod
    async def _run_async_stage(
        stage: str,
        system: StatTargets,
        func: Callable[[Any], Awaitable[bool]],
        **kwargs,
    ):
        """Standard method for running an opensearch stage and collecting stats for it."""
        try:
            with azul_health_check_timing.labels(system=system, action=stage).time():
                result: bool = await func(**kwargs)
                value = SUCCESS_VALUE
                if not result:
                    value = FAIL_VALUE
            azul_health_check_status.labels(system=system, action=stage).set(value)
        except Exception as e:
            logger.error(
                f"Error occurred while gathering {system} stats {e} with stack track {traceback.format_exc()}"
            )
            azul_health_check_status.labels(system=system, action=stage).set(FAIL_VALUE)

    async def run(self, run_once=False) -> float:
        """Scrape all the provided services in an async loop.

        Returns an int of how long until next scrape if run_once is enabled.
        """
        start_time = None
        finish_time = None
        while True:
            start_time = time.time()
            clean_exit = True
            try:
                threads: list[SysExitThread] = []
                for thread_func in self._thread_stat_scrape_func:
                    threads.append(SysExitThread(target=thread_func, args=(self.s.max_scrape_time,)))
                    threads[-1].start()

                # Wait for all async tasks to finish
                async with asyncio.timeout(self.s.max_scrape_time):
                    async with asyncio.TaskGroup() as tg:
                        # Start all async tasks.
                        for stat_mapping_func in self._async_stat_scrape_func:
                            tg.create_task(stat_mapping_func())

                for cur_thread in threads:
                    max_wait = self.s.max_scrape_time - (time.time() - start_time)
                    cur_thread.join(max_wait)
                    if cur_thread.is_alive():
                        raise TimeoutError("Thread failed to complete on time.")
            except TimeoutError:
                stats_status.labels(reason=TIMEOUT_LABEL).set(FAIL_VALUE)
                clean_exit = False
                logger.warning(
                    "Services couldn't scrape in time and timed out, "
                    + f"services: {self._running_services}, formatted exception: {traceback.format_exc()}."
                )
                if run_once:
                    raise
            except Exception as e:
                stats_status.labels(reason=UNKNOWN_LABEL).set(FAIL_VALUE)
                clean_exit = False
                logger.error(f"Error when collecting stats {e} with stack track {traceback.format_exc()}")
                if run_once:
                    raise

            if clean_exit:
                stats_status.labels(reason=TIMEOUT_LABEL).set(SUCCESS_VALUE)
                stats_status.labels(reason=UNKNOWN_LABEL).set(SUCCESS_VALUE)

            # Ensure we don't scrape too often and wait until next start.
            finish_time = time.time()
            seconds_to_wait_until_next_scrape = self.s.max_scrape_time - (finish_time - start_time)
            if run_once:
                return seconds_to_wait_until_next_scrape
            if seconds_to_wait_until_next_scrape > 0:
                time.sleep(seconds_to_wait_until_next_scrape)
            else:
                logger.warning(f"No sleep between scrapes, last scrape took {finish_time - start_time}seconds")


def main():
    """Main loop of application."""
    port = settings.StatsSettings().prometheus_port
    if port:
        logger.info(f"started prometheus metrics server started on port {port}")
        start_http_server(port)

    stats_collector = StatsCollector(StatsSettings())
    asyncio.run(stats_collector.run())


if __name__ == "__main__":
    main()
