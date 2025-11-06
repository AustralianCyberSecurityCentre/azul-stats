import os

import opensearchpy

from azul_stats.main import (
    FAIL_VALUE,
    OPENSEARCH_AUTH_LABEL,
    OPENSEARCH_COLLECT_STATS_LABEL,
    OPENSEARCH_CREATE_INDEX_LABEL,
    OPENSEARCH_GET_AGG_LABEL,
    OPENSEARCH_GET_DOC_LABEL,
    OPENSEARCH_INDEX_DOCS_LABEL,
    SUCCESS_VALUE,
    StatsCollector,
    azul_health_check_status,
    azul_health_check_timing,
    opensearch_cluster_shard_size,
    opensearch_health_info,
)
from azul_stats.opensearch_wrapper import AsyncSearchWrapperManager, SearchWrapper
from azul_stats.settings import StatsSettings, StatTargets

from .base_test import BaseTest


class TestOpensearchStatsMethods(BaseTest):
    """Test all the Opensearch metrics stages including the whole opensearch collection stage."""

    @classmethod
    def setUpClass(cls) -> None:
        """Set the system for parent functionality."""
        cls_setup = super().setUpClass()
        cls.system = StatTargets.opensearch
        return cls_setup

    async def asyncSetUp(self) -> None:
        """Delete the test index after each test."""
        sw = SearchWrapper()
        async with AsyncSearchWrapperManager(sw):
            await sw.delete_index()
        self.generated_doc = SearchWrapper.generate_test_doc()
        self.statsSettings = StatsSettings()
        self.statsSettings.opensearch = True
        self.stats_collector = StatsCollector(self.statsSettings)
        setup_result = super().setUp()
        return setup_result

    def _set_all_health_to(self, value: int):
        self.set_health_value(OPENSEARCH_AUTH_LABEL, value)
        self.set_health_value(OPENSEARCH_CREATE_INDEX_LABEL, value)
        self.set_health_value(OPENSEARCH_INDEX_DOCS_LABEL, value)
        self.set_health_value(OPENSEARCH_GET_DOC_LABEL, value)
        self.set_health_value(OPENSEARCH_GET_AGG_LABEL, value)

    async def _create_index_stage_helper(self, sw: SearchWrapper):
        await self.stats_collector._run_opensearch_stage(
            OPENSEARCH_CREATE_INDEX_LABEL, self.stats_collector._create_index_stage, sw
        )
        self.assertTrue(await sw._client.indices.exists(index=sw._opensearch_settings.test_index))

    async def test_create_index_stage(self):
        """Verify that the index stage can successfully create"""
        sw = SearchWrapper()
        async with AsyncSearchWrapperManager(sw):
            await self._create_index_stage_helper(sw)

    async def test_all_stages_success(self):
        """Run all opensearch stages in order (ensuring none raise any exceptions)."""
        sw = SearchWrapper()
        async with AsyncSearchWrapperManager(sw):
            await self.stats_collector._create_index_stage(sw)
            await self.stats_collector._index_docs_stage(sw, docs=[self.generated_doc])
            await self.stats_collector._opensearch_get_doc_stage(sw, doc=self.generated_doc)
            await self.stats_collector._get_docs_agg_stage(sw)

    async def test_all_stages_with_stats(self):
        """Run all opensearch stages in order (ensuring none stats are set correctly)."""
        self._set_all_health_to(FAIL_VALUE)
        await self.stats_collector.run(run_once=True)
        # Ensure status stats are captured
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_AUTH_LABEL))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_CREATE_INDEX_LABEL))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_INDEX_DOCS_LABEL))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_GET_DOC_LABEL))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_GET_AGG_LABEL))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_COLLECT_STATS_LABEL))

        self._set_all_health_to(FAIL_VALUE)
        await self.stats_collector.opensearch()
        # Ensure status stats are captured
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_AUTH_LABEL))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_CREATE_INDEX_LABEL))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_INDEX_DOCS_LABEL))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_GET_DOC_LABEL))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_GET_AGG_LABEL))
        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_COLLECT_STATS_LABEL))
        # Ensure timing stats are captured
        self.assertGreater(
            azul_health_check_timing.labels(system=self.system, action=OPENSEARCH_CREATE_INDEX_LABEL)._value.get(), 0
        )
        self.assertGreater(
            azul_health_check_timing.labels(system=self.system, action=OPENSEARCH_INDEX_DOCS_LABEL)._value.get(), 0
        )
        self.assertGreater(
            azul_health_check_timing.labels(system=self.system, action=OPENSEARCH_GET_DOC_LABEL)._value.get(), 0
        )
        self.assertGreater(
            azul_health_check_timing.labels(system=self.system, action=OPENSEARCH_GET_AGG_LABEL)._value.get(), 0
        )
        self.assertGreater(
            azul_health_check_timing.labels(system=self.system, action=OPENSEARCH_COLLECT_STATS_LABEL)._value.get(), 0
        )

    async def test_indexing_fails(self):
        """Test that indexing fails when auth is bad and records the stat."""
        self.set_bad_env("opensearch_password", "notCorrectOhDear!")
        sw = SearchWrapper()
        async with AsyncSearchWrapperManager(sw):
            self.set_health_value(OPENSEARCH_INDEX_DOCS_LABEL, SUCCESS_VALUE)
            await self.stats_collector._run_opensearch_stage(
                OPENSEARCH_INDEX_DOCS_LABEL, self.stats_collector._index_docs_stage, sw, docs=[self.generated_doc]
            )
            self.assertEqual(FAIL_VALUE, self.get_health_value(OPENSEARCH_INDEX_DOCS_LABEL))

    async def test_get_doc_fails(self):
        """Test that get_doc fails when there is no docs and records the stat."""
        self._set_all_health_to(FAIL_VALUE)
        sw = SearchWrapper()
        async with AsyncSearchWrapperManager(sw):
            self.assertFalse(await sw._client.indices.exists(index=sw._opensearch_settings.test_index))
            self.set_health_value(OPENSEARCH_GET_DOC_LABEL, SUCCESS_VALUE)
            # Fail to get doc because there is no index
            await self.stats_collector._run_opensearch_stage(
                OPENSEARCH_GET_DOC_LABEL, self.stats_collector._opensearch_get_doc_stage, sw, doc=self.generated_doc
            )

            self.assertEqual(FAIL_VALUE, self.get_health_value(OPENSEARCH_GET_DOC_LABEL))
            self.set_health_value(OPENSEARCH_GET_DOC_LABEL, SUCCESS_VALUE)
            self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_GET_DOC_LABEL))

            # Create index and fail again because there is still no documents.
            await self._create_index_stage_helper(sw)
            await self.stats_collector._run_opensearch_stage(
                OPENSEARCH_GET_DOC_LABEL, self.stats_collector._opensearch_get_doc_stage, sw, doc=self.generated_doc
            )
            self.assertEqual(FAIL_VALUE, self.get_health_value(OPENSEARCH_GET_DOC_LABEL))

    async def test_get_docs_agg_fails(self):
        """Test that get_docs_agg fails when there is no docs and records the stat."""
        self._set_all_health_to(FAIL_VALUE)
        sw = SearchWrapper()
        async with AsyncSearchWrapperManager(sw):
            self.set_health_value(OPENSEARCH_GET_AGG_LABEL, SUCCESS_VALUE)
            self.assertFalse(await sw._client.indices.exists(index=sw._opensearch_settings.test_index))
            # Fail to get aggregation because there is no index.
            await self.stats_collector._run_opensearch_stage(
                OPENSEARCH_GET_AGG_LABEL, self.stats_collector._get_docs_agg_stage, sw
            )
            self.assertEqual(FAIL_VALUE, self.get_health_value(OPENSEARCH_GET_AGG_LABEL))
            self.set_health_value(OPENSEARCH_GET_AGG_LABEL, SUCCESS_VALUE)
            self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_GET_AGG_LABEL))

            # Create index and fail again because there are still no documents.
            await self._create_index_stage_helper(sw)
            await self.stats_collector._run_opensearch_stage(
                OPENSEARCH_GET_AGG_LABEL, self.stats_collector._get_docs_agg_stage, sw
            )
            self.assertEqual(FAIL_VALUE, self.get_health_value(OPENSEARCH_GET_AGG_LABEL))

    async def test_fail_auth(self):
        self._set_all_health_to(SUCCESS_VALUE)
        self.set_bad_env("opensearch_password", "notCorrectOhDear!")
        sw = SearchWrapper()
        async with AsyncSearchWrapperManager(sw):
            with self.assertRaises(opensearchpy.AuthenticationException):
                await self.stats_collector._create_index_stage(sw)

        # Re-create to set bad password
        self.stats_collector = StatsCollector(StatsSettings())

        await self.stats_collector.opensearch()
        self.assertEqual(FAIL_VALUE, self.get_health_value(OPENSEARCH_AUTH_LABEL))
        self.assertEqual(FAIL_VALUE, self.get_health_value(OPENSEARCH_CREATE_INDEX_LABEL))
        self.assertEqual(FAIL_VALUE, self.get_health_value(OPENSEARCH_INDEX_DOCS_LABEL))
        self.assertEqual(FAIL_VALUE, self.get_health_value(OPENSEARCH_GET_DOC_LABEL))
        self.assertEqual(FAIL_VALUE, self.get_health_value(OPENSEARCH_GET_AGG_LABEL))
        self.assertEqual(FAIL_VALUE, self.get_health_value(OPENSEARCH_COLLECT_STATS_LABEL))

    async def test_collect_stats(self):
        """Collect Opensearch stats and verify they are in a suitable format."""
        self._set_all_health_to(FAIL_VALUE)
        opensearch_cluster_shard_size.clear()
        opensearch_health_info.clear()
        await self.stats_collector.opensearch()

        # Verify info is set correctly
        at_least_one_sample = False
        for m in opensearch_health_info.collect():
            self.assertEqual(m.name, "opensearch_health_info_summary")
            label_keys = list(m.samples[0].labels.keys())
            label_keys.sort()
            self.assertEqual(
                label_keys,
                ["active_primary_shards", "active_shards", "number_of_data_nodes", "number_of_nodes", "status"],
            )
            at_least_one_sample = True
        self.assertTrue(at_least_one_sample)

        # Verify shard stats have been set.
        at_least_one_shard_size = False
        max_index_size = -1
        for m in opensearch_cluster_shard_size.collect():
            # Verify there is at least the one test index.
            self.assertGreaterEqual(len(m.samples), 1)
            for s in m.samples:
                # Verify all stats into this gauge have the name `opensearch_shard_size`
                self.assertEqual(s.name, "opensearch_shard_size")
                max_index_size = max(max_index_size, s.value)
            at_least_one_shard_size = True

        # Verify there is an index with at least 10 bytes of data.
        self.assertGreater(max_index_size, 10)
        self.assertTrue(at_least_one_shard_size)

        self.assertEqual(SUCCESS_VALUE, self.get_health_value(OPENSEARCH_COLLECT_STATS_LABEL))
