from azul_stats.opensearch_wrapper import AsyncSearchWrapperManager, SearchWrapper

from .base_test import BaseTest


class TestOpensearchWrapper(BaseTest):

    async def asyncTearDown(self) -> None:
        """Delete the test index after each test."""
        sw = SearchWrapper()
        async with AsyncSearchWrapperManager(sw):
            await sw.delete_index()
        return super().tearDown()

    async def test_create_and_delete_index(self):
        sw = SearchWrapper()
        async with AsyncSearchWrapperManager(sw):
            await sw.init_index_and_template()
            self.assertTrue(await sw._client.indices.exists(index=sw._opensearch_settings.test_index))
            await sw.delete_index()
            self.assertFalse(await sw._client.indices.exists(index=sw._opensearch_settings.test_index))

    async def test_indexing_and_searching(self):
        """Create an index add documents and then query for them."""
        sw = SearchWrapper()
        async with AsyncSearchWrapperManager(sw):
            # Create index
            await sw.init_index_and_template()
            generated_doc = sw.generate_test_doc()

            # Index documents
            await sw.index_doc(generated_doc)
            self.assertEqual(await sw.index_doc(sw.generate_test_doc()), True)
            self.assertEqual(await sw.index_doc(sw.generate_test_doc()), True)

            # Verify indexed document can be retrieved.
            doc = await sw.get_doc(generated_doc["id"])
            self.assertEqual(1, len(doc["hits"]["hits"]))
            self.assertEqual(doc["hits"]["hits"][0]["_source"], generated_doc)

            # Verify aggregation query works.
            agg = await sw.get_docs_with_aggregation()
            self.assertEqual(3, agg["hits"]["total"]["value"])
            # Will probably be 3 because it's aggregating on event_ints but because they are random
            # If they are all the same the value could be as low as 1.
            self.assertGreaterEqual(len(agg["aggregations"]["event_ints"]["buckets"]), 1)

    async def test_collect_stats(self):
        """Test that collecting stats gets an appropriate json response with the expected keys"""
        sw = SearchWrapper()
        async with AsyncSearchWrapperManager(sw):
            await sw.init_index_and_template()
            culled_health_info, culled_shard_info = await sw.collect_stats()
            health_info_keys = list(culled_health_info.keys())
            health_info_keys.sort()
            self.assertEqual(
                health_info_keys,
                ["active_primary_shards", "active_shards", "number_of_data_nodes", "number_of_nodes", "status"],
            )

            # Ensure there is at least the one created index.
            print(culled_shard_info)
            self.assertGreaterEqual(len(culled_shard_info.get("indices", {}).keys()), 1)

    async def test_collect_no_stats(self):
        """Test that collecting stats gets an appropriate json response with the expected keys"""
        sw = SearchWrapper()
        sw._opensearch_settings.scrape_stats = False
        async with AsyncSearchWrapperManager(sw):
            culled_health_info, culled_shard_info = await sw.collect_stats()
            self.assertIsNone(culled_health_info)
            self.assertIsNone(culled_shard_info)
