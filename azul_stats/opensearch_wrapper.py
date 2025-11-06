"""Wrapper for Opensearch client to make it simpler to use after authenticating to the Opensearch server."""

import datetime

from opensearchpy import AsyncOpenSearch

from azul_stats.settings import OpensearchSettings
from azul_stats.utils import random_int, random_word


class SearchWrapper:
    """Wrapper for Opensearch."""

    def __init__(self):
        """Configure Opensearch settings and login, recording how long it takes."""
        self._opensearch_settings = OpensearchSettings()
        # Setup index settings and mappings.
        self._template_settings = {
            "index.mapping.total_fields.limit": 2000,
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "refresh_interval": "30s",
            "analysis": {
                "analyzer": {
                    "path": {"tokenizer": "hierarchy"},
                    "path_reversed": {"tokenizer": "hierarchy_reversed"},
                    "pathw": {"tokenizer": "hierarchyw"},
                    "pathw_reversed": {"tokenizer": "hierarchyw_reversed"},
                    "alphanumeric": {"tokenizer": "alphanumeric"},
                },
                "tokenizer": {
                    "hierarchy": {"type": "path_hierarchy", "delimiter": "/"},
                    "hierarchy_reversed": {"type": "path_hierarchy", "delimiter": "/", "reverse": "true"},
                    "hierarchyw": {"type": "path_hierarchy", "delimiter": "\\"},
                    "hierarchyw_reversed": {"type": "path_hierarchy", "delimiter": "\\", "reverse": "true"},
                    "alphanumeric": {
                        "type": "char_group",
                        "tokenize_on_chars": ["whitespace", "punctuation", "symbol"],
                    },
                },
            },
        }

        self.index_mapping = {
            "dynamic": "strict",
            "properties": {
                "id": {"type": "keyword"},
                "event_name": {"type": "keyword"},
                "event_int": {"type": "integer"},
                "timestamp": {"type": "date"},
            },
        }

    def init_client(self):
        """Initalise the Opensearch client, doesn't actually verify the client is valid and can contact Opensearch."""
        # Username and password
        if self._opensearch_settings.username and self._opensearch_settings.password:
            access = {"http_auth": (self._opensearch_settings.username, self._opensearch_settings.password)}
        else:
            access = {"Authorization": self._opensearch_settings.jwt}

        client = AsyncOpenSearch(
            hosts=[self._opensearch_settings.host],
            http_compress=True,  # enables gzip compression for request bodies
            verify_certs=self._opensearch_settings.certificate_verification,
            ssl_show_warn=self._opensearch_settings.certificate_verification,
            timeout=60,
            **access,
        )
        self._client = client

    async def init_index_and_template(self):
        """Create an index for all test documents to be inserted into."""
        template = {
            "settings": self._template_settings,
            "aliases": {self._opensearch_settings.test_alias: {}},
            "mappings": self.index_mapping,
            "index_patterns": [self._opensearch_settings.test_index, self._opensearch_settings.test_index_pattern],
            # "order": 1,
            # "version": 1,
        }
        # update template first, so existing indices can be manually fixed/rebuilt off of template if needed
        await self._client.indices.put_template(name=self._opensearch_settings.test_alias, body=template, ignore=404)
        await self._client.indices.put_mapping(
            body=template["mappings"], index=self._opensearch_settings.test_index, ignore=404
        )
        await self._client.indices.put_mapping(
            body=template["mappings"], index=self._opensearch_settings.test_index_pattern + ".*", ignore=404
        )

        if not await self._client.indices.exists(index=self._opensearch_settings.test_index):
            await self._client.indices.create(index=self._opensearch_settings.test_index)

    async def delete_index(self):
        """Delete the test index to ensure it doesn't get too big."""
        if await self._client.indices.exists(index=self._opensearch_settings.test_index):
            await self._client.indices.delete(index=self._opensearch_settings.test_index)

    @staticmethod
    def generate_test_doc() -> dict:
        """Generate a test document with randomised values."""
        return {
            "id": random_word(10),
            "event_name": random_word(10),
            "event_int": random_int(1),
            "timestamp": datetime.datetime.now().isoformat(),
        }

    async def index_doc(self, doc_to_index: dict) -> bool:
        """Index the provided document and return true if it was successful."""
        # Refresh set to true to ensure the doc is available for searching.
        index_result = await self._client.index(
            index=self._opensearch_settings.test_index, body=doc_to_index, id=doc_to_index["id"], refresh=True
        )
        return index_result.get("result", "").lower() == "created"

    async def get_doc(self, id: str) -> dict:
        """Make an Opensearch query to get and retrieve a document."""
        body = {"query": {"term": {"id": id}}}
        resp = await self._client.search(index=self._opensearch_settings.test_index, body=body, ignore=404)
        return resp

    async def get_docs_with_aggregation(self) -> dict:
        """Make an aggregation query to Opensearch to aggregate a number of docs."""
        body = {
            "size": 0,
            "aggs": {
                "event_ints": {
                    "terms": {"field": "event_name", "size": 10},
                    "aggs": {"max_value": {"max": {"field": "event_int"}}},
                },
            },
        }
        agg_result = await self._client.search(index=self._opensearch_settings.test_index, body=body, ignore=404)
        return agg_result

    async def collect_stats(self) -> tuple[dict | None, dict | None]:
        """Collect stats from opensearch and host them in prometheus stats for scraping."""
        if self._opensearch_settings.scrape_stats:
            culled_health_info = await self._client.cluster.health(
                filter_path="status,number_of_nodes,number_of_data_nodes,active_primary_shards,active_shards,"
            )
            culled_shard_info = await self._client.indices.stats(
                index=self._opensearch_settings.scrape_index_pattern,
                level="shards",
                filter_path="indices.*.shards.*.routing.primary,"
                "indices.*.shards.*.routing.node,indices.*.shards.*.store.size_in_bytes",
            )
            return culled_health_info, culled_shard_info
        return None, None


class AsyncSearchWrapperManager:
    """Manage the OpensearchWrapper asynchronously and ensure to close the client when work is done."""

    def __init__(self, s: SearchWrapper):
        """Get the search wrapper to manage."""
        self.s = s

    async def __aenter__(self):
        """Login to the search wrapper."""
        self.s.init_client()

    async def __aexit__(self, *args, **kwargs):
        """Cleanly close the search wrapper."""
        await self.s._client.close()
