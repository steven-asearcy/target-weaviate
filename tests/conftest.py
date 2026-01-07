"""Test configuration for target-weaviate."""

import pytest


@pytest.fixture()
def config():
    """Return sample configuration for testing."""
    return {
        "weaviate_url": "https://test-cluster.weaviate.network",
        "weaviate_api_key": "test-api-key",
        "collection_name": "TestCollection",
        "load_method": "append-only",
        "batch_size": 100,
        "create_collection_if_missing": True,
    }


@pytest.fixture()
def upsert_config():
    """Return sample configuration for upsert testing."""
    return {
        "weaviate_url": "https://test-cluster.weaviate.network",
        "weaviate_api_key": "test-api-key",
        "collection_name": "TestCollection",
        "load_method": "upsert",
        "primary_key": ["item_id", "category"],
        "batch_size": 100,
        "create_collection_if_missing": True,
    }


@pytest.fixture()
def overwrite_config():
    """Return sample configuration for overwrite testing."""
    return {
        "weaviate_url": "https://test-cluster.weaviate.network",
        "weaviate_api_key": "test-api-key",
        "collection_name": "TestCollection",
        "load_method": "overwrite",
        "batch_size": 100,
        "create_collection_if_missing": True,
    }


@pytest.fixture()
def local_config():
    """Return sample configuration for local Weaviate testing."""
    return {
        "weaviate_url": "localhost",
        "collection_name": "TestCollection",
        "load_method": "append-only",
        "batch_size": 100,
        "create_collection_if_missing": True,
    }
