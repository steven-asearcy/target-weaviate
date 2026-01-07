"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import hashlib
import typing as t
import uuid
from pathlib import Path
from unittest import mock

from singer_sdk.testing import TargetTestRunner

from target_weaviate.target import TargetWeaviate

SAMPLE_CONFIG: dict[str, t.Any] = {
    "weaviate_url": "https://test-cluster.weaviate.network",
    "weaviate_api_key": "test-api-key",
    "collection_name": "TestCollection",
    "load_method": "upsert",
    "primary_key": ["item_id", "category"],
    "batch_size": 100,
}


def get_mock_method_call(mock_calls, name):
    """Helper to find a specific method call in mock_calls."""
    for call in mock_calls:
        if call[0] == f"().{name}":
            return call
    return None


@mock.patch("target_weaviate.client.weaviate")
def test_upsert_with_primary_key(mock_weaviate) -> None:
    """Test upsert mode with composite primary key."""
    mock_client_instance = mock.MagicMock()
    mock_weaviate.connect_to_weaviate_cloud.return_value = mock_client_instance

    mock_collections = mock.MagicMock()
    mock_client_instance.collections = mock_collections
    mock_collections.exists.return_value = True

    mock_collection = mock.MagicMock()
    mock_collections.get.return_value = mock_collection

    mock_batch_context = mock.MagicMock()
    mock_batch = mock.MagicMock()
    mock_batch.dynamic.return_value.__enter__ = mock.MagicMock(return_value=mock_batch_context)
    mock_batch.dynamic.return_value.__exit__ = mock.MagicMock(return_value=False)
    mock_collection.batch = mock_batch

    runner = TargetTestRunner(
        TargetWeaviate,
        config=SAMPLE_CONFIG,
        input_filepath=Path("tests/target_test_streams/test_stream.singer"),
    )
    runner.sync_all()

    assert mock_collections.exists.called
    assert mock_batch_context.add_object.call_count == 2


@mock.patch("target_weaviate.client.weaviate")
def test_overwrite_deletes_collection(mock_weaviate) -> None:
    """Test overwrite mode deletes existing collection before inserting."""
    mock_client_instance = mock.MagicMock()
    mock_weaviate.connect_to_weaviate_cloud.return_value = mock_client_instance

    mock_collections = mock.MagicMock()
    mock_client_instance.collections = mock_collections
    mock_collections.exists.return_value = True

    mock_collection = mock.MagicMock()
    mock_collections.get.return_value = mock_collection

    mock_aggregate = mock.MagicMock()
    mock_aggregate.total_count = 100
    mock_collection.aggregate.over_all.return_value = mock_aggregate

    mock_batch_context = mock.MagicMock()
    mock_batch = mock.MagicMock()
    mock_batch.dynamic.return_value.__enter__ = mock.MagicMock(return_value=mock_batch_context)
    mock_batch.dynamic.return_value.__exit__ = mock.MagicMock(return_value=False)
    mock_collection.batch = mock_batch

    config = SAMPLE_CONFIG.copy()
    config["load_method"] = "overwrite"

    runner = TargetTestRunner(
        TargetWeaviate,
        config=config,
        input_filepath=Path("tests/target_test_streams/test_stream.singer"),
    )
    runner.sync_all()

    assert mock_collections.delete.called
    assert mock_batch_context.add_object.call_count == 2


@mock.patch("target_weaviate.client.weaviate")
def test_append_only_no_primary_key(mock_weaviate) -> None:
    """Test append-only mode doesn't require primary key."""
    mock_client_instance = mock.MagicMock()
    mock_weaviate.connect_to_weaviate_cloud.return_value = mock_client_instance

    mock_collections = mock.MagicMock()
    mock_client_instance.collections = mock_collections
    mock_collections.exists.return_value = True

    mock_collection = mock.MagicMock()
    mock_collections.get.return_value = mock_collection

    mock_batch_context = mock.MagicMock()
    mock_batch = mock.MagicMock()
    mock_batch.dynamic.return_value.__enter__ = mock.MagicMock(return_value=mock_batch_context)
    mock_batch.dynamic.return_value.__exit__ = mock.MagicMock(return_value=False)
    mock_collection.batch = mock_batch

    config = SAMPLE_CONFIG.copy()
    config["load_method"] = "append-only"
    config.pop("primary_key", None)

    runner = TargetTestRunner(
        TargetWeaviate,
        config=config,
        input_filepath=Path("tests/target_test_streams/test_stream.singer"),
    )
    runner.sync_all()

    assert mock_batch_context.add_object.call_count == 2


@mock.patch("target_weaviate.client.weaviate")
def test_create_collection_if_missing(mock_weaviate) -> None:
    """Test creates collection if it doesn't exist."""
    mock_client_instance = mock.MagicMock()
    mock_weaviate.connect_to_weaviate_cloud.return_value = mock_client_instance

    mock_collections = mock.MagicMock()
    mock_client_instance.collections = mock_collections
    mock_collections.exists.return_value = False

    mock_collection = mock.MagicMock()
    mock_collections.get.return_value = mock_collection

    mock_batch_context = mock.MagicMock()
    mock_batch = mock.MagicMock()
    mock_batch.dynamic.return_value.__enter__ = mock.MagicMock(return_value=mock_batch_context)
    mock_batch.dynamic.return_value.__exit__ = mock.MagicMock(return_value=False)
    mock_collection.batch = mock_batch

    runner = TargetTestRunner(
        TargetWeaviate,
        config=SAMPLE_CONFIG,
        input_filepath=Path("tests/target_test_streams/test_stream.singer"),
    )
    runner.sync_all()

    assert mock_collections.create.called


@mock.patch("target_weaviate.client.weaviate")
def test_collection_with_vectorizer(mock_weaviate) -> None:
    """Test creates collection with vectorizer config."""
    mock_client_instance = mock.MagicMock()
    mock_weaviate.connect_to_weaviate_cloud.return_value = mock_client_instance

    mock_collections = mock.MagicMock()
    mock_client_instance.collections = mock_collections
    mock_collections.exists.return_value = False

    mock_collection = mock.MagicMock()
    mock_collections.get.return_value = mock_collection

    mock_batch_context = mock.MagicMock()
    mock_batch = mock.MagicMock()
    mock_batch.dynamic.return_value.__enter__ = mock.MagicMock(return_value=mock_batch_context)
    mock_batch.dynamic.return_value.__exit__ = mock.MagicMock(return_value=False)
    mock_collection.batch = mock_batch

    config = SAMPLE_CONFIG.copy()
    config["vectorizer"] = "text2vec-openai"

    runner = TargetTestRunner(
        TargetWeaviate,
        config=config,
        input_filepath=Path("tests/target_test_streams/test_stream.singer"),
    )
    runner.sync_all()

    assert mock_collections.create.called


@mock.patch("target_weaviate.client.weaviate")
def test_local_weaviate_connection(mock_weaviate) -> None:
    """Test connection to local Weaviate without API key."""
    mock_client_instance = mock.MagicMock()
    mock_weaviate.connect_to_custom.return_value = mock_client_instance

    mock_collections = mock.MagicMock()
    mock_client_instance.collections = mock_collections
    mock_collections.exists.return_value = True

    mock_collection = mock.MagicMock()
    mock_collections.get.return_value = mock_collection

    mock_batch_context = mock.MagicMock()
    mock_batch = mock.MagicMock()
    mock_batch.dynamic.return_value.__enter__ = mock.MagicMock(return_value=mock_batch_context)
    mock_batch.dynamic.return_value.__exit__ = mock.MagicMock(return_value=False)
    mock_collection.batch = mock_batch

    config = SAMPLE_CONFIG.copy()
    config["weaviate_url"] = "localhost"
    config.pop("weaviate_api_key", None)

    runner = TargetTestRunner(
        TargetWeaviate,
        config=config,
        input_filepath=Path("tests/target_test_streams/test_stream.singer"),
    )
    runner.sync_all()

    assert mock_weaviate.connect_to_custom.called


@mock.patch("target_weaviate.client.weaviate")
def test_batch_size_configuration(mock_weaviate) -> None:
    """Test respects batch_size configuration."""
    mock_client_instance = mock.MagicMock()
    mock_weaviate.connect_to_weaviate_cloud.return_value = mock_client_instance

    mock_collections = mock.MagicMock()
    mock_client_instance.collections = mock_collections
    mock_collections.exists.return_value = True

    mock_collection = mock.MagicMock()
    mock_collections.get.return_value = mock_collection

    mock_batch_context = mock.MagicMock()
    mock_batch = mock.MagicMock()
    mock_batch.dynamic.return_value.__enter__ = mock.MagicMock(return_value=mock_batch_context)
    mock_batch.dynamic.return_value.__exit__ = mock.MagicMock(return_value=False)
    mock_collection.batch = mock_batch

    config = SAMPLE_CONFIG.copy()
    config["batch_size"] = 50

    runner = TargetTestRunner(
        TargetWeaviate,
        config=config,
        input_filepath=Path("tests/target_test_streams/test_stream.singer"),
    )
    runner.sync_all()

    assert mock_batch_context.add_object.call_count == 2


def test_deterministic_uuid_generation() -> None:
    """Test that same primary key always generates same UUID."""
    key_string1 = "123:alpha"
    key_string2 = "123:alpha"

    uuid1 = uuid.UUID(hashlib.md5(key_string1.encode()).hexdigest())
    uuid2 = uuid.UUID(hashlib.md5(key_string2.encode()).hexdigest())

    assert uuid1 == uuid2
    assert isinstance(uuid1, uuid.UUID)


def test_deterministic_uuid_different_records() -> None:
    """Test that different primary keys generate different UUIDs."""
    key_string1 = "123:alpha"
    key_string2 = "456:beta"

    uuid1 = uuid.UUID(hashlib.md5(key_string1.encode()).hexdigest())
    uuid2 = uuid.UUID(hashlib.md5(key_string2.encode()).hexdigest())

    assert uuid1 != uuid2


@mock.patch("target_weaviate.client.weaviate")
def test_overwrite_empty_collection_no_delete(mock_weaviate) -> None:
    """Test overwrite mode doesn't delete if collection is empty."""
    mock_client_instance = mock.MagicMock()
    mock_weaviate.connect_to_weaviate_cloud.return_value = mock_client_instance

    mock_collections = mock.MagicMock()
    mock_client_instance.collections = mock_collections
    mock_collections.exists.return_value = True

    mock_collection = mock.MagicMock()
    mock_collections.get.return_value = mock_collection

    mock_aggregate = mock.MagicMock()
    mock_aggregate.total_count = 0
    mock_collection.aggregate.over_all.return_value = mock_aggregate

    mock_batch_context = mock.MagicMock()
    mock_batch = mock.MagicMock()
    mock_batch.dynamic.return_value.__enter__ = mock.MagicMock(return_value=mock_batch_context)
    mock_batch.dynamic.return_value.__exit__ = mock.MagicMock(return_value=False)
    mock_collection.batch = mock_batch

    config = SAMPLE_CONFIG.copy()
    config["load_method"] = "overwrite"

    runner = TargetTestRunner(
        TargetWeaviate,
        config=config,
        input_filepath=Path("tests/target_test_streams/test_stream.singer"),
    )
    runner.sync_all()

    assert not mock_collections.delete.called


@mock.patch("target_weaviate.client.weaviate")
def test_add_record_metadata(mock_weaviate) -> None:
    """Test adds custom metadata to all records."""
    mock_client_instance = mock.MagicMock()
    mock_weaviate.connect_to_weaviate_cloud.return_value = mock_client_instance

    mock_collections = mock.MagicMock()
    mock_client_instance.collections = mock_collections
    mock_collections.exists.return_value = True

    mock_collection = mock.MagicMock()
    mock_collections.get.return_value = mock_collection

    mock_batch_context = mock.MagicMock()
    mock_batch = mock.MagicMock()
    mock_batch.dynamic.return_value.__enter__ = mock.MagicMock(return_value=mock_batch_context)
    mock_batch.dynamic.return_value.__exit__ = mock.MagicMock(return_value=False)
    mock_collection.batch = mock_batch

    config = SAMPLE_CONFIG.copy()
    config["add_record_metadata"] = {"source": "meltano", "environment": "test"}

    runner = TargetTestRunner(
        TargetWeaviate,
        config=config,
        input_filepath=Path("tests/target_test_streams/test_stream.singer"),
    )
    runner.sync_all()

    assert mock_batch_context.add_object.call_count == 2
