"""Weaviate target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_weaviate.sinks import WeaviateSink


class TargetWeaviate(Target):
    """Singer target for Weaviate vector database."""

    name = "target-weaviate"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "weaviate_url",
            th.StringType,
            required=True,
            description="Weaviate instance URL (e.g., https://my-cluster.weaviate.network)",
        ),
        th.Property(
            "weaviate_api_key",
            th.StringType,
            required=False,
            secret=True,
            description="Weaviate API key for authentication. Required for Weaviate Cloud.",
        ),
        th.Property(
            "collection_name",
            th.StringType,
            required=False,
            description="Weaviate collection name. If not provided, uses the stream name.",
        ),
        th.Property(
            "primary_key",
            th.ArrayType(th.StringType),
            required=False,
            description=(
                "List of property names to use as composite primary key for upsert operations. "
                "Required when load_method is 'upsert'. "
                "Example: ['id'] or ['retailer', 'product_id']"
            ),
        ),
        th.Property(
            "batch_size",
            th.IntegerType,
            required=False,
            default=100,
            description="Maximum number of records to write in one batch.",
        ),
        th.Property(
            "add_record_metadata",
            th.ObjectType(),
            required=False,
            description="Additional metadata to add to all records.",
        ),
        th.Property(
            "vectorizer",
            th.StringType,
            required=False,
            description=(
                "Vectorizer to use when creating a new collection. "
                "Examples: 'text2vec-cohere', 'text2vec-openai', 'none'. "
                "Only used if the collection doesn't exist and needs to be created."
            ),
        ),
        th.Property(
            "create_collection_if_missing",
            th.BooleanType,
            required=False,
            default=True,
            description="Automatically create the collection if it doesn't exist.",
        ),
    ).to_dict()

    default_sink_class = WeaviateSink


if __name__ == "__main__":
    TargetWeaviate.cli()

