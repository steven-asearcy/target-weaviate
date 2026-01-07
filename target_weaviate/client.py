"""Weaviate client wrapper."""

from __future__ import annotations

import weaviate
from weaviate.classes.config import Configure, Property, DataType


class WeaviateClient:
    """Wrapper for Weaviate client operations."""

    def __init__(
        self,
        weaviate_url: str,
        weaviate_api_key: str | None = None,
        logger=None,
    ):
        self.logger = logger
        self.weaviate_url = weaviate_url
        self.weaviate_api_key = weaviate_api_key
        self._client = None

    def connect(self) -> weaviate.WeaviateClient:
        if self._client:
            return self._client

        if self.weaviate_api_key:
            self._client = weaviate.connect_to_weaviate_cloud(
                cluster_url=self.weaviate_url,
                auth_credentials=weaviate.auth.AuthApiKey(self.weaviate_api_key),
            )
        else:
            self._client = weaviate.connect_to_custom(
                http_host=self.weaviate_url,
                http_port=80,
                http_secure=False,
                grpc_host=self.weaviate_url,
                grpc_port=50051,
                grpc_secure=False,
            )

        if self.logger:
            self.logger.info(f"Connected to Weaviate at {self.weaviate_url}")

        return self._client

    def close(self):
        if self._client:
            self._client.close()
            self._client = None

    def collection_exists(self, collection_name: str) -> bool:
        client = self.connect()
        return client.collections.exists(collection_name)

    def create_collection(
        self,
        collection_name: str,
        properties: list[dict] | None = None,
        vectorizer: str | None = None,
    ):
        client = self.connect()

        if self.logger:
            self.logger.info(f"Creating collection '{collection_name}'...")

        vectorizer_config = None
        if vectorizer:
            if vectorizer == "text2vec-cohere":
                vectorizer_config = Configure.Vectorizer.text2vec_cohere()
            elif vectorizer == "text2vec-openai":
                vectorizer_config = Configure.Vectorizer.text2vec_openai()
            elif vectorizer == "none":
                vectorizer_config = Configure.Vectorizer.none()

        property_objects = []
        if properties:
            for prop in properties:
                property_objects.append(
                    Property(
                        name=prop["name"],
                        data_type=DataType[prop.get("data_type", "TEXT").upper()],
                    )
                )

        if property_objects and vectorizer_config:
            client.collections.create(
                name=collection_name,
                properties=property_objects,
                vectorizer_config=vectorizer_config,
            )
        elif property_objects:
            client.collections.create(name=collection_name, properties=property_objects)
        elif vectorizer_config:
            client.collections.create(name=collection_name, vectorizer_config=vectorizer_config)
        else:
            client.collections.create(name=collection_name)

        if self.logger:
            self.logger.info(f"Collection '{collection_name}' created successfully")

    def delete_collection(self, collection_name: str):
        client = self.connect()
        if self.logger:
            self.logger.info(f"Deleting collection '{collection_name}'...")
        client.collections.delete(collection_name)

    def get_collection(self, collection_name: str):
        client = self.connect()
        return client.collections.get(collection_name)

    def batch_insert(self, collection_name: str, records: list[dict]):
        collection = self.get_collection(collection_name)
        with collection.batch.dynamic() as batch:
            for record in records:
                batch.add_object(properties=record)

    def count_objects(self, collection_name: str) -> int:
        collection = self.get_collection(collection_name)
        aggregate = collection.aggregate.over_all(total_count=True)
        return aggregate.total_count

