"""Weaviate target sink class, which handles writing streams."""

from __future__ import annotations

import hashlib
import uuid

from singer_sdk.sinks import BatchSink
from singer_sdk.helpers.capabilities import TargetLoadMethods

from target_weaviate.client import WeaviateClient


class WeaviateSink(BatchSink):
    """Weaviate target sink class."""

    max_size = 100

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = None
        self._collection_initialized = False
        self.collection_name = self.config.get("collection_name") or self.stream_name
        
        if self.config.get("batch_size"):
            self.max_size = self.config["batch_size"]

    @property
    def client(self) -> WeaviateClient:
        if not self._client:
            self._client = WeaviateClient(
                weaviate_url=self.config["weaviate_url"],
                weaviate_api_key=self.config.get("weaviate_api_key"),
                logger=self.logger,
            )
        return self._client

    def _ensure_collection_initialized(self, sample_record: dict | None = None):
        if self._collection_initialized:
            return
            
        exists = self.client.collection_exists(self.collection_name)
        
        if exists and self.config.get("load_method") == TargetLoadMethods.OVERWRITE:
            count = self.client.count_objects(self.collection_name)
            if count > 0:
                self.logger.info(
                    f"Deleting collection '{self.collection_name}' "
                    f"because load_method is {TargetLoadMethods.OVERWRITE}"
                )
                self.client.delete_collection(self.collection_name)
                exists = False
            else:
                self.logger.info(
                    f"The load_method is {TargetLoadMethods.OVERWRITE} but the collection is empty, not re-creating."
                )
        
        if not exists:
            if self.config.get("create_collection_if_missing", True):
                properties = None
                if sample_record and self.schema:
                    properties = self._infer_properties_from_schema()
                
                self.client.create_collection(
                    self.collection_name,
                    properties=properties,
                    vectorizer=self.config.get("vectorizer"),
                )
            else:
                raise ValueError(
                    f"Collection '{self.collection_name}' does not exist and "
                    f"create_collection_if_missing is False"
                )
        
        self._collection_initialized = True

    def _infer_properties_from_schema(self) -> list[dict]:
        if not self.schema or "properties" not in self.schema:
            return None
            
        properties = []
        for prop_name, prop_def in self.schema["properties"].items():
            prop_type = prop_def.get("type", [])
            
            if isinstance(prop_type, list):
                prop_type = [t for t in prop_type if t != "null"]
                prop_type = prop_type[0] if prop_type else "string"
            
            if prop_type in ["string", "date-time", "date", "time"]:
                data_type = "TEXT"
            elif prop_type in ["integer", "number"]:
                data_type = "NUMBER"
            elif prop_type == "boolean":
                data_type = "BOOLEAN"
            elif prop_type == "array":
                data_type = "TEXT_ARRAY"
            elif prop_type == "object":
                data_type = "OBJECT"
            else:
                data_type = "TEXT"
            
            properties.append({
                "name": prop_name,
                "data_type": data_type,
            })
        
        return properties

    def process_record(self, record: dict, context: dict) -> None:
        if not self._collection_initialized:
            self._ensure_collection_initialized(sample_record=record)
        
        if "records" not in context:
            context["records"] = []

        if self.config.get("add_record_metadata"):
            record.update(self.config["add_record_metadata"])

        context["records"].append(record)

    def process_batch(self, context: dict) -> None:
        records = context["records"]

        if self.config.get("load_method") == TargetLoadMethods.UPSERT:
            self._batch_upsert(records)
        else:
            self._batch_insert(records)

    def _batch_insert(self, records: list[dict]) -> None:
        self.logger.info(f"Inserting {len(records)} records into '{self.collection_name}'")
        self.client.batch_insert(self.collection_name, records)

    def _batch_upsert(self, records: list[dict]) -> None:
        primary_key = self.config.get("primary_key")
        if not primary_key:
            raise ValueError("primary_key must be specified when load_method is 'upsert'")

        self.logger.info(f"Upserting {len(records)} records into '{self.collection_name}'")
        collection = self.client.get_collection(self.collection_name)
        
        with collection.batch.dynamic() as batch:
            for record in records:
                key_values = {key: record.get(key) for key in primary_key if key in record}

                if len(key_values) != len(primary_key):
                    self.logger.warning(
                        f"Record missing primary key fields. "
                        f"Expected: {primary_key}, Got: {list(key_values.keys())}"
                    )
                    continue

                # generate deterministic uuid from primary key
                key_string = ":".join(str(key_values[k]) for k in sorted(primary_key))
                deterministic_uuid = uuid.UUID(hashlib.md5(key_string.encode()).hexdigest())
                
                batch.add_object(properties=record, uuid=deterministic_uuid)
        
        self.logger.info(f"Batch upsert completed for {len(records)} records")

    def clean_up(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

