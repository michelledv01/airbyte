#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import json
from datetime import datetime
from typing import Dict, Generator, Union

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
)
from airbyte_cdk.sources import Source
from google.cloud.firestore_v1.field_path import FieldPath

from source_google_firestore.FirestoreSource import FirestoreSource


class SourceGoogleFirestore(Source):

    def __init__(self):
        self.source_project_id: Union[None, str] = None
        self.credentials: Union[None, str] = None
        self.firestore: Union[None, FirestoreSource] = None
        self.append_sub_collections: Union[None, bool] = None
        self.document_id_field_name: Union[None, str] = None

    def initiate_connections(self, config: json):
        self.credentials = config["credentials_json"]
        self.source_project_id = config["project_id"]
        self.document_id_field_name = config["document_id_field_name"] if "document_id_field_name" in config else "id"
        self.firestore = FirestoreSource(project_id=self.source_project_id, credentials_json=self.credentials)
        self.append_sub_collections = self.enable_append_sub_collections(config)

    @staticmethod
    def enable_append_sub_collections(config: json) -> Union[bool, ValueError]:
        append_sub_collections = config["append_sub_collections"]
        if append_sub_collections == "No":
            return False
        elif append_sub_collections == "Yes":
            return True
        else:
            return ValueError(f"Invalid response for enable sub collection appending.")

    def get_documents_query(self, collection_name: str, document: dict):
        firestore = self.firestore
        base_query = firestore.get_documents(collection_name).limit(1000).order_by(self.document_id_field_name)
        start_after = {f"{self.document_id_field_name}": document[self.document_id_field_name]} if document else None
        if document is not None:
            return base_query.start_after(start_after).stream()
        else:
            return base_query.stream()

    def get_sub_collection_documents(self, collection_name, parent_id):
        if not self.append_sub_collections:
            return {}
        else:
            firestore = self.firestore
            sub_collections_documents = {}

            # Fetch documents from sub-collections
            for sub_collection in firestore.get_sub_collections(collection_name, parent_id):
                sub_collection_name = sub_collection.id
                documents = [child_doc.to_dict() for child_doc in sub_collection.stream()]
                sub_collections_documents[sub_collection_name] = documents

            return sub_collections_documents

    def get_airbyte_data(self, logger: AirbyteLogger, parent_documents: list, collection_name: str):
        airbyte_data = []
        for parent_doc in parent_documents:
            logger.info(f"Fetching sub-collections for {parent_doc[self.document_id_field_name]}")
            # Fetch nested sub-collections for each parent document
            sub_collections_documents = self.get_sub_collection_documents(collection_name, parent_doc[self.document_id_field_name])
            # Combine parent document and sub-collection documents
            data = {"document_id": parent_doc[self.document_id_field_name], "document_content": parent_doc | sub_collections_documents}
            airbyte_data.append(data)
        return airbyte_data

    def fetch_records(self, logger: AirbyteLogger, collection_name: str, start_at=None, data=None) -> list[dict]:
        if data is None:
            data = []
        parent_documents = [doc.to_dict() for doc in self.get_documents_query(collection_name, start_at)]
        for doc in self.get_documents_query(collection_name, start_at):
            print(doc)
        # Fetch sub-collections for each parent document
        data.extend(self.get_airbyte_data(logger, list(parent_documents), collection_name))
        next_start_at = parent_documents[-1] if parent_documents else None

        if next_start_at is not None:
            logger.info(f"Fetching next batch of documents from {collection_name}")
            return self.fetch_records(logger, collection_name, next_start_at, data)
        else:
            return data

    @staticmethod
    def send_airbyte_message(stream_name: str, data: list[dict]):
        for record in data:
            print(f"Sending record: {record}")
            yield AirbyteMessage(
                type=Type.RECORD,
                record=AirbyteRecordMessage(
                    stream=stream_name,
                    data=record,
                    emitted_at=int(datetime.now().timestamp()) * 1000
                )
            )

    def check(self, logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus:
        self.initiate_connections(config)
        logger.info("Connecting to firestore.")
        try:
            self.firestore.check()
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")

    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:
        streams = []
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "document_id": {"type": "string"},
                "document_content": {"type": "object"},
            },
        }

        self.initiate_connections(config)
        logger.info("Connecting to firestore.")

        # Fetch collections from the configured project
        collections = self.firestore.collections()

        for collection in collections:
            stream_name = f"{collection.id}"
            sync_modes = ["full_refresh"]
            streams.append(AirbyteStream(name=stream_name, json_schema=json_schema, supported_sync_modes=sync_modes))

        return AirbyteCatalog(streams=streams)

    def read(
            self, logger: AirbyteLogger, config: json, catalog: ConfiguredAirbyteCatalog, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        self.initiate_connections(config)

        for airbyte_stream in catalog.streams:
            collection_name = airbyte_stream.stream.name
            logger.info(f"Fetching documents from {collection_name}")
            formatted_documents: list[dict] = self.fetch_records(logger, collection_name, None)
            print(formatted_documents)
            logger.info(f"Finished fetching documents from {collection_name}")
            logger.info(f"Streaming documents from {collection_name} to Airbyte.")
            return self.send_airbyte_message(collection_name, formatted_documents)
