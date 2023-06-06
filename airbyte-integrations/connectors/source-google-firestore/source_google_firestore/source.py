#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import json
from datetime import datetime
from typing import Dict, Generator

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

from source_google_firestore.FirestoreSource import FirestoreSource


class SourceGoogleFirestore(Source):

    def __init__(self):
        self.source_project_id = None
        self.credentials = None
        self.firestore = None
        self.append_sub_collections = None

    def initiate_connections(self, config: json):
        self.credentials = config["credentials_json"]
        self.source_project_id = config["project_id"]
        self.firestore = FirestoreSource(project_id=self.source_project_id, credentials_json=self.credentials)
        self.append_sub_collections = self.enable_append_sub_collections(config)

    def enable_append_sub_collections(self, config: json) -> bool:
        append_sub_collections = config["append_sub_collections"]
        if append_sub_collections == "No": return False
        elif append_sub_collections == "Yes": return True
        else: return ValueError(f"Invalid response for enable sub collection appending.")

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
        logger.info("Connecting to firestore.")
        firestore = self.firestore

        for airbyte_stream in catalog.streams:
            collection_name = airbyte_stream.stream.name
            logger.info(f"Fetching documents from {collection_name}")

            # Fetch parent documents
            parent_documents = firestore.get_documents(collection_name)

            # Fetch nested sub-collections for each parent document
            for parent_doc in parent_documents:
                sub_collections_documents = {}
                sub_collections = None

                if self.append_sub_collections:
                    sub_collections = firestore.get_sub_collections(collection_name, parent_doc.id)

                # Fetch documents from sub-collections
                for sub_collection in sub_collections:
                    sub_collection_name = sub_collection.id
                    sub_collections_documents[sub_collection_name] = []

                    # Append sub-collection documents to list
                    for child_doc in sub_collection.stream():
                        sub_collections_documents[sub_collection_name].append(child_doc.to_dict())

                # Combine parent document and sub-collection documents
                data = {"document_id": parent_doc.id, "document_content": parent_doc.to_dict() | sub_collections_documents }

                yield AirbyteMessage(
                    type=Type.RECORD,
                    record=AirbyteRecordMessage(
                        stream=collection_name,
                        data=data,
                        emitted_at=int(datetime.now().timestamp()) * 1000),
                )
