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

    def initiate_connections(self, config: json):
        self.credentials = config["credentials_json"]
        self.source_project_id = config["project_id"]
        self.firestore = FirestoreSource(project_id=self.source_project_id, credentials_json=self.credentials)

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
        for collection in self.firestore.collections():
            stream_name = f"{collection.id}"
            sync_modes = ["full_refresh"]
            streams.append(AirbyteStream(name=stream_name, json_schema=json_schema, supported_sync_modes=sync_modes))
        return AirbyteCatalog(streams=streams)

    def read(
            self, logger: AirbyteLogger, config: json, catalog: ConfiguredAirbyteCatalog, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        self.initiate_connections(config)
        logger.info("Connecting to firestore.")
        for airbyte_stream in catalog.streams:
            stream_name = airbyte_stream.stream.name
            logger.info(f"Fetching documents from {stream_name}")
            for doc in self.firestore.get(stream_name):
                data = {"document_id": doc.id, "document_content": doc.to_dict(), **doc.to_dict()}
                yield AirbyteMessage(
                    type=Type.RECORD,
                    record=AirbyteRecordMessage(
                        stream=stream_name,
                        data=data,
                        emitted_at=int(datetime.now().timestamp()) * 1000),
                )
