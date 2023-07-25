#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import json
from typing import Dict, Generator, Union

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
)

from airbyte_cdk.sources import Source
from airbyte_protocol.models import SyncMode

from source_google_firestore.FirestoreFullRefresh import FirestoreFullRefresh
from source_google_firestore.FirestoreIncrementalRefresh import FirestoreIncrementalRefresh
from source_google_firestore.FirestoreSource import FirestoreSource


class SourceGoogleFirestore(Source):
    def __init__(self):
        self.firestore: Union[None, FirestoreSource] = None

    def initiate_connections(self, config: json):
        self.firestore = FirestoreSource(project_id=config["project_id"], credentials_json=config["credentials_json"])

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
        cursor_field = config.cursor_field if "cursor_field" in config else "updated_at"
        primary_key = config.primary_key if "primary_key" in config else "id"

        self.initiate_connections(config)
        logger.info("Connecting to firestore.")

        # Fetch collections from the configured project
        collections = self.firestore.collections()
        for collection in collections:
            streams.append(
                AirbyteStream(
                    name=f"{collection.id}",
                    json_schema=json_schema,
                    supported_sync_modes=["full_refresh", "incremental"],
                    source_defined_cursor=True,
                    cursor_field=[cursor_field],
                    default_cursor_field=[cursor_field],
                    primary_key=[[primary_key]],
                    default_primary_key=[[primary_key]]
                )
            )

        for stream in streams:
            logger.info(stream)

        return AirbyteCatalog(streams=streams)

    def read(
            self, logger: AirbyteLogger, config: json, catalog: ConfiguredAirbyteCatalog, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        logger.info("Starting sync.")
        self.initiate_connections(config)
        firestore = self.firestore
        logger.info("Connecting to firestore.")

        for airbyte_stream in catalog.streams:
            collection_name = airbyte_stream.stream.name
            logger.info(f"Fetching documents from {collection_name}")
            if airbyte_stream.sync_mode == SyncMode.full_refresh:
                firestore_full_refresh = FirestoreFullRefresh(firestore, logger, config, airbyte_stream)
                return firestore_full_refresh.stream()
            else:
                firestore_incremental_refresh = FirestoreIncrementalRefresh(firestore, logger, config, airbyte_stream)
                return firestore_incremental_refresh.stream(state)
