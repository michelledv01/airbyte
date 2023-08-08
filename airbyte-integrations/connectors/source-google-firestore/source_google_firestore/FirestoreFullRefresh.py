import json

from airbyte_cdk import AirbyteLogger
from airbyte_protocol.models import ConfiguredAirbyteStream

from source_google_firestore.AirbyteHelpers import AirbyteHelpers
from source_google_firestore.FirestoreSource import FirestoreSource
from source_google_firestore.QueryHelpers import QueryHelpers


class FirestoreFullRefresh:
    def __init__(self, firestore: FirestoreSource, logger: AirbyteLogger, config: json, airbyte_stream: ConfiguredAirbyteStream):
        self.query = QueryHelpers(firestore, logger, config, airbyte_stream)
        self.airbyte = AirbyteHelpers(airbyte_stream, config)
        self.logger = logger

    def stream(self):
        documents: list[dict] = self.query.fetch_records()
        self.logger.info(f"Finished fetching documents. Total documents: {len(documents)}")
        return self.airbyte.send_airbyte_message(documents)
