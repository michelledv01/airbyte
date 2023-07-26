import json

from airbyte_cdk import AirbyteLogger
from airbyte_protocol.models import ConfiguredAirbyteStream

from source_google_firestore.AirbyteHelpers import AirbyteHelpers
from source_google_firestore.FirestoreSource import FirestoreSource
from source_google_firestore.QueryHelpers import QueryHelpers


class FirestoreFullRefresh:
    def __init__(self, firestore: FirestoreSource, logger: AirbyteLogger, config: json, airbyte_stream: ConfiguredAirbyteStream):
        self.query_helpers = QueryHelpers(firestore, logger, config, airbyte_stream)
        self.airbyte_stream = airbyte_stream
        self.logger = logger

    def stream(self):
        documents: list[dict] = self.query_helpers.fetch_records()
        airbyte = AirbyteHelpers(self.airbyte_stream)
        return airbyte.send_airbyte_message(documents)
