import json
from abc import ABC
from datetime import datetime, timedelta
from typing import Mapping, Any

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.sources.streams import IncrementalMixin
from airbyte_protocol.models import ConfiguredAirbyteStream, Type, AirbyteMessage

from source_google_firestore.AirbyteHelpers import AirbyteHelpers
from source_google_firestore.FirestoreSource import FirestoreSource
from source_google_firestore.QueryHelpers import QueryHelpers


class FirestoreIncrementalRefresh(IncrementalMixin, ABC):
    def __init__(self, firestore: FirestoreSource, logger: AirbyteLogger, config: json, airbyte_stream: ConfiguredAirbyteStream):
        self.query_helpers = QueryHelpers(firestore, logger, config, airbyte_stream)
        self.airbyte_stream = airbyte_stream
        self.cursor_field = airbyte_stream.default_cursor_field.pop()
        self.logger = logger
        self._cursor_value = None

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value is not None:
            return {self.cursor_field: self._cursor_value}
        else:
            return {self.cursor_field: ''}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value

    def chuck_time(self, last_updated_at):
        timeframes = []
        last_updated_at = datetime.strptime(last_updated_at, "%Y-%m-%dT%H:%M:%S.%f")
        sync_time = datetime.utcnow()
        while last_updated_at < sync_time:
            timeframes.append({self.cursor_field: last_updated_at})
            last_updated_at += timedelta(minutes=1)
        return timeframes

    def stream(self, state):
        last_updated_at = state[self.cursor_field] if state[self.cursor_field] else None
        timeframes = self.chuck_time(last_updated_at) if last_updated_at else None
        airbyte = AirbyteHelpers(self.logger, self.airbyte_stream)

        if timeframes is None or not timeframes:
            documents = self.query_helpers.fetch_records()
            airbyte.send_airbyte_message(documents)
            self._cursor_value = datetime.utcnow()
        else:
            for timeframe in timeframes:
                documents: list[dict] = self.query_helpers.fetch_records(cursor_value=timeframe[self.cursor_field])
                print(documents)
                airbyte.send_airbyte_message(documents)
                self._cursor_value = timeframe[self.cursor_field]
        yield AirbyteMessage(type=Type.STATE, state=self.state)
