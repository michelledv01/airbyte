from datetime import datetime

from airbyte_cdk import AirbyteLogger
from airbyte_protocol.models import ConfiguredAirbyteStream, AirbyteMessage, AirbyteRecordMessage, Type


class AirbyteHelpers:
    def __init__(self, airbyte_stream: ConfiguredAirbyteStream):
        self.airbyte_stream = airbyte_stream
        self.primary_key = airbyte_stream.source_defined_primary_key.pop().pop()
        self.stream_name = airbyte_stream.stream.name

    def format_data(self, data: list[dict]) -> list[dict]:
        formatted_data = []
        for record in data:
            formatted_data.append({"document_id": record[self.primary_key], "document_content": record})
        return data

    def send_airbyte_message(self, data: list[dict]):
        stream_name = self.stream_name
        formatted_data = self.format_data(data)
        for record in formatted_data:
            yield AirbyteMessage(
                type=Type.RECORD,
                record=AirbyteRecordMessage(
                    stream=stream_name,
                    data=record,
                    emitted_at=int(datetime.now().timestamp()) * 1000
                )
            )
