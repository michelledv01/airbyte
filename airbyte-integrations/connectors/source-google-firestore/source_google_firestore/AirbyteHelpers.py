import json

from datetime import datetime

from airbyte_protocol.models import ConfiguredAirbyteStream, AirbyteMessage, AirbyteRecordMessage, Type


class AirbyteHelpers:
    def __init__(self, airbyte_stream: ConfiguredAirbyteStream, config: json):
        self.primary_key = config.get("primary_key", "id")
        self.stream_name = airbyte_stream.stream.name

    @staticmethod
    def get_timestamp(record: dict):
        if record.get("updated_at"):
            return record.get("updated_at")
        elif record.get("created_at"):
            return record.get("created_at")
        else:
            return None

    def format_data(self, data: list[dict]) -> list[dict]:
        formatted_data = []
        for record in data:
            data = {"document_id": record[self.primary_key], "document_content": record, "updated_at": self.get_timestamp(record)}
            formatted_data.append(data)
        return formatted_data

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
