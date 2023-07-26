import json
from typing import Union

from airbyte_cdk import AirbyteLogger
from airbyte_protocol.models import ConfiguredAirbyteStream
from google.cloud.firestore_v1 import FieldFilter
from google.api_core.datetime_helpers import DatetimeWithNanoseconds


from source_google_firestore.FirestoreSource import FirestoreSource


def enable_append_sub_collections(config: json) -> Union[bool, ValueError]:
    append_sub_collections = config["append_sub_collections"]
    if append_sub_collections == "No":
        return False
    elif append_sub_collections == "Yes":
        return True


class QueryHelpers:
    def __init__(self, firestore: FirestoreSource, logger: AirbyteLogger, config: json, airbyte_stream: ConfiguredAirbyteStream):
        self.firestore = firestore
        self.logger = logger
        self.airbyte_stream = airbyte_stream
        self.primary_key = airbyte_stream.primary_key.pop().pop()
        self.cursor_field = airbyte_stream.cursor_field.pop()
        self.append_sub_collections = enable_append_sub_collections(config)

    def get_documents_query(self, collection_name: str, document: dict, cursor_value):
        firestore = self.firestore
        cursor_field = self.cursor_field
        base_query = firestore.get_documents(collection_name).limit(1000)

        if cursor_value:
            start_after = FieldFilter(cursor_field, ">=", DatetimeWithNanoseconds.fromtimestamp(cursor_value["start_at"]))
            end_before = FieldFilter(cursor_field, "<", DatetimeWithNanoseconds.fromtimestamp(cursor_value["end_at"]))
            base_query = base_query.order_by(cursor_field).where(filter=start_after).where(filter=end_before)
        else:
            base_query = base_query.order_by(self.primary_key)

        if document is not None:
            start_after = {self.primary_key: document[self.primary_key]} if document else None
            if start_after:
                base_query = base_query.start_after(start_after)

        return base_query

    def get_sub_collection_documents(self, collection_name, parent_id):
        firestore = self.firestore
        sub_collections_documents = {}
        # Fetch documents from sub-collections
        for sub_collection in firestore.get_sub_collections(collection_name, parent_id):
            sub_collection_name = sub_collection.id
            documents = [child_doc.to_dict() for child_doc in sub_collection.stream()]
            sub_collections_documents[sub_collection_name] = documents

        return sub_collections_documents

    def handle_sub_collections(self, parent_documents: list, collection_name: str):
        if not self.append_sub_collections:
            return parent_documents
        else:
            documents = []
            for parent_doc in parent_documents:
                # Fetch nested sub-collections for each parent document
                sub_collections_documents = self.get_sub_collection_documents(collection_name, parent_doc[self.primary_key])
                documents.append(parent_doc | sub_collections_documents)
            return documents

    def fetch_records(self, start_at=None, data=None, cursor_value=None) -> list[dict]:
        collection_name = self.airbyte_stream.stream.name

        if data is None:
            data = []

        base_query = self.get_documents_query(collection_name, start_at, cursor_value)
        parent_documents = [doc.to_dict() for doc in base_query.stream()]
        data.extend(self.handle_sub_collections(list(parent_documents), collection_name))
        next_start_at = parent_documents[-1] if parent_documents else None

        if next_start_at is not None:
            return self.fetch_records(next_start_at, data)
        else:
            return data
