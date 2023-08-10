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
        self.collection_name = airbyte_stream.stream.name
        self.primary_key = config.get("primary_key", "id")
        self.cursor_field = config.get("cursor_field", "updated_at")
        self.append_sub_collections = enable_append_sub_collections(config)

    def get_documents_query(self, document: dict, cursor_value):
        firestore = self.firestore
        cursor_field = self.cursor_field
        base_query = firestore.get_documents(self.collection_name).limit(1000)

        if cursor_value:
            start_after = FieldFilter(cursor_field, ">=", DatetimeWithNanoseconds.fromtimestamp(cursor_value["start_at"]))
            end_before = FieldFilter(cursor_field, "<", DatetimeWithNanoseconds.fromtimestamp(cursor_value["end_at"]))
            base_query = base_query.order_by(cursor_field).where(filter=start_after).where(filter=end_before)
        else:
            base_query = base_query.order_by(self.primary_key)

        if document is not None:
            if cursor_value is None:
                start_after = {self.primary_key: document[self.primary_key]} if document else None
                base_query = base_query.start_after(start_after)
            else:
                start_after = {self.primary_key: document[self.primary_key], self.cursor_field: document.get(self.cursor_field, None)}
                base_query = base_query.start_after(start_after)

        return base_query

    def get_sub_collection_documents(self, parent_id):
        firestore = self.firestore
        sub_collections_documents = {}
        # Fetch documents from sub-collections
        for sub_collection in firestore.get_sub_collections(self.collection_name, str(parent_id)):
            sub_collection_name = sub_collection.id
            documents = [child_doc.to_dict() for child_doc in sub_collection.stream()]
            sub_collections_documents[sub_collection_name] = documents

        return sub_collections_documents

    def handle_sub_collections(self, parent_documents: list):
        documents = []
        for parent_doc in parent_documents:
            # Fetch nested sub-collections for each parent document
            sub_collections_documents = self.get_sub_collection_documents(parent_doc[self.primary_key])
            documents.append(parent_doc | sub_collections_documents)
        return documents

    def fetch_records(self, start_at=None, cursor_value=None, data=[]) -> list[dict]:
        logger = self.logger

        base_query = self.get_documents_query(start_at, cursor_value)
        documents = [doc.to_dict() for doc in base_query.stream()]

        if self.append_sub_collections:
            documents = self.handle_sub_collections(documents)

        data.extend(documents)

        next_start_at = documents[-1] if documents else None

        if next_start_at is not None:
            logger.info(f"Fetching next batch of documents. Last document: {next_start_at[self.primary_key]} Total documents: {len(data)}")
            return self.fetch_records(next_start_at, cursor_value, data)
        else:
            return data
