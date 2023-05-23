#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
from typing import Any, Optional, Generator
from google.cloud import firestore
from google.cloud.firestore_v1 import DocumentSnapshot
from google.oauth2 import service_account


def to_document(snapshot: DocumentSnapshot) -> Optional[dict[str, Any]]:
    return snapshot.to_dict()


class FirestoreSource:
    def __init__(self, project_id: str, credentials_json: Optional[str] = None):
        connection = {"project": project_id}

        if credentials_json:
            try:
                json_account_info = json.loads(credentials_json, strict=False)
            except ValueError:
                raise ValueError("The 'credentials_json' field must contain a valid JSON document with service account access data.")

            credentials = service_account.Credentials.from_service_account_info(json_account_info)
            connection["credentials"] = credentials

        self.client = firestore.Client(**connection)

    def check(self) -> bool:
        return bool(self.collections())

    def read(self, collection_name: str):
        map(to_document, self.get())

    def collections(self) -> list:
        return list(self.client.collections())

    def get(self, name: str) -> Generator[DocumentSnapshot, Any, None]:
        return self.client.collection(name).stream()
