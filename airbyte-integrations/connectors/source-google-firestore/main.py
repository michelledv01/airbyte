#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_google_firestore import SourceGoogleFirestore

if __name__ == "__main__":
    source = SourceGoogleFirestore()
    launch(source, sys.argv[1:])
