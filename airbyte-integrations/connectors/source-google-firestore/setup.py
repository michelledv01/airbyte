#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#cd airbyte-integrations/connectors/source


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = ["airbyte-cdk~=0.2", "google-cloud-firestore", "google-auth"]

TEST_REQUIREMENTS = ["pytest~=6.2", "connector-acceptance-test"]

setup(
    name="source_google_firestore",
    description="Source implementation for Google Firestore.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "*.yaml"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
