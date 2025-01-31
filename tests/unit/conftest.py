from karapace.protobuf.kotlin_wrapper import trim_margin
from karapace.schema_models import SchemaType, ValidatedTypedSchema
from tests.utils import schema_avro_json, schema_protobuf, schema_protobuf2

import pytest


class MockClient:
    def __init__(self, *args, **kwargs):
        pass

    async def get_schema_for_id(self, *args, **kwargs):  # pylint: disable=unused-argument
        return ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json)

    async def get_latest_schema(self, *args, **kwargs):  # pylint: disable=unused-argument
        return 1, ValidatedTypedSchema.parse(SchemaType.AVRO, schema_avro_json)

    async def post_new_schema(self, *args, **kwargs):  # pylint: disable=unused-argument
        return 1


class MockProtobufClient:
    def __init__(self, *args, **kwargs):
        pass

    async def get_schema_for_id2(self, *args, **kwargs):  # pylint: disable=unused-argument
        return ValidatedTypedSchema.parse(SchemaType.PROTOBUF, trim_margin(schema_protobuf2))

    async def get_schema_for_id(self, *args, **kwargs):  # pylint: disable=unused-argument
        if args[0] != 1:
            return None
        return ValidatedTypedSchema.parse(SchemaType.PROTOBUF, trim_margin(schema_protobuf))

    async def get_latest_schema(self, *args, **kwargs):  # pylint: disable=unused-argument
        return 1, ValidatedTypedSchema.parse(SchemaType.PROTOBUF, trim_margin(schema_protobuf))

    async def post_new_schema(self, *args, **kwargs):  # pylint: disable=unused-argument
        return 1


@pytest.fixture(name="mock_registry_client")
def create_basic_registry_client() -> MockClient:
    return MockClient()


@pytest.fixture(name="mock_protobuf_registry_client")
def create_basic_protobuf_registry_client() -> MockProtobufClient:
    return MockProtobufClient()
