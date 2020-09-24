"""
karapace - Kafka schema reader

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from avro.schema import parse, Schema as AvroSchema, SchemaParseException
from enum import Enum, unique
from json import JSONDecodeError, loads
from jsonschema.exceptions import SchemaError
from jsonschema.validators import Draft7Validator
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, NodeNotReadyError, TopicAlreadyExistsError
from karapace import constants
from karapace.statsd import StatsClient
from karapace.utils import json_encode
from queue import Queue
from threading import Lock, Thread
from typing import Dict

import json
import logging
import time

log = logging.getLogger(__name__)


class InvalidSchema(Exception):
    pass


@unique
class SchemaType(str, Enum):
    AVRO = "AVRO"
    JSONSCHEMA = "JSON"
    PROTOBUF = "PROTOBUF"


class TypedSchema:
    def __init__(self, schema, schema_type: SchemaType):
        self.schema_type = schema_type
        self.schema = schema

    @staticmethod
    def parse_json(schema_str: str):
        try:
            js = loads(schema_str)
            Draft7Validator.check_schema(js)
            assert "type" in js
            return TypedSchema(Draft7Validator(js), SchemaType.JSONSCHEMA)
        except (JSONDecodeError, SchemaError, AssertionError) as e:
            raise InvalidSchema from e

    @staticmethod
    def parse_avro(schema_str: str):  # pylint: disable=inconsistent-return-statements
        try:
            return TypedSchema(parse(schema_str), SchemaType.AVRO)
        except SchemaParseException as e:
            raise InvalidSchema from e

    @staticmethod
    def parse(schema_type: SchemaType, schema_str: str):  # pylint: disable=inconsistent-return-statements
        if schema_type is SchemaType.AVRO:
            return TypedSchema.parse_avro(schema_str)
        if schema_type is SchemaType.JSONSCHEMA:
            return TypedSchema.parse_json(schema_str)
        raise InvalidSchema(f"Unknown parser {schema_type} for {schema_str}")

    def to_json(self):
        if isinstance(self.schema, Draft7Validator):
            return self.schema.schema
        if isinstance(self.schema, AvroSchema):
            return self.schema.to_json(names=None)
        return self.schema

    def __str__(self):
        return json_encode(self.to_json(), compact=True)

    def __repr__(self):
        return f"TypedSchema(type={self.schema_type}, schema={json_encode(self.to_json())})"

    def __eq__(self, other):
        return isinstance(other, TypedSchema) and self.__str__() == other.__str__() and self.schema_type is other.schema_type


class KafkaSchemaReader(Thread):
    def __init__(self, config, master_coordinator=None):
        Thread.__init__(self)
        self.master_coordinator = master_coordinator
        self.log = logging.getLogger("KafkaSchemaReader")
        self.timeout_ms = 200
        self.config = config
        self.subjects = {}
        self.schemas: Dict[int, TypedSchema] = {}
        self.global_schema_id = 0
        self.offset = 0
        self.admin_client = None
        self.schema_topic = None
        self.topic_replication_factor = self.config["replication_factor"]
        self.consumer = None
        self.queue = Queue()
        self.ready = False
        self.running = True
        self.id_lock = Lock()
        sentry_config = config.get("sentry", {"dsn": None}).copy()
        if "tags" not in sentry_config:
            sentry_config["tags"] = {}
        self.stats = StatsClient(sentry_config=sentry_config)

    def init_consumer(self):
        # Group not set on purpose, all consumers read the same data
        self.consumer = KafkaConsumer(
            self.config["topic_name"],
            enable_auto_commit=False,
            api_version=(1, 0, 0),
            bootstrap_servers=self.config["bootstrap_uri"],
            client_id=self.config["client_id"],
            security_protocol=self.config["security_protocol"],
            ssl_cafile=self.config["ssl_cafile"],
            ssl_certfile=self.config["ssl_certfile"],
            ssl_keyfile=self.config["ssl_keyfile"],
            auto_offset_reset="earliest",
        )

    def init_admin_client(self):
        try:
            self.admin_client = KafkaAdminClient(
                api_version_auto_timeout_ms=constants.API_VERSION_AUTO_TIMEOUT_MS,
                bootstrap_servers=self.config["bootstrap_uri"],
                client_id=self.config["client_id"],
                security_protocol=self.config["security_protocol"],
                ssl_cafile=self.config["ssl_cafile"],
                ssl_certfile=self.config["ssl_certfile"],
                ssl_keyfile=self.config["ssl_keyfile"],
            )
            return True
        except (NodeNotReadyError, NoBrokersAvailable, AssertionError):
            self.log.warning("No Brokers available yet, retrying init_admin_client()")
            time.sleep(2.0)
        except:  # pylint: disable=bare-except
            self.log.exception("Failed to initialize admin client, retrying init_admin_client()")
            time.sleep(2.0)
        return False

    @staticmethod
    def get_new_schema_topic(config):
        return NewTopic(
            name=config["topic_name"],
            num_partitions=constants.SCHEMA_TOPIC_NUM_PARTITIONS,
            replication_factor=config["replication_factor"],
            topic_configs={"cleanup.policy": "compact"}
        )

    def create_schema_topic(self):
        schema_topic = self.get_new_schema_topic(self.config)
        try:
            self.log.info("Creating topic: %r", schema_topic)
            self.admin_client.create_topics([schema_topic], timeout_ms=constants.TOPIC_CREATION_TIMEOUT_MS)
            self.log.info("Topic: %r created successfully", self.config["topic_name"])
            self.schema_topic = schema_topic
            return True
        except TopicAlreadyExistsError:
            self.log.warning("Topic: %r already exists", self.config["topic_name"])
            self.schema_topic = schema_topic
            return True
        except:  # pylint: disable=bare-except
            self.log.exception("Failed to create topic: %r, retrying create_schema_topic()", self.config["topic_name"])
            time.sleep(5)
        return False

    def get_schema_id(self, new_schema):
        with self.id_lock:
            schemas = self.schemas.items()
        for schema_id, schema in schemas:
            if schema == new_schema:
                return schema_id
        with self.id_lock:
            self.global_schema_id += 1
            return self.global_schema_id

    def close(self):
        self.log.info("Closing schema_reader")
        self.running = False

    def run(self):
        while self.running:
            try:
                if not self.admin_client:
                    if self.init_admin_client() is False:
                        continue
                if not self.schema_topic:
                    if self.create_schema_topic() is False:
                        continue
                if not self.consumer:
                    self.init_consumer()
                self.handle_messages()
            except Exception as e:  # pylint: disable=broad-except
                if self.stats:
                    self.stats.unexpected_exception(ex=e, where="schema_reader_loop")
                self.log.exception("Unexpected exception in schema reader loop")
        try:
            if self.admin_client:
                self.admin_client.close()
            if self.consumer:
                self.consumer.close()
        except Exception as e:  # pylint: disable=broad-except
            if self.stats:
                self.stats.unexpected_exception(ex=e, where="schema_reader_exit")
            self.log.exception("Unexpected exception closing schema reader")

    def handle_messages(self):
        raw_msgs = self.consumer.poll(timeout_ms=self.timeout_ms)
        if self.ready is False and raw_msgs == {}:
            self.ready = True
        add_offsets = False
        if self.master_coordinator is not None:
            master, _ = self.master_coordinator.get_master_info()
            # keep old behavior for True. When master is False, then we are a follower, so we should not accept direct
            # writes anyway. When master is None, then this particular node is waiting for a stable value, so any
            # messages off the topic are writes performed by another node
            if master is True:
                add_offsets = True

        for _, msgs in raw_msgs.items():
            for msg in msgs:
                try:
                    key = json.loads(msg.key.decode("utf8"))
                except json.JSONDecodeError:
                    self.log.exception("Invalid JSON in msg.key: %r, value: %r", msg.key, msg.value)
                    continue

                value = None
                if msg.value:
                    try:
                        value = json.loads(msg.value.decode("utf8"))
                    except json.JSONDecodeError:
                        self.log.exception("Invalid JSON in msg.value: %r, key: %r", msg.value, msg.key)
                        continue

                self.log.info("Read new record: key: %r, value: %r, offset: %r", key, value, msg.offset)
                self.handle_msg(key, value)
                self.offset = msg.offset
                self.log.info("Handled message, current offset: %r", self.offset)
                if self.ready and add_offsets:
                    self.queue.put(self.offset)

    def handle_msg(self, key: dict, value: dict):
        if key["keytype"] == "CONFIG":
            if "subject" in key and key["subject"] is not None:
                if not value:
                    self.log.info("Deleting compatibility config completely for subject: %r", key["subject"])
                    self.subjects[key["subject"]].pop("compatibility", None)
                    return
                self.log.info(
                    "Setting subject: %r config to: %r, value: %r", key["subject"], value["compatibilityLevel"], value
                )
                if not key["subject"] in self.subjects:
                    self.log.info("Adding first version of subject: %r with no schemas", key["subject"])
                    self.subjects[key["subject"]] = {"schemas": {}}
                subject_data = self.subjects.get(key["subject"])
                subject_data["compatibility"] = value["compatibilityLevel"]
            else:
                self.log.info("Setting global config to: %r, value: %r", value["compatibilityLevel"], value)
                self.config["compatibility"] = value["compatibilityLevel"]
        elif key["keytype"] == "SCHEMA":
            if not value:
                self.log.info("Deleting subject: %r version: %r completely", key["subject"], key["version"])
                self.subjects[key["subject"]]["schemas"].pop(key["version"], None)
                return
            schema_type = value.get("schemaType", "AVRO")
            schema_str = value["schema"]
            try:
                typed_schema = TypedSchema.parse(schema_type=SchemaType(schema_type), schema_str=schema_str)
            except InvalidSchema:
                try:
                    schema_json = json.loads(schema_str)
                    typed_schema = TypedSchema(schema_type=SchemaType(schema_type), schema=schema_json)
                except JSONDecodeError:
                    self.log.error("Invalid json: %s", value["schema"])
                    return
            self.log.debug("Got typed schema %r", typed_schema)
            subject = value["subject"]
            if subject not in self.subjects:
                self.log.info("Adding first version of subject: %r, value: %r", subject, value)
                self.subjects[subject] = {
                    "schemas": {
                        value["version"]: {
                            "schema": typed_schema,
                            "version": value["version"],
                            "id": value["id"],
                            "deleted": value.get("deleted", False),
                        }
                    }
                }
                self.log.info("Setting schema_id: %r with schema: %r", value["id"], typed_schema)
                self.schemas[value["id"]] = typed_schema
                if value["id"] > self.global_schema_id:  # Not an existing schema
                    self.global_schema_id = value["id"]
            elif value.get("deleted", False) is True:
                self.log.info("Deleting subject: %r, version: %r", subject, value["version"])
                if not value["version"] in self.subjects[subject]["schemas"]:
                    self.log.error(
                        "Subject: %r, version: %r, value: %r did not exist, should have.", subject, value["version"], value
                    )
                else:
                    self.subjects[subject]["schemas"][value["version"]]["deleted"] = True
            elif value.get("deleted", False) is False:
                self.log.info("Adding new version of subject: %r, value: %r", subject, value)
                self.subjects[subject]["schemas"][value["version"]] = {
                    "schema": typed_schema,
                    "version": value["version"],
                    "id": value["id"],
                    "deleted": value.get("deleted", False),
                }
                self.log.info("Setting schema_id: %r with schema: %r", value["id"], value["schema"])
                with self.id_lock:
                    self.schemas[value["id"]] = typed_schema
                if value["id"] > self.global_schema_id:  # Not an existing schema
                    self.global_schema_id = value["id"]
        elif key["keytype"] == "DELETE_SUBJECT":
            self.log.info("Deleting subject: %r, value: %r", value["subject"], value)
            if not value["subject"] in self.subjects:
                self.log.error("Subject: %r did not exist, should have", value["subject"])
            else:
                updated_schemas = {
                    key: self._delete_schema_below_version(schema, value["version"])
                    for key, schema in self.subjects[value["subject"]]["schemas"].items()
                }
                self.subjects[value["subject"]]["schemas"] = updated_schemas
        elif key["keytype"] == "NOOP":  # for spec completeness
            pass

    @staticmethod
    def _delete_schema_below_version(schema, version):
        if schema["version"] <= version:
            schema["deleted"] = True
        return schema

    def get_schemas(self, subject):
        non_deleted_schemas = {
            key: val
            for key, val in self.subjects[subject]["schemas"].items()
            if val.get("deleted", False) is False
        }
        return non_deleted_schemas
