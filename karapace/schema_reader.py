"""
karapace - Kafka schema reader

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from avro.schema import Schema as AvroSchema, SchemaParseException
from contextlib import closing, ExitStack
from dataclasses import dataclass, field
from enum import Enum, unique
from json import JSONDecodeError
from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import KafkaError, NoBrokersAvailable, NodeNotReadyError, TopicAlreadyExistsError
from kafka.future import Future
from kafka.structs import TopicPartition
from karapace import constants
from karapace.avro_compatibility import parse_avro_schema_definition
from karapace.karapace import (
    create_kafka_admin_from_config, create_kafka_consumer_from_config, create_kafka_producer_from_config
)
from karapace.master_coordinator import MasterCoordinator
from karapace.statsd import StatsClient
from karapace.utils import json_encode, KarapaceKafkaClient
from queue import Queue
from threading import Lock, Thread
from typing import Dict, List, Optional, TYPE_CHECKING

import json
import logging
import time

if TYPE_CHECKING:
    from karapace.compatibility import CompatibilityModes

log = logging.getLogger(__name__)

SubjectName = str
SchemaID = int
SchemaVersion = int

ConsumerPollResult = Dict[TopicPartition, List[ConsumerRecord]]


def parse_jsonschema_definition(schema_definition: str) -> Draft7Validator:
    """Parses and validates `schema_definition`.

    Raises:
        SchemaError: If `schema_definition` is not a valid Draft7 schema.
    """
    schema = json.loads(schema_definition)
    Draft7Validator.check_schema(schema)
    return Draft7Validator(schema)


class InvalidSchema(Exception):
    pass


@unique
class SchemaType(str, Enum):
    AVRO = "AVRO"
    JSONSCHEMA = "JSON"
    PROTOBUF = "PROTOBUF"


@unique
class KeyType(Enum):
    SCHEMA = "SCHEMA"
    CONFIG = "CONFIG"
    DELETE_SUBJECT = "DELETE_SUBJECT"
    NOOP = "NOOP"


class TypedSchema:
    SUPPORTED_TYPES = {SchemaType.AVRO, SchemaType.JSONSCHEMA}

    def __init__(self, schema, schema_type: SchemaType, schema_str: str):
        self.schema_type = schema_type
        self.schema = schema
        self.schema_str = schema_str

    @staticmethod
    def parse_json(schema_str: str):
        try:
            return TypedSchema(parse_jsonschema_definition(schema_str), SchemaType.JSONSCHEMA, schema_str)
        # TypeError - Raised when the user forgets to encode the schema as a string.
        except (TypeError, JSONDecodeError, SchemaError, AssertionError) as e:
            raise InvalidSchema from e

    @staticmethod
    def parse_avro(schema_str: str):  # pylint: disable=inconsistent-return-statements
        try:
            ts = TypedSchema(parse_avro_schema_definition(schema_str), SchemaType.AVRO, schema_str)
            return ts
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

    def __str__(self) -> str:
        return json_encode(self.to_json(), compact=True)

    def __repr__(self):
        return f"TypedSchema(type={self.schema_type}, schema={json_encode(self.to_json())})"

    def __eq__(self, other):
        return (
            isinstance(other, TypedSchema) and self.__str__() == other.__str__() and self.schema_type is other.schema_type
        )


@dataclass
class Schema:
    schema: TypedSchema
    version: SchemaVersion
    global_id: SchemaID
    deleted: bool = False


@dataclass
class Subject:
    name: SubjectName
    versions: Dict[int, Schema] = field(default_factory=dict)
    compatibility: Optional["CompatibilityModes"] = field(default=None)

    def shallow_copy(self) -> "Subject":
        return Subject(self.name, dict(self.versions), self.compatibility)

    def next_version(self) -> SchemaVersion:
        """Returns version for the next schema in the subject.

        Note:
            This class is not thread safe, the caller must make sure `next_version` is not called
            concurrently.
        """
        return max(self.versions) + 1


class KafkaSchemaReader(Thread):
    def __init__(
        self,
        config: dict,
        master_coordinator: Optional[MasterCoordinator] = None,
    ) -> None:
        Thread.__init__(self)
        self.master_coordinator = master_coordinator
        self.log = logging.getLogger("KafkaSchemaReader")
        self.timeout_ms = 200
        self.config = config
        self.offset = 0
        self.admin_client = None
        self.schema_topic = None
        self.topic_replication_factor = self.config["replication_factor"]
        self.consumer = None
        self.queue: "Queue[int]" = Queue()
        self.ready = False
        self.running = True
        sentry_config = config.get("sentry", {"dsn": None}).copy()
        if "tags" not in sentry_config:
            sentry_config["tags"] = {}
        self.stats = StatsClient(sentry_config=sentry_config)

        # This thread will modify these objects in the background, they must not be accessed directly
        # because that may lead to synchronization issues (e.g. `RuntimeError: dictionary changed
        # size during iteration` or data races)
        self._subjectname_to_subjectdata: Dict[SubjectName, Subject] = {}
        self._schemas: Dict[int, TypedSchema] = {}

        # This coarse lock must be acquired before reading or writing to:
        # - _subjectname_to_subjectdata
        # - _schemas
        #
        # This lock will prevent the following data races:
        # - The same subject won't have two schemas registered at the same time, preventing data
        # races with their version numbers
        # - At one time there will be only one schema registered globally. This prevents the same
        # schema from being registered twice concurrently giving it a consistent id.
        #
        # Note:
        # - There are plenty of different race conditions besides the ones mentioned above, that are
        # prevented by having only one consumer processing messages from the schema topic.
        # - This lock must not be acquired outside of this thread! Otherwise there is the risk of
        # deadlocks occuring.
        self._lock = Lock()

    def _init(self) -> None:
        retry_after_secs = 1.0

        with ExitStack() as stack:
            consumer = None
            while consumer is None:
                try:
                    consumer = create_kafka_consumer_from_config(self.config)
                    stack.push(closing(consumer))
                except KafkaError:
                    self.log.exception("Unable to create consumer, retrying")
                    time.sleep(retry_after_secs)

            admin = None
            while admin is None:
                try:
                    admin = create_kafka_admin_from_config(self.config)
                    stack.push(closing(admin))
                except (NodeNotReadyError, NoBrokersAvailable, AssertionError):
                    self.log.warning("No Brokers available yet, retrying _init_admin_client()")
                    time.sleep(retry_after_secs)
                except KafkaError:
                    self.log.exception("Unable to create admin, retrying")
                    time.sleep(retry_after_secs)

            producer = None
            while producer is None:
                try:
                    producer = create_kafka_producer_from_config(self.config)
                    stack.push(closing(producer))
                except KafkaError:
                    self.log.exception("Unable to create producer, retrying")
                    time.sleep(retry_after_secs)

            return producer

    @staticmethod
    def get_new_schema_topic(config):
        return NewTopic(
            name=config["topic_name"],
            num_partitions=constants.SCHEMA_TOPIC_NUM_PARTITIONS,
            replication_factor=config["replication_factor"],
            topic_configs={"cleanup.policy": "compact"},
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
            self.log.exception(
                "Failed to create topic: %r, retrying create_schema_topic()",
                self.config["topic_name"],
            )
            time.sleep(5)
        return False

    def _get_global_schema_id(self, new_schema) -> int:
        assert self._lock.locked(), "_get_global_schema_id must be called with the lock acquired"

        for schema_id, schema in self._schemas.items():
            if schema == new_schema:
                return schema_id

        # XXX: Benchmark this and consider using something else, e.g. heapq
        return max(self._schemas, default=-1) + 1

    def close(self):
        self.log.info("Closing schema_reader")
        self.running = False

    def run(self):
        while self.running:
            try:
                if not self.admin_client:
                    if self._init_admin_client() is False:
                        continue
                if not self.schema_topic:
                    if self.create_schema_topic() is False:
                        continue
                if not self.consumer:
                    self._init_consumer()
                self._handle_messages()
            except Exception as e:  # pylint: disable=broad-except
                if self.stats:
                    self.stats.unexpected_exception(ex=e, where="schema_reader_loop")
                self.log.exception("Unexpected exception in schema reader loop")
        try:
            if self.admin_client:
                self.admin_client.close()
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.close()
        except Exception as e:  # pylint: disable=broad-except
            if self.stats:
                self.stats.unexpected_exception(ex=e, where="schema_reader_exit")
            self.log.exception("Unexpected exception closing schema reader")

    def _handle_messages(self) -> None:
        assert self.consumer, "The thread must be running!"
        raw_msgs: ConsumerPollResult = self.consumer.poll(timeout_ms=self.timeout_ms)
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
                self._handle_message(key, value)
                self.offset = msg.offset
                if self.ready and add_offsets:
                    self.queue.put(self.offset)
        self.log.info("Handled messages, current offset: %r", self.offset)

    def _handle_message(self, key: dict, value: Optional[dict]) -> None:
        if key["keytype"] == KeyType.CONFIG.value:
            if value:
                subject = key.get("subject")
                compatibility_mode = value.get("compatibilityLevel")
                if subject:
                    self._handle_config(subject, compatibility_mode)
            else:
                self.log.error("CONFIG message missing value")
        elif key["keytype"] == KeyType.SCHEMA.value:
            subject_name = key["schema"]

            # XXX: Is it possible for the version to be missing?
            version = key.get("version", default=-1)

            self._handle_schema(subject_name, version, value)
        elif key["keytype"] == KeyType.DELETE_SUBJECT.value:
            if value:
                self._handle_delete_subject(value["subject"], value["version"])
            else:
                self.log.error("DELETE_SUBJECT message missing value")
        elif key["keytype"] == KeyType.NOOP.value:
            pass

    def _handle_config(self, subject_name: Optional[str], compatibility_mode: Optional["CompatibilityModes"]) -> None:
        """Behavior of CONFIG message."""
        assert self._lock.locked(), "_handle_config must be called with the lock acquired"

        if subject_name:
            if compatibility_mode is None:
                self.log.info("Deleting compatibility config completely for subject: %r", subject_name)
                self._subjectname_to_subjectdata[subject_name].compatibility = None

            subject_data = self._subjectname_to_subjectdata.get(subject_name)
            if subject_data is None:
                self.log.info("Adding first version of subject: %r with no schemas", subject_name)
                self._subjectname_to_subjectdata[subject_name] = Subject(
                    name=subject_name,
                    compatibility=compatibility_mode,
                )
            else:
                self.log.info(
                    "Setting subject: %r config to: %r",
                    subject_name,
                    compatibility_mode,
                )
                subject_data.compatibility = compatibility_mode
        else:
            self.log.info("Setting global config to: %r", compatibility_mode)
            self.config["compatibility"] = str(compatibility_mode)

    def _handle_schema(self, subject_name: str, version: int, value: Optional[dict]) -> None:
        subject_data = self._subjectname_to_subjectdata.get(subject_name)

        if value is None:  # permanent delete
            if subject_data is None:
                self.log.error("Subject %s did not exist, should have", subject_name)
            elif version not in subject_data.versions:
                self.log.error(
                    "Version %d for subject %s did not exist, should have",
                    version,
                    subject_name,
                )
            else:
                self.log.info("Deleting subject: %r version: %r completely", subject_name, version)
                subject_data.versions.pop(version, None)
        else:
            schema_type = value.get("schemaType", "AVRO")
            schema_str = value["schema"]
            version = value["version"]
            global_id = value["id"]
            deleted = value.get("deleted", False)
            try:
                typed_schema = TypedSchema.parse(schema_type=SchemaType(schema_type), schema_str=schema_str)
            except InvalidSchema:
                self.log.error("Invalid json: %s", schema_str)
                return

            if subject_data is None:
                self.log.info(
                    "First schema seen for subject %r value: %r schema %r",
                    subject_name,
                    value,
                    typed_schema,
                )
                schema_data = Schema(
                    schema=typed_schema,
                    version=version,
                    global_id=global_id,
                    deleted=deleted,
                )
                subject_data = Subject(
                    name=subject_name,
                    versions={version: schema_data},
                )
                self._subjectname_to_subjectdata[subject_name] = subject_data
                self._schemas[global_id] = typed_schema
            elif deleted is True:
                self.log.info("Deleting subject: %r, version: %r", subject_name, version)
                if version not in subject_data.versions:
                    self._schemas[global_id] = typed_schema
                else:
                    subject_data.versions[version].deleted = True
            else:
                self.log.info("Adding new version of subject: %r, value: %r", subject_name, value)
                subject_data.versions[version] = Schema(
                    schema=typed_schema,
                    version=version,
                    global_id=global_id,
                    deleted=deleted,
                )
                self._schemas[global_id] = typed_schema

    def _handle_delete_subject(self, subject_name: str, version: int) -> None:
        """Behavior of DELETE_SUBJECT message."""
        assert self._lock.locked(), "_handle_delete_subject must be called with the lock acquired"

        subject_data = self._subjectname_to_subjectdata.get(subject_name)
        if subject_data is None:
            self.log.error("Subject: %r did not exist, should have", subject_name)
        else:
            self.log.info("Deleting subject: %r", subject_name)
            for schema_data in subject_data.versions.values():
                if schema_data.version <= version:
                    schema_data.deleted = True

    def _send_kafka_message(self, key: str, value: str) -> Future:
        key_encoded = key.encode("utf8")
        value_encoded = value.encode("utf8")
        future: Future = self.producer.send(self.config["topic_name"], key=key_encoded, value=value_encoded)
        self.producer.flush(timeout=self.kafka_timeout)
        msg = future.get(self.kafka_timeout)
        self.log.debug("Sent kafka msg key: %r, value: %r, offset: %r", key, value, msg.offset)
        self.get_offset_from_queue(msg.offset)
        return future

    def _send_delete_subject_message(self, subject: str, version: int):
        key = '{{"subject":"{}","magic":0,"keytype":"DELETE_SUBJECT"}}'.format(subject)
        value = '{{"subject":"{}","version":{}}}'.format(subject, version)
        return self._send_kafka_message(key, value)

    def _send_schema_message(
        self,
        *,
        subject: str,
        schema: Optional[TypedSchema],
        schema_id: int,
        version: int,
        deleted: bool,
    ):
        key = '{{"subject":"{}","version":{},"magic":1,"keytype":"SCHEMA"}}'.format(subject, version)
        if schema:
            valuedict = {
                "subject": subject,
                "version": version,
                "id": schema_id,
                "schema": schema.schema_str,
                "deleted": deleted,
            }
            if schema.schema_type is not SchemaType.AVRO:
                valuedict["schemaType"] = schema.schema_type
            value = json_encode(valuedict, compact=True)
        else:
            value = ""
        return self.send_kafka_message(key, value)

    def send_config_message(self, compatibility_level: "CompatibilityModes", subject=None):
        if subject is not None:
            key = '{{"subject":"{}","magic":0,"keytype":"CONFIG"}}'.format(subject)
        else:
            key = '{"subject":null,"magic":0,"keytype":"CONFIG"}'
        value = '{{"compatibilityLevel":"{}"}}'.format(compatibility_level.value)
        return self.send_kafka_message(key, value)

    def send_schema_message(
        self,
        *,
        subject: str,
        schema: Optional[TypedSchema],
        schema_id: int,
        version: int,
        deleted: bool,
    ):
        key = '{{"subject":"{}","version":{},"magic":1,"keytype":"SCHEMA"}}'.format(subject, version)
        if schema:
            valuedict = {
                "subject": subject,
                "version": version,
                "id": schema_id,
                "schema": schema.schema_str,
                "deleted": deleted,
            }
            if schema.schema_type is not SchemaType.AVRO:
                valuedict["schemaType"] = schema.schema_type
            value = json_encode(valuedict, compact=True)
        else:
            value = ""
        return self.send_kafka_message(key, value)

    def config_set(self, compatibility_mode: "CompatibilityModes") -> None:
        self.config_subject_set(subject_name=None, compatibility_mode=compatibility_mode)

    def config_subject_set(self, subject_name: Optional[str], compatibility_mode: "CompatibilityModes") -> None:
        with self._lock:
            # First update the kafka topic with the new config
            self.send_config_message(compatibility_level=compatibility_mode, subject=subject_name)

            # Now update our internal state before releasing the lock. The configuration would
            # eventually be applied because of the message above, but meanwhile the compatibility
            # would be invalid.
            self._handle_config(subject_name, compatibility_mode)

    def subject_delete(self, subject_name: str, permanent: bool) -> None:
        with self._lock:
            subject_data = self._subjectname_to_subjectdata[subject_name]

            # First Validate the operation
            if permanent:
                if any(not schema.deleted for schema in subject_data.versions.values()):
                    raise RuntimeError(f"Subject '{subject_name}' was not deleted first before being permanently deleted")

            # Now update the kafka topic with the Delete messages
            if permanent:
                for schema in subject_data.versions.values():
                    self.log.info(
                        "Permanently deleting subject '%s' version %s (schema id=%s)",
                        subject_name,
                        schema.version,
                        schema.global_id,
                    )
                    self.send_schema_message(
                        subject=subject_name,
                        schema=None,
                        schema_id=schema.global_id,
                        version=schema.version,
                        deleted=True,
                    )

            if subject_data.versions:
                latest_schema_version = max(subject_data.versions)
            else:
                latest_schema_version = 0

            self._send_delete_subject_message(subject_name, latest_schema_version)

            # Now update our internal state, eventually this would be updated by the messages sent
            # above. But meanwhile the internal state of the master would be inconsistent, so update
            # it right away.
            if permanent:
                # The effect of handling each of the individual messages above is that the versions
                # dictionary became empty
                subject_data.versions = dict()
            else:
                self._handle_delete_subject(subject_name, latest_schema_version)

    def subject_version_delete(self, subject_name: SubjectName, schema_data: Schema, permanent: bool) -> None:
        with self._lock:
            # First update the kafka topic with the Delete messages
            self.send_schema_message(
                subject=subject_name,
                schema=None if permanent else schema_data.schema,
                schema_id=schema_data.global_id,
                version=schema_data.version,
                deleted=True,
            )

            # Now update our internal state, eventually this would be updated by the messages sent
            # above. But meanwhile the internal state of the master would be inconsistent, and some
            # valid schemas would be rejected because of incompatibilities.
            value = {
                "schemaType": schema_data.schema.schema_type,
                "schema": schema_data.schema.schema,
                "version": schema_data.version,
                "id": schema_data.global_id,
                "deleted": schema_data.deleted,
            }
            self._handle_schema(subject_name, schema_data.version, value)

    def apply_schema_after_validation(
        self,
        subject_name: str,
        new_schema: Optional[TypedSchema],
    ) -> SchemaID:
        """Apply changes to a schema.

        Note:
            This method must only be called after proper validation is performed by the front-end.
            This includes:

            - On register: checking the new schema is not being duplicated for the subject
              (otherwise the existing version should be used)
            - On register: Checking the new schema is not incompatible given the subject's
              configuration (otherwise this would be invalid)
        """
        with self._lock:
            global_id = self._get_global_schema_id(new_schema)
            subject_data = self._subjectname_to_subjectdata[subject_name]
            next_version = subject_data.next_version()
            deleted = new_schema is None

            self.send_schema_message(
                subject=subject_data.name,
                schema=new_schema,
                schema_id=global_id,
                version=next_version,
                deleted=deleted,
            )

            # Now update our internal state, eventually this would be updated by the messages sent
            # above. But meanwhile the internal state of the master would be inconsistent, and it
            # would be possible to register incompatible schemas or reuse the version.
            value = None
            if new_schema:
                value = {
                    "schemaType": new_schema.schema_type,
                    "schema": new_schema.schema,
                    "version": next_version,
                    "id": global_id,
                    "deleted": deleted,
                }
            self._handle_schema(subject_name, next_version, value)

        return global_id

    def shallow_copy_of_subject(self, subject: SubjectName) -> Optional[Subject]:
        # Note:
        # - Because of the AVRO library it is not possible to perform a
        # deepcopy here. So only private attributes of KafkaSchemaReader are
        # copied
        with self._lock:
            subject_data = self._subjectname_to_subjectdata.get(subject)
            if subject_data:
                return subject_data.shallow_copy()
            return subject_data

    def shallow_copy_of_all_subjects(self) -> Dict[SubjectName, Subject]:
        # Note:
        # - Because of the AVRO library it is not possible to perform a
        # deepcopy here. So only private attributes of KafkaSchemaReader are
        # copied
        with self._lock:
            return {
                subject: subject_data.shallow_copy()
                for subject, subject_data in self._subjectname_to_subjectdata.items()
            }

    def get_typed_schema_by_id(self, schema_id: int) -> Optional[TypedSchema]:
        # Acquiring the lock explicitly instead of relying on the interpreter's implementation
        # detail
        with self._lock:
            return self._schemas.get(schema_id)
