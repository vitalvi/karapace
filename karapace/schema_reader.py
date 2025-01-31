"""
karapace - Kafka schema reader

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from contextlib import closing, ExitStack
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaConfigurationError, NoBrokersAvailable, NodeNotReadyError, TopicAlreadyExistsError
from karapace import constants
from karapace.config import Config
from karapace.master_coordinator import MasterCoordinator
from karapace.schema_models import SchemaType, TypedSchema
from karapace.statsd import StatsClient
from karapace.utils import KarapaceKafkaClient
from threading import Event, Lock, Thread
from typing import Any, Dict, Optional

import json
import logging

Offset = int
Subject = str
Version = int
Schema = Dict[str, Any]
# Container type for a subject, with configuration settings and all the schemas
SubjectData = Dict[str, Any]
SchemaId = int

# The value `0` is a valid offset and it represents the first message produced
# to a topic, therefore it can not be used.
OFFSET_EMPTY = -1
LOG = logging.getLogger(__name__)

KAFKA_CLIENT_CREATION_TIMEOUT_SECONDS = 2.0
SCHEMA_TOPIC_CREATION_TIMEOUT_SECONDS = 5.0


def _create_consumer_from_config(config: Config) -> KafkaConsumer:
    # Group not set on purpose, all consumers read the same data
    session_timeout_ms = config["session_timeout_ms"]
    request_timeout_ms = max(session_timeout_ms, KafkaConsumer.DEFAULT_CONFIG["request_timeout_ms"])
    return KafkaConsumer(
        config["topic_name"],
        enable_auto_commit=False,
        api_version=(1, 0, 0),
        bootstrap_servers=config["bootstrap_uri"],
        client_id=config["client_id"],
        security_protocol=config["security_protocol"],
        ssl_cafile=config["ssl_cafile"],
        ssl_certfile=config["ssl_certfile"],
        ssl_keyfile=config["ssl_keyfile"],
        sasl_mechanism=config["sasl_mechanism"],
        sasl_plain_username=config["sasl_plain_username"],
        sasl_plain_password=config["sasl_plain_password"],
        auto_offset_reset="earliest",
        session_timeout_ms=session_timeout_ms,
        request_timeout_ms=request_timeout_ms,
        kafka_client=KarapaceKafkaClient,
        metadata_max_age_ms=config["metadata_max_age_ms"],
    )


def _create_admin_client_from_config(config: Config) -> KafkaAdminClient:
    return KafkaAdminClient(
        api_version_auto_timeout_ms=constants.API_VERSION_AUTO_TIMEOUT_MS,
        bootstrap_servers=config["bootstrap_uri"],
        client_id=config["client_id"],
        security_protocol=config["security_protocol"],
        ssl_cafile=config["ssl_cafile"],
        ssl_certfile=config["ssl_certfile"],
        ssl_keyfile=config["ssl_keyfile"],
        sasl_mechanism=config["sasl_mechanism"],
        sasl_plain_username=config["sasl_plain_username"],
        sasl_plain_password=config["sasl_plain_password"],
    )


def new_schema_topic_from_config(config: Config) -> NewTopic:
    return NewTopic(
        name=config["topic_name"],
        num_partitions=constants.SCHEMA_TOPIC_NUM_PARTITIONS,
        replication_factor=config["replication_factor"],
        topic_configs={"cleanup.policy": "compact"},
    )


class OffsetsWatcher:
    """Synchronization container for threads to wait until an offset is seen.

    This works under the assumption offsets are used only once, which should be
    correct as long as no unclean leader election is performed.
    """

    def __init__(self) -> None:
        # Lock used to protected _events, any modifications to that object must
        # be performed with this lock acquired
        self._events_lock = Lock()
        self._events: Dict[Offset, Event] = dict()

    def offset_seen(self, new_offset: int) -> None:
        with self._events_lock:
            # Note: The reader thread could be already waiting on the offset,
            # in that case the existing object must be used, otherwise a new
            # one is created.
            event = self._events.setdefault(new_offset, Event())
            event.set()

    def wait_for_offset(self, expected_offset: int, timeout: float) -> bool:
        """Block until expected_offset is seen.

        Args:
            expected_offset: The message offset generated by the producer.
            timeout: How long the caller will wait for the offset in seconds.
        """
        with self._events_lock:
            # Note: The writer thread could be already seen the offset, in that
            # case the existing object must be used, otherwise a new one is
            # created.
            event = self._events.setdefault(expected_offset, Event())

        result = event.wait(timeout=timeout)

        with self._events_lock:
            del self._events[expected_offset]

        return result


class KafkaSchemaReader(Thread):
    def __init__(
        self,
        config: Config,
        master_coordinator: Optional[MasterCoordinator] = None,
    ) -> None:
        Thread.__init__(self, name="schema-reader")
        self.master_coordinator = master_coordinator
        self.timeout_ms = 200
        self.config = config
        self.subjects: Dict[Subject, SubjectData] = {}
        self.schemas: Dict[int, TypedSchema] = {}
        self.global_schema_id = 0
        self.admin_client: Optional[KafkaAdminClient] = None
        self.topic_replication_factor = self.config["replication_factor"]
        self.consumer: Optional[KafkaConsumer] = None
        self.offset_watcher = OffsetsWatcher()
        self.id_lock = Lock()
        self.stats = StatsClient(
            sentry_config=config["sentry"],  # type: ignore[arg-type]
        )

        # Thread synchronization objects
        # - offset is used by the REST API to wait until this thread has
        # consumed the produced messages. This makes the REST APIs more
        # consistent (e.g. a request to a schema that was just produced will
        # return the correct data.)
        # - ready is used by the REST API to wait until this thread has
        # synchronized with data in the schema topic. This prevents the server
        # from returning stale data (e.g. a schemas has been soft deleted, but
        # the topic has not been compacted yet, waiting allows this to consume
        # the soft delete message and return the correct data instead of the
        # old stale version that has not been deleted yet.)
        self.offset = OFFSET_EMPTY
        self.ready = False

        # This event controls when the Reader should stop running, it will be
        # set by another thread (e.g. `KarapaceSchemaRegistry`)
        self._stop = Event()

        # Content based deduplication of schemas. This is used to reduce memory
        # usage when the same schema is produce multiple times to the same or
        # different subjects. The deduplication is based on the schema content
        # instead of the ids to handle corrupt data (where the ids are equal
        # but the schema themselves don't match)
        self._hash_to_schema: Dict[int, TypedSchema] = {}
        self._hash_to_schema_id_on_subject: Dict[int, SchemaId] = {}

    def get_schema_id(self, new_schema: TypedSchema) -> int:
        with self.id_lock:
            for schema_id, schema in self.schemas.items():
                if schema == new_schema:
                    return schema_id
            self.global_schema_id += 1
            return self.global_schema_id

    def get_schema_id_if_exists(self, *, subject: Subject, schema: TypedSchema) -> Optional[SchemaId]:
        return self._hash_to_schema_id_on_subject.get(hash((subject, schema.__str__())), None)

    def _set_schema_id_on_subject(self, *, subject: Subject, schema: TypedSchema, schema_id: SchemaId) -> None:
        self._hash_to_schema_id_on_subject.setdefault(hash((subject, schema.__str__())), schema_id)

    def _delete_from_schema_id_on_subject(self, *, subject: Subject, schema: TypedSchema) -> None:
        self._hash_to_schema_id_on_subject.pop(hash((subject, schema.__str__())), None)

    def close(self) -> None:
        LOG.info("Closing schema_reader")
        self._stop.set()

    def run(self) -> None:
        with ExitStack() as stack:
            while not self._stop.is_set() and self.admin_client is None:
                try:
                    self.admin_client = _create_admin_client_from_config(self.config)
                    stack.enter_context(closing(self.admin_client))
                except (NodeNotReadyError, NoBrokersAvailable, AssertionError):
                    LOG.warning("[Admin Client] No Brokers available yet. Retrying")
                    self._stop.wait(timeout=KAFKA_CLIENT_CREATION_TIMEOUT_SECONDS)
                except KafkaConfigurationError:
                    LOG.exception("[Admin Client] Invalid configuration. Bailing")
                    raise
                except Exception as e:  # pylint: disable=broad-except
                    LOG.exception("[Admin Client] Unexpected exception. Retrying")
                    self.stats.unexpected_exception(ex=e, where="admin_client_instantiation")
                    self._stop.wait(timeout=2.0)

            while not self._stop.is_set() and self.consumer is None:
                try:
                    self.consumer = _create_consumer_from_config(self.config)
                    stack.enter_context(closing(self.consumer))
                except (NodeNotReadyError, NoBrokersAvailable, AssertionError):
                    LOG.warning("[Consumer] No Brokers available yet. Retrying")
                    self._stop.wait(timeout=2.0)
                except KafkaConfigurationError:
                    LOG.exception("[Consumer] Invalid configuration. Bailing")
                    raise
                except Exception as e:  # pylint: disable=broad-except
                    LOG.exception("[Consumer] Unexpected exception. Retrying")
                    self.stats.unexpected_exception(ex=e, where="consumer_instantiation")
                    self._stop.wait(timeout=KAFKA_CLIENT_CREATION_TIMEOUT_SECONDS)

            schema_topic_exists = False
            schema_topic = new_schema_topic_from_config(self.config)
            schema_topic_create = [schema_topic]
            while not self._stop.is_set() and not schema_topic_exists:
                try:
                    LOG.info("[Schema Topic] Creating %r", schema_topic)
                    self.admin_client.create_topics(schema_topic_create, timeout_ms=constants.TOPIC_CREATION_TIMEOUT_MS)
                    LOG.info("[Schema Topic] Successfully created %r", schema_topic.name)
                    schema_topic_exists = True
                except TopicAlreadyExistsError:
                    LOG.warning("[Schema Topic] Already exists %r", schema_topic.name)
                    schema_topic_exists = True
                except:  # pylint: disable=bare-except
                    LOG.exception("[Schema Topic] Failed to create %r, retrying", schema_topic.name)
                    self._stop.wait(timeout=SCHEMA_TOPIC_CREATION_TIMEOUT_SECONDS)

            while not self._stop.is_set():
                try:
                    self.handle_messages()
                except Exception as e:  # pylint: disable=broad-except
                    self.stats.unexpected_exception(ex=e, where="schema_reader_loop")
                    LOG.exception("Unexpected exception in schema reader loop")

    def handle_messages(self) -> None:
        assert self.consumer is not None, "Thread must be started"

        raw_msgs = self.consumer.poll(timeout_ms=self.timeout_ms)
        if self.ready is False and not raw_msgs:
            self.ready = True
        add_offsets = False
        if self.master_coordinator is not None:
            are_we_master, _ = self.master_coordinator.get_master_info()
            # keep old behavior for True. When are_we_master is False, then we are a follower, so we should not accept direct
            # writes anyway. When are_we_master is None, then this particular node is waiting for a stable value, so any
            # messages off the topic are writes performed by another node
            # Also if master_elibility is disabled by configuration, disable writes too
            if are_we_master is True:
                add_offsets = True

        for _, msgs in raw_msgs.items():
            for msg in msgs:
                try:
                    key = json.loads(msg.key.decode("utf8"))
                except json.JSONDecodeError:
                    LOG.exception("Invalid JSON in msg.key")
                    continue

                value = None
                if msg.value:
                    try:
                        value = json.loads(msg.value.decode("utf8"))
                    except json.JSONDecodeError:
                        LOG.exception("Invalid JSON in msg.value")
                        continue

                self.handle_msg(key, value)
                self.offset = msg.offset
                if self.ready and add_offsets:
                    self.offset_watcher.offset_seen(self.offset)

    def _handle_msg_config(self, key: dict, value: Optional[dict]) -> None:
        subject = key.get("subject")
        if subject is not None:
            if subject not in self.subjects:
                LOG.info("Adding first version of subject: %r with no schemas", subject)
                self.subjects[subject] = {"schemas": {}}

            if not value:
                LOG.info("Deleting compatibility config completely for subject: %r", subject)
                self.subjects[subject].pop("compatibility", None)
            else:
                LOG.info("Setting subject: %r config to: %r, value: %r", subject, value["compatibilityLevel"], value)
                self.subjects[subject]["compatibility"] = value["compatibilityLevel"]
        elif value is not None:
            LOG.info("Setting global config to: %r, value: %r", value["compatibilityLevel"], value)
            self.config["compatibility"] = value["compatibilityLevel"]

    def _handle_msg_delete_subject(self, key: dict, value: Optional[dict]) -> None:  # pylint: disable=unused-argument
        if value is None:
            LOG.error("DELETE_SUBJECT record doesnt have a value, should have")
            return

        subject = value["subject"]
        if subject not in self.subjects:
            LOG.error("Subject: %r did not exist, should have", subject)
        else:
            LOG.info("Deleting subject: %r, value: %r", subject, value)

            updated_schemas: Dict[int, Schema] = {}
            for schema_key, schema in self.subjects[subject]["schemas"].items():
                updated_schemas[schema_key] = self._delete_schema_below_version(schema, value["version"])
                self._delete_from_schema_id_on_subject(subject=subject, schema=schema["schema"])
            self.subjects[value["subject"]]["schemas"] = updated_schemas

    def _handle_msg_schema_hard_delete(self, key: dict) -> None:
        subject, version = key["subject"], key["version"]

        if subject not in self.subjects:
            LOG.error("Hard delete: Subject %s did not exist, should have", subject)
        elif version not in self.subjects[subject]["schemas"]:
            LOG.error("Hard delete: Version %d for subject %s did not exist, should have", version, subject)
        else:
            LOG.info("Hard delete: subject: %r version: %r", subject, version)
            self.subjects[subject]["schemas"].pop(version, None)

    def _handle_msg_schema(self, key: dict, value: Optional[dict]) -> None:
        if not value:
            self._handle_msg_schema_hard_delete(key)
            return

        schema_type = value.get("schemaType", "AVRO")
        schema_str = value["schema"]
        schema_subject = value["subject"]
        schema_id = value["id"]
        schema_version = value["version"]
        schema_deleted = value.get("deleted", False)

        try:
            schema_type_parsed = SchemaType(schema_type)
        except ValueError:
            LOG.error("Invalid schema type: %s", schema_type)
            return

        # This does two jobs:
        # - Validates the schema's JSON
        # - Re-encode the schema to make sure small differences on formatting
        # won't interfere with the equality. Note: This means it is possible
        # for the REST API to return data that is formated differently from
        # what is available in the topic.
        if schema_type_parsed in [SchemaType.AVRO, SchemaType.JSONSCHEMA]:
            try:
                schema_str = json.dumps(json.loads(schema_str), sort_keys=True)
            except json.JSONDecodeError:
                LOG.error("Schema is not invalid JSON")
                return

        if schema_subject not in self.subjects:
            LOG.info("Adding first version of subject: %r with no schemas", schema_subject)
            self.subjects[schema_subject] = {"schemas": {}}

        subjects_schemas = self.subjects[schema_subject]["schemas"]

        with self.id_lock:
            # dedup schemas to reduce memory pressure
            schema_str = self._hash_to_schema.setdefault(hash(schema_str), schema_str)

            if schema_version in subjects_schemas:
                LOG.info("Updating entry subject: %r version: %r id: %r", schema_subject, schema_version, schema_id)
            else:
                LOG.info("Adding entry subject: %r version: %r id: %r", schema_subject, schema_version, schema_id)

            typed_schema = TypedSchema(
                schema_type=schema_type_parsed,
                schema_str=schema_str,
            )
            subjects_schemas[schema_version] = {
                "schema": typed_schema,
                "version": schema_version,
                "id": schema_id,
                "deleted": schema_deleted,
            }

            self.schemas[schema_id] = typed_schema
            self.global_schema_id = max(self.global_schema_id, schema_id)
            if not schema_deleted:
                self._set_schema_id_on_subject(subject=schema_subject, schema=typed_schema, schema_id=schema_id)
            else:
                self._delete_from_schema_id_on_subject(subject=schema_subject, schema=typed_schema)

    def handle_msg(self, key: dict, value: Optional[dict]) -> None:
        if key["keytype"] == "CONFIG":
            self._handle_msg_config(key, value)
        elif key["keytype"] == "SCHEMA":
            self._handle_msg_schema(key, value)
        elif key["keytype"] == "DELETE_SUBJECT":
            self._handle_msg_delete_subject(key, value)
        elif key["keytype"] == "NOOP":  # for spec completeness
            pass

    @staticmethod
    def _delete_schema_below_version(schema: Schema, version: Version) -> Schema:
        if schema["version"] <= version:
            schema["deleted"] = True
        return schema

    def get_schemas(
        self,
        subject: Subject,
        *,
        include_deleted: bool = False,
    ) -> Dict:
        if subject not in self.subjects:
            return {}
        if include_deleted:
            return self.subjects[subject]["schemas"]
        non_deleted_schemas = {
            key: val for key, val in self.subjects[subject]["schemas"].items() if val.get("deleted", False) is False
        }
        return non_deleted_schemas
