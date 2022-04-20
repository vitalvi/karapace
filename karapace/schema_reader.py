"""
karapace - Kafka schema reader

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, NodeNotReadyError, TopicAlreadyExistsError
from karapace import constants
from karapace.config import Config
from karapace.master_coordinator import MasterCoordinator
from karapace.schema_models import SchemaType, TypedSchema
from karapace.statsd import StatsClient
from karapace.utils import KarapaceKafkaClient
from threading import Event, Lock, Thread
from typing import Any, Dict, Optional

import logging
import time
import ujson

Offset = int
Subject = str
Version = int
Schema = Dict[str, Any]
# Container type for a subject, with configuration settings and all the schemas
SubjectData = Dict[str, Any]

# The value `0` is a valid offset and it represents the first message produced
# to a topic, therefore it can not be used.
OFFSET_EMPTY = -1
LOG = logging.getLogger(__name__)


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
        self.schema_topic = None
        self.topic_replication_factor = self.config["replication_factor"]
        self.consumer: Optional[KafkaConsumer] = None
        self.offset_watcher = OffsetsWatcher()
        self.running = True
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

    def init_consumer(self) -> None:
        # Group not set on purpose, all consumers read the same data
        session_timeout_ms = self.config["session_timeout_ms"]
        request_timeout_ms = max(session_timeout_ms, KafkaConsumer.DEFAULT_CONFIG["request_timeout_ms"])
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
            sasl_mechanism=self.config["sasl_mechanism"],
            sasl_plain_username=self.config["sasl_plain_username"],
            sasl_plain_password=self.config["sasl_plain_password"],
            auto_offset_reset="earliest",
            session_timeout_ms=session_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            kafka_client=KarapaceKafkaClient,
            metadata_max_age_ms=self.config["metadata_max_age_ms"],
        )

    def init_admin_client(self) -> bool:
        try:
            self.admin_client = KafkaAdminClient(
                api_version_auto_timeout_ms=constants.API_VERSION_AUTO_TIMEOUT_MS,
                bootstrap_servers=self.config["bootstrap_uri"],
                client_id=self.config["client_id"],
                security_protocol=self.config["security_protocol"],
                ssl_cafile=self.config["ssl_cafile"],
                ssl_certfile=self.config["ssl_certfile"],
                ssl_keyfile=self.config["ssl_keyfile"],
                sasl_mechanism=self.config["sasl_mechanism"],
                sasl_plain_username=self.config["sasl_plain_username"],
                sasl_plain_password=self.config["sasl_plain_password"],
            )
            return True
        except (NodeNotReadyError, NoBrokersAvailable, AssertionError):
            LOG.warning("No Brokers available yet, retrying init_admin_client()")
            time.sleep(2.0)
        except:  # pylint: disable=bare-except
            LOG.exception("Failed to initialize admin client, retrying init_admin_client()")
            time.sleep(2.0)
        return False

    @staticmethod
    def get_new_schema_topic(config: dict) -> NewTopic:
        return NewTopic(
            name=config["topic_name"],
            num_partitions=constants.SCHEMA_TOPIC_NUM_PARTITIONS,
            replication_factor=config["replication_factor"],
            topic_configs={"cleanup.policy": "compact"},
        )

    def create_schema_topic(self) -> bool:
        assert self.admin_client is not None, "Thread must be started"

        schema_topic = self.get_new_schema_topic(self.config)
        try:
            LOG.info("Creating topic: %r", schema_topic)
            self.admin_client.create_topics([schema_topic], timeout_ms=constants.TOPIC_CREATION_TIMEOUT_MS)
            LOG.info("Topic: %r created successfully", self.config["topic_name"])
            self.schema_topic = schema_topic
            return True
        except TopicAlreadyExistsError:
            LOG.warning("Topic: %r already exists", self.config["topic_name"])
            self.schema_topic = schema_topic
            return True
        except:  # pylint: disable=bare-except
            LOG.exception("Failed to create topic: %r, retrying create_schema_topic()", self.config["topic_name"])
            time.sleep(5)
        return False

    def get_schema_id(self, new_schema: TypedSchema) -> int:
        with self.id_lock:
            for schema_id, schema in self.schemas.items():
                if schema == new_schema:
                    return schema_id
            self.global_schema_id += 1
            return self.global_schema_id

    def close(self) -> None:
        LOG.info("Closing schema_reader")
        self.running = False

    def run(self) -> None:
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
                LOG.info("Status: offset: %r, ready: %r", self.offset, self.ready)
            except Exception as e:  # pylint: disable=broad-except
                if self.stats:
                    self.stats.unexpected_exception(ex=e, where="schema_reader_loop")
                LOG.exception("Unexpected exception in schema reader loop")
        try:
            if self.admin_client:
                self.admin_client.close()
            if self.consumer:
                self.consumer.close()
        except Exception as e:  # pylint: disable=broad-except
            if self.stats:
                self.stats.unexpected_exception(ex=e, where="schema_reader_exit")
            LOG.exception("Unexpected exception closing schema reader")

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
                    key = ujson.loads(msg.key.decode("utf8"))
                except ValueError:
                    LOG.exception("Invalid JSON in msg.key")
                    continue

                value = None
                if msg.value:
                    try:
                        value = ujson.loads(msg.value.decode("utf8"))
                    except ValueError:
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
            updated_schemas = {
                key: self._delete_schema_below_version(schema, value["version"])
                for key, schema in self.subjects[subject]["schemas"].items()
            }
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

        # Protobuf doesn't use JSON
        if schema_type_parsed in [SchemaType.AVRO, SchemaType.JSONSCHEMA]:
            try:
                ujson.loads(schema_str)
            except ValueError:
                LOG.error("Schema is not invalid JSON")
                return

        if schema_subject not in self.subjects:
            LOG.info("Adding first version of subject: %r with no schemas", schema_subject)
            self.subjects[schema_subject] = {"schemas": {}}

        subjects_schemas = self.subjects[schema_subject]["schemas"]

        typed_schema = TypedSchema(
            schema_type=schema_type_parsed,
            schema_str=schema_str,
        )
        schema = {
            "schema": typed_schema,
            "version": schema_version,
            "id": schema_id,
            "deleted": schema_deleted,
        }

        if schema_version in subjects_schemas:
            LOG.info("Updating entry subject: %r version: %r id: %r", schema_subject, schema_version, schema_id)
        else:
            LOG.info("Adding entry subject: %r version: %r id: %r", schema_subject, schema_version, schema_id)

        subjects_schemas[schema_version] = schema

        with self.id_lock:
            self.schemas[schema_id] = typed_schema
            self.global_schema_id = max(self.global_schema_id, schema_id)

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
        if include_deleted:
            return self.subjects[subject]["schemas"]
        non_deleted_schemas = {
            key: val for key, val in self.subjects[subject]["schemas"].items() if val.get("deleted", False) is False
        }
        return non_deleted_schemas
