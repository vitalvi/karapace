"""
karapace - main

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from functools import partial
from http import HTTPStatus
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient
from karapace import constants
from karapace.config import Config
from karapace.rapu import HTTPResponse, RestApp
from karapace.utils import KarapaceKafkaClient
from typing import NoReturn, Union

import asyncio
import logging
import os
import time

LOG_FORMAT_JOURNAL = "%(name)-20s\t%(threadName)s\t%(levelname)-8s\t%(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT_JOURNAL)


def create_kafka_producer_from_config(config: Config) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=config["bootstrap_uri"],
        security_protocol=config["security_protocol"],
        ssl_cafile=config["ssl_cafile"],
        ssl_certfile=config["ssl_certfile"],
        ssl_keyfile=config["ssl_keyfile"],
        sasl_mechanism=config["sasl_mechanism"],
        sasl_plain_username=config["sasl_plain_username"],
        sasl_plain_password=config["sasl_plain_password"],
        api_version=(1, 0, 0),
        metadata_max_age_ms=config["metadata_max_age_ms"],
        max_block_ms=2000,  # missing topics will block unless we cache cluster metadata and pre-check
        connections_max_idle_ms=config[
            "connections_max_idle_ms"
        ],  # helps through cluster upgrades ??
        kafka_client=KarapaceKafkaClient,
    )


def create_kafka_consumer_from_config(config: Config) -> KafkaConsumer:
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


def create_kafka_admin_from_config(config: Config) -> KafkaAdminClient:
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


class KarapaceBase(RestApp):
    def __init__(self, config_file_path: str, config: dict) -> None:
        self.config = {}
        self.producer = None
        self.kafka_timeout = 10
        self.config_path = config_file_path
        self.config = config
        self._sentry_config = self.config.get("sentry", {"dsn": None}).copy()
        if os.environ.get("SENTRY_DSN"):
            self._sentry_config["dsn"] = os.environ["SENTRY_DSN"]
        if "tags" not in self._sentry_config:
            self._sentry_config["tags"] = {}
        self._sentry_config["tags"]["app"] = "Karapace"

        super().__init__(app_name="karapace", sentry_config=self._sentry_config)
        self.route("/", callback=self.root_get, method="GET")
        self.log = logging.getLogger("Karapace")
        self.app.on_startup.append(self.create_http_client)
        self.master_lock = asyncio.Lock()
        self.log.info("Karapace initialized")

    def _create_producer(self) -> KafkaProducer:
        producer = None
        while producer is None:
            try:
                producer = create_kafka_producer_from_config(self.config)
            except:  # pylint: disable=bare-except
                self.log.exception("Unable to create producer, retrying")
                time.sleep(1)

        return producer

    def close(self) -> None:
        if not self.producer:
            return
        self.producer.close()
        self.producer = None

    @staticmethod
    def r(body: Union[dict, list, str], content_type: str, status: HTTPStatus = HTTPStatus.OK) -> NoReturn:
        raise HTTPResponse(
            body=body,
            status=status,
            content_type=content_type,
            headers={},
        )

    @staticmethod
    def internal_error(message: str, content_type: str) -> NoReturn:
        KarapaceBase.r(
            content_type=content_type,
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
            body={
                "message": message,
                "error_code": HTTPStatus.INTERNAL_SERVER_ERROR.value
            },
        )

    @staticmethod
    def unprocessable_entity(message: str, sub_code: int, content_type: str) -> NoReturn:
        KarapaceBase.r(
            content_type=content_type,
            status=HTTPStatus.UNPROCESSABLE_ENTITY,
            body={
                "message": message,
                "error_code": sub_code
            },
        )

    @staticmethod
    def topic_entity(message: str, sub_code: int, content_type: str) -> NoReturn:
        KarapaceBase.r(
            content_type=content_type,
            status=HTTPStatus.UNPROCESSABLE_ENTITY,
            body={
                "message": message,
                "error_code": sub_code
            },
        )

    @staticmethod
    def not_found(message: str, sub_code: int, content_type: str) -> NoReturn:
        KarapaceBase.r(
            content_type=content_type,
            status=HTTPStatus.NOT_FOUND,
            body={
                "message": message,
                "error_code": sub_code
            },
        )

    async def root_get(self) -> NoReturn:
        self.r({}, "application/json")


empty_response = partial(KarapaceBase.r, body={}, status=HTTPStatus.NO_CONTENT, content_type="application/json")
