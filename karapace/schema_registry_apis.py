from contextlib import closing
from enum import Enum, unique
from http import HTTPStatus
from karapace import version as karapace_version
from karapace.avro_compatibility import is_incompatible
from karapace.compatibility import check_compatibility, CompatibilityModes
from karapace.config import read_config
from karapace.karapace import KarapaceBase
from karapace.master_coordinator import MasterCoordinator
from karapace.rapu import HTTPRequest
from karapace.schema_reader import InvalidSchema, KafkaSchemaReader, Schema, SchemaType, Subject, TypedSchema
from karapace.utils import json_encode
from typing import Any, List, NoReturn, Optional, Tuple, Union

import argparse
import asyncio
import logging
import sys
import time

# Literal is only supported for py3.8+ this allows type checking the code with
# the latest version of python, but also running it with older versions.
#
# typing_extensions was intentionally avoided
try:
    from typing import Literal  # pylint: disable=ungrouped-imports

    VersionIntOrLatest = Union[Literal["latest"], int]
except ImportError:
    VersionIntOrLatest = Any  # type: ignore

LATEST = "latest"


@unique
class SchemaErrorCodes(Enum):
    HTTP_NOT_FOUND = HTTPStatus.NOT_FOUND.value
    HTTP_CONFLICT = HTTPStatus.CONFLICT.value
    HTTP_UNPROCESSABLE_ENTITY = HTTPStatus.UNPROCESSABLE_ENTITY.value
    HTTP_INTERNAL_SERVER_ERROR = HTTPStatus.INTERNAL_SERVER_ERROR.value
    SUBJECT_NOT_FOUND = 40401
    VERSION_NOT_FOUND = 40402
    SCHEMA_NOT_FOUND = 40403
    SUBJECT_SOFT_DELETED = 40404
    SUBJECT_NOT_SOFT_DELETED = 40405
    SCHEMAVERSION_SOFT_DELETED = 40406
    SCHEMAVERSION_NOT_SOFT_DELETED = 40407
    INVALID_VERSION_ID = 42202
    INVALID_COMPATIBILITY_LEVEL = 42203
    INVALID_AVRO_SCHEMA = 44201
    NO_MASTER_ERROR = 50003


class InvalidSchemaType(Exception):
    pass


class KarapaceSchemaRegistry(KarapaceBase):
    # pylint: disable=attribute-defined-outside-init
    def __init__(self, config_file_path: str, config: dict) -> None:
        super().__init__(config_file_path=config_file_path, config=config)
        self._add_routes()

        self.mc = MasterCoordinator(config=self.config)
        self.ksr = KafkaSchemaReader(config=self.config, master_coordinator=self.mc)

        self.mc.start()
        self.ksr.start()

    def _add_routes(self):
        self.route(
            "/compatibility/subjects/<subject:path>/versions/<version:path>",
            callback=self.compatibility_check,
            method="POST",
            schema_request=True,
        )
        self.route(
            "/config/<subject:path>",
            callback=self.config_subject_get,
            method="GET",
            schema_request=True,
            with_request=True,
            json_body=False,
        )
        self.route(
            "/config/<subject:path>",
            callback=self.config_subject_set,
            method="PUT",
            schema_request=True,
        )
        self.route(
            "/config",
            callback=self.config_get,
            method="GET",
            schema_request=True,
        )
        self.route(
            "/config",
            callback=self.config_set,
            method="PUT",
            schema_request=True,
        )
        self.route(
            "/schemas/ids/<schema_id:path>/versions",
            callback=self.schemas_get_versions,
            method="GET",
            schema_request=True,
        )
        self.route(
            "/schemas/ids/<schema_id:path>",
            callback=self.schemas_get,
            method="GET",
            schema_request=True,
        )
        self.route(
            "/schemas/types",
            callback=self.schemas_types,
            method="GET",
            schema_request=True,
        )
        self.route(
            "/subjects",
            callback=self.subjects_list,
            method="GET",
            schema_request=True,
        )
        self.route(
            "/subjects/<subject:path>/versions",
            callback=self.subject_post,
            method="POST",
            schema_request=True,
        )
        self.route(
            "/subjects/<subject:path>",
            callback=self.subjects_schema_post,
            method="POST",
            schema_request=True,
        )
        self.route(
            "/subjects/<subject:path>/versions",
            callback=self.subject_versions_list,
            method="GET",
            schema_request=True,
        )
        self.route(
            "/subjects/<subject:path>/versions/<version>",
            callback=self.subject_version_get,
            method="GET",
            schema_request=True,
        )
        self.route(
            "/subjects/<subject:path>/versions/<version:path>",  # needs
            callback=self.subject_version_delete,
            method="DELETE",
            schema_request=True,
            with_request=True,
            json_body=False,
        )
        self.route(
            "/subjects/<subject:path>/versions/<version>/schema",
            callback=self.subject_version_schema_get,
            method="GET",
            schema_request=True,
        )
        self.route(
            "/subjects/<subject:path>",
            callback=self.subject_delete,
            method="DELETE",
            schema_request=True,
            with_request=True,
            json_body=False,
        )

    def close(self):
        super().close()
        self.log.info("Shutting down all auxiliary threads")
        if self.mc:
            self.mc.close()
        if self.ksr:
            self.ksr.close()

    def _validate_version_int(self, content_type: str, version: Any) -> int:
        try:
            version = int(version)
        except ValueError:
            pass

        is_valid_int = isinstance(version, int) and version >= 0

        if not is_valid_int:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_VERSION_ID.value,
                    "message": (
                        "The specified version is not a valid version id. "
                        "Allowed values are between [1, 2^31-1]"
                    ),
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

        return version

    def _validate_version_int_or_latest(
        self,
        content_type: str,
        version: str,
    ) -> VersionIntOrLatest:
        if version == LATEST:
            return "latest"

        try:
            version_int = int(version)
            is_valid_int = version_int >= 0
        except ValueError:
            is_valid_int = False

        if not is_valid_int:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_VERSION_ID.value,
                    "message": (
                        "The specified version is not a valid version id. "
                        'Allowed values are between [1, 2^31-1] and the string "latest"'
                    ),
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

        return version_int

    def _validate_schema_id(self, content_type: str, schema_id: str) -> int:
        try:
            return int(schema_id)
        except ValueError:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_NOT_FOUND.value,
                    "message": "HTTP 404 Not Found",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

    def _validate_schema_type(self, content_type: str, schema_type: str) -> SchemaType:
        try:
            parse_schema_type = SchemaType(schema_type)
        except ValueError:
            self.log.warning("Invalid schema type: %r", schema_type)
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_UNPROCESSABLE_ENTITY.value,
                    "message": f"Invalid {schema_type} schema",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

        if parse_schema_type not in TypedSchema.SUPPORTED_TYPES:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_UNPROCESSABLE_ENTITY.value,
                    "message": f"Unsupported schemaType: {schema_type}",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

        return parse_schema_type

    def _validate_schema(self, content_type: str, schema_type: SchemaType, schema: str) -> TypedSchema:
        try:
            return TypedSchema.parse(schema_type, schema)
        except (InvalidSchema, InvalidSchemaType):
            self.log.warning("Invalid schema: %r", schema)
            self.r(
                body={
                    # XXX: Is the same error code used for the other types?
                    "error_code": SchemaErrorCodes.INVALID_AVRO_SCHEMA.value,
                    "message": f"Invalid {schema_type} schema",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

    def _validate_schema_request_body(self, content_type: str, body) -> None:
        if not isinstance(body, dict):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                    "message": "Internal Server Error",
                },
                content_type=content_type,
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        for field in body:
            if field not in {"schema", "schemaType"}:
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.HTTP_UNPROCESSABLE_ENTITY.value,
                        "message": f"Unrecognized field: {field}",
                    },
                    content_type=content_type,
                    status=HTTPStatus.UNPROCESSABLE_ENTITY,
                )

    def _validate_compatibility_level(self, content_type: str, compatibility_mode: Any) -> CompatibilityModes:
        try:
            return CompatibilityModes(compatibility_mode)
        except (ValueError, KeyError):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.INVALID_COMPATIBILITY_LEVEL.value,
                    "message": "Invalid compatibility level. Valid values are none, backward, forward and full",
                },
                content_type=content_type,
                status=HTTPStatus.UNPROCESSABLE_ENTITY,
            )

    def _validate_is_new_schema(
        self,
        new_schema: TypedSchema,
        schemadata_in_version_order: List[Schema],
        content_type: str,
    ) -> None:
        """If the schema is already registered for the subject use it instead of re-registering.

        Note: This ignores soft deleted schemas
        """
        for schema_data in schemadata_in_version_order:
            if schema_data.schema == new_schema:
                self.r({"id": schema_data.global_id}, content_type)

        self.log.debug("No existing schema matched: %s", new_schema)

    def _validate_compatibility_new_schema(
        self,
        subject: str,
        new_schema: TypedSchema,
        content_type: str,
    ) -> None:
        subject_data = self.ksr.shallow_copy_of_subject(subject)

        if subject_data is not None:
            schemadata_in_version_order = sorted(
                (schema for schema in subject_data.versions.values() if schema.deleted is False),
                key=lambda schema: schema.version,
            )

            self._validate_is_new_schema(new_schema, schemadata_in_version_order, content_type)

            compatibility_mode = self._get_compatibility_mode_for_subject(subject_data, content_type)

            last_schema = schemadata_in_version_order[-1]

            # Run a compatibility check between on file schema(s) and the one being submitted now
            # the check is either towards the latest one or against all previous ones in case of
            # transitive mode
            if compatibility_mode.is_transitive():
                check_against = schemadata_in_version_order
            else:
                check_against = [last_schema]

            for old_schemadata in check_against:
                result = check_compatibility(
                    old_schema=old_schemadata.schema,
                    new_schema=new_schema,
                    compatibility_mode=compatibility_mode,
                )
                if is_incompatible(result):
                    message = set(result.messages).pop() if result.messages else ""
                    self.log.warning("Incompatible schema: %s", result)
                    self.r(
                        body={
                            "error_code": SchemaErrorCodes.HTTP_CONFLICT.value,
                            "message": f"Incompatible schema, compatibility_mode={compatibility_mode.value} {message}",
                        },
                        content_type=content_type,
                        status=HTTPStatus.CONFLICT,
                    )

    async def get_master(self) -> Tuple[Optional[bool], Optional[str]]:
        async with self.master_lock:
            while True:
                master, master_url = self.mc.get_master_info()
                if master is None:
                    self.log.info("No master set: %r, url: %r", master, master_url)
                elif self.ksr.ready is False:
                    self.log.info("Schema reader isn't ready yet: %r", self.ksr.ready)
                else:
                    return master, master_url
                await asyncio.sleep(1.0)

    def _get_subject(self, subject: str, content_type: str) -> Subject:
        subject_data = self.ksr.shallow_copy_of_subject(subject)

        if subject_data is None or not subject_data.versions:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                    "message": f"Subject '{subject}' not found.",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        return subject_data

    def _get_subject_and_schema_by_id_or_latest(
        self,
        content_type: str,
        subject: str,
        version: str,
    ) -> Tuple[Subject, Schema]:
        version_int_or_latest = self._validate_version_int_or_latest(content_type, version)
        subject_data = self._get_subject(subject, content_type)
        schema_data = None
        max_version = max(subject_data.versions)

        if version_int_or_latest == "latest":
            schema_data = subject_data.versions[max_version]
        elif version_int_or_latest <= max_version:
            schema_data = subject_data.versions.get(version_int_or_latest)
        else:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        if not schema_data:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        return subject_data, schema_data

    def _get_compatibility_mode_for_subject(self, subject_data: Subject, content_type: str) -> CompatibilityModes:
        compatibility_mode = subject_data.compatibility

        if compatibility_mode is None:
            try:
                parsed_compatibility_mode = CompatibilityModes(self.ksr.config["compatibility"])
            except ValueError:
                # Using INTERNAL_SERVER_ERROR because the subject and configuration
                # should have been validated before.
                self.r(
                    body={
                        "error_code": SchemaErrorCodes.HTTP_INTERNAL_SERVER_ERROR.value,
                        "message": f"Unknown compatibility mode {compatibility_mode}",
                    },
                    content_type=content_type,
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
        return parsed_compatibility_mode

    def _get_schemas(self, subject: str, include_deleted: bool) -> List[Schema]:
        subject_data = self.ksr.shallow_copy_of_subject(subject)
        if subject_data and not include_deleted:
            return [schema for schema in subject_data.versions.values() if schema.deleted is False]
        if subject_data:
            return list(subject_data.versions.values())
        return []

    def _get_offset_from_queue(self, sent_offset):
        start_time = time.monotonic()
        while True:
            self.log.info("Starting to wait for offset: %r from ksr queue", sent_offset)
            offset = self.ksr.queue.get()
            if offset == sent_offset:
                self.log.info(
                    "We've consumed back produced offset: %r message back, everything is in sync, took: %.4f",
                    offset,
                    time.monotonic() - start_time,
                )
                break
            self.log.warning("Put the offset: %r back to queue, someone else is waiting for this?", offset)
            self.ksr.queue.put(offset)

    async def _forward_request_remote_and_raise(self, *, body, url, content_type, method="POST") -> NoReturn:
        self.log.info("Forwarding request to master: %r", url)
        response = await self.http_request(url=url, method=method, json=body, timeout=60.0)
        self.r(body=response.body, content_type=content_type, status=HTTPStatus(response.status))

    async def compatibility_check(
        self,
        content_type: str,
        *,
        subject: str,
        version: str,
        request,
    ) -> NoReturn:
        body = request.json
        schema_type = self._validate_schema_type(
            content_type=content_type,
            schema_type=body.get("schemaType", SchemaType.AVRO.value),
        )
        new_schema = self._validate_schema(content_type, schema_type, body["schema"])
        subject_data, old_schema_data = self._get_subject_and_schema_by_id_or_latest(
            content_type,
            subject,
            version,
        )
        compatibility_mode = self._get_compatibility_mode_for_subject(subject_data, content_type)

        result = check_compatibility(
            old_schema=old_schema_data.schema,
            new_schema=new_schema,
            compatibility_mode=compatibility_mode,
        )
        if is_incompatible(result):
            self.log.warning(
                "Invalid schema %s found by compatibility check: old: %s new: %s",
                result,
                old_schema_data.schema,
                new_schema,
            )
            self.r({"is_compatible": False}, content_type)
        self.r({"is_compatible": True}, content_type)

    async def schemas_get(self, content_type, *, schema_id) -> NoReturn:
        schema_id_int = self._validate_schema_id(content_type, schema_id)
        schema = self.ksr.get_typed_schema_by_id(schema_id_int)
        if not schema:
            self.log.warning("Schema: %r that was requested, not found", schema_id)
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SCHEMA_NOT_FOUND.value,
                    "message": "Schema not found",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )
        response_body = {"schema": schema.schema_str}
        if schema.schema_type is not SchemaType.AVRO:
            response_body["schemaType"] = schema.schema_type
        self.r(response_body, content_type)

    async def schemas_get_versions(self, content_type: str, *, schema_id: str) -> NoReturn:
        schema_id_int = self._validate_schema_id(content_type, schema_id)
        subject_versions = []
        for subject in self.ksr.shallow_copy_of_all_subjects().values():
            for version, schema in subject.versions.items():
                if schema.global_id == schema_id_int and not schema.deleted:
                    subject_versions.append({"subject": subject, "version": version})

        if not subject_versions:
            self.log.warning("Schema: %r that was requested, not found", schema_id)
            self.r(
                body={
                    "error_code": SchemaErrorCodes.HTTP_NOT_FOUND.value,
                    "message": "HTTP 404 Not Found",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        subject_versions = sorted(subject_versions, key=lambda s: (s["subject"], s["version"]))
        self.r(subject_versions, content_type)

    async def schemas_types(self, content_type: str) -> NoReturn:
        self.r([schema_type.value for schema_type in TypedSchema.SUPPORTED_TYPES], content_type)

    async def config_get(self, content_type: str) -> NoReturn:
        # Note: The format sent by the user differs from the return value, this
        # is for compatibility reasons.
        self.r({"compatibilityLevel": self.ksr.config["compatibility"]}, content_type)

    async def config_set(self, content_type: str, *, request) -> NoReturn:
        body = request.json
        compatibility_level = self._validate_compatibility_level(content_type, request.json.get("compatibility"))

        are_we_master, master_url = await self.get_master()

        if are_we_master is None:
            self.no_master_error(content_type)

        if are_we_master is False:
            await self._forward_request_remote_and_raise(
                body=body,
                url=f"{master_url}/config",
                content_type=content_type,
                method="PUT",
            )

        self.ksr.config_set(compatibility_level)
        self.r({"compatibility": self.ksr.config["compatibility"]}, content_type)

    async def config_subject_get(
        self,
        content_type,
        subject: str,
        *,
        request: HTTPRequest,
    ) -> NoReturn:
        subject_data = self.ksr.shallow_copy_of_subject(subject)
        default_to_global = request.query.get("defaultToGlobal", "false").lower() == "true"

        if subject_data:
            compatibility = subject_data.compatibility
        elif default_to_global:
            compatibility = self.ksr.config["compatibility"]
        else:
            compatibility = None

        if compatibility:
            # Note: The format sent by the user differs from the return
            # value, this is for compatibility reasons.
            self.r(
                {"compatibilityLevel": compatibility},
                content_type,
            )

        self.r(
            body={
                "error_code": SchemaErrorCodes.SUBJECT_NOT_FOUND.value,
                "message": "Subject not found.",
            },
            content_type=content_type,
            status=HTTPStatus.NOT_FOUND,
        )

    async def config_subject_set(self, content_type: str, *, request, subject):
        body = request.json
        compatibility_level = self._validate_compatibility_level(content_type, body.get("compatibility"))

        are_we_master, master_url = await self.get_master()

        if are_we_master is None:
            self.no_master_error(content_type)

        if are_we_master is False:
            await self._forward_request_remote_and_raise(
                body=body,
                url=f"{master_url}/config/{subject}",
                content_type=content_type,
                method="PUT",
            )

        self.ksr.config_subject_set(subject, compatibility_level)
        self.r({"compatibility": compatibility_level.value}, content_type)

    async def subjects_list(self, content_type: str) -> NoReturn:
        subjects_list = [subject.name for subject in self.ksr.shallow_copy_of_all_subjects().values() if subject.versions]
        self.r(subjects_list, content_type, status=HTTPStatus.OK)

    async def subject_delete(self, content_type: str, *, subject: str, request: HTTPRequest) -> NoReturn:
        permanent = request.query.get("permanent", "false").lower() == "true"
        subject_data = self._get_subject(subject, content_type)

        if permanent and any(not schema.deleted for schema in subject_data.versions.values()):
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SUBJECT_NOT_SOFT_DELETED.value,
                    "message": f"Subject '{subject}' was not deleted first before being permanently deleted",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        are_we_master, master_url = await self.get_master()
        if are_we_master is None:
            self.no_master_error(content_type)

        if are_we_master is False:
            await self._forward_request_remote_and_raise(
                body={},
                url=f"{master_url}/subjects/{subject}?permanent={permanent}",
                content_type=content_type,
                method="DELETE",
            )

        version_list = sorted(subject_data.versions)
        self.ksr.subject_delete(subject, permanent)
        self.r(version_list, content_type, status=HTTPStatus.OK)

    async def subject_version_get(self, content_type: str, *, subject: str, version: str) -> NoReturn:
        _, schema_data = self._get_subject_and_schema_by_id_or_latest(
            content_type,
            subject,
            version,
        )
        schema_id = schema_data.global_id
        schema = schema_data.schema

        ret = {
            "subject": subject,
            "version": version,
            "id": schema_id,
            "schema": schema.schema_str,
        }
        if schema.schema_type is not SchemaType.AVRO:
            ret["schemaType"] = schema.schema_type

        self.r(ret, content_type)

    async def subject_version_delete(self, content_type: str, *, subject: str, version: str, request: HTTPRequest):
        version_int = self._validate_version_int(content_type, version)
        permanent = request.query.get("permanent", "false").lower() == "true"

        subject_data = self._get_subject(subject, content_type)
        schema_data = subject_data.versions.get(version_int)

        if not schema_data:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.VERSION_NOT_FOUND.value,
                    "message": f"Version {version} not found.",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        if schema_data.deleted and not permanent:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SCHEMAVERSION_SOFT_DELETED.value,
                    "message": f"Subject '{subject}' Version 1 was soft deleted. Set permanent=true to delete permanently",
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        # Cannot directly hard delete
        if permanent and not schema_data.deleted:
            self.r(
                body={
                    "error_code": SchemaErrorCodes.SCHEMAVERSION_NOT_SOFT_DELETED.value,
                    "message": (
                        f"Subject '{subject}' Version {version} was not deleted "
                        "first before being permanently deleted"
                    ),
                },
                content_type=content_type,
                status=HTTPStatus.NOT_FOUND,
            )

        are_we_master, master_url = await self.get_master()

        if are_we_master is None:
            self.no_master_error(content_type)

        if are_we_master is False:
            await self._forward_request_remote_and_raise(
                body={},
                url=f"{master_url}/subjects/{subject}/versions/{version}?permanent={permanent}",
                content_type=content_type,
                method="DELETE",
            )

        self.ksr.subject_version_delete(subject, schema_data, permanent)
        self.r(version, content_type, status=HTTPStatus.OK)

    async def subject_version_schema_get(self, content_type: str, *, subject: str, version: str) -> NoReturn:
        _, schema_data = self._get_subject_and_schema_by_id_or_latest(content_type, subject, version)
        self.r(schema_data.schema.schema_str, content_type)

    async def subject_versions_list(self, content_type: str, *, subject: str) -> NoReturn:
        subject_data = self._get_subject(subject, content_type)
        self.r(list(subject_data.versions), content_type, status=HTTPStatus.OK)

    async def subjects_schema_post(self, content_type: str, *, subject: str, request) -> NoReturn:
        body = request.json
        self._validate_schema_request_body(content_type, body)
        subject_data = self._get_subject(subject, content_type)
        schema_type = self._validate_schema_type(content_type, body.get("schemaType", SchemaType.AVRO.value))
        new_schema = self._validate_schema(content_type, schema_type, body.get("schema"))

        # XXX: Should this include deleted schemas?
        for schema_data in subject_data.versions.values():
            if schema_data.schema == new_schema:
                ret = {
                    "subject": subject,
                    "version": schema_data.version,
                    "id": schema_data.global_id,
                    "schema": new_schema.schema_str,
                }
                if schema_type is not SchemaType.AVRO:
                    ret["schemaType"] = schema_type
                self.r(ret, content_type)

        self.log.debug("Schema %r did not match", new_schema)
        self.r(
            body={
                "error_code": SchemaErrorCodes.SCHEMA_NOT_FOUND.value,
                "message": "Schema not found",
            },
            content_type=content_type,
            status=HTTPStatus.NOT_FOUND,
        )

    async def subject_post(self, content_type: str, *, subject: str, request) -> NoReturn:
        body = request.json
        self._validate_schema_request_body(content_type, body)
        schema_type = self._validate_schema_type(
            content_type=content_type,
            schema_type=body.get("schemaType", SchemaType.AVRO.value),
        )
        new_schema = self._validate_schema(content_type, schema_type, body.get("schema"))

        self._validate_compatibility_new_schema(subject, new_schema, content_type)

        are_we_master, master_url = await self.get_master()

        if are_we_master is None:
            self.no_master_error(content_type)

        if are_we_master is False:
            await self._forward_request_remote_and_raise(
                body=body,
                url=f"{master_url}/subjects/{subject}/versions",
                content_type=content_type,
                method="POST",
            )

        schema_id = self.ksr.apply_schema_after_validation(subject, new_schema)
        self.r({"id": schema_id}, content_type)

    def no_master_error(self, content_type: str) -> NoReturn:
        self.r(
            body={
                "error_code": SchemaErrorCodes.NO_MASTER_ERROR.value,
                "message": "Error while forwarding the request to the master.",
            },
            content_type=content_type,
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )


def main() -> int:
    parser = argparse.ArgumentParser(prog="karapace", description="Karapace: Your Kafka essentials in one tool")
    parser.add_argument(
        "--version",
        action="version",
        help="show program version",
        version=karapace_version.__version__,
    )
    parser.add_argument("config_file", help="configuration file path", type=argparse.FileType())
    arg = parser.parse_args()

    with closing(arg.config_file):
        config = read_config(arg.config_file)

    logging.getLogger().setLevel(config["log_level"])  # type: ignore
    kc = KarapaceSchemaRegistry(config_file_path=arg.config_file.name, config=config)
    try:
        kc.run(host=kc.config["host"], port=kc.config["port"])
    except Exception:  # pylint: disable-broad-except
        if kc.raven_client:
            kc.raven_client.captureException(tags={"where": "karapace"})
        raise

    return 0


if __name__ == "__main__":
    sys.exit(main())
