import asyncio
import logging
import warnings
from collections import defaultdict
from ssl import SSLContext
from typing import Any

import async_timeout

from aiokafka import __version__
from aiokafka.abc import AbstractTokenProvider
from aiokafka.client import AIOKafkaClient
from aiokafka.errors import (
    LeaderNotAvailableError,
    NotControllerError,
    NotLeaderForPartitionError,
    for_code,
)
from aiokafka.protocol.admin import (
    AlterConfigsRequest,
    CreatePartitionsRequest,
    CreateTopicsRequest,
    DeleteRecordsRequest,
    DeleteTopicsRequest,
    DescribeConfigsRequest,
    DescribeGroupsRequest,
    ListGroupsRequest,
)
from aiokafka.protocol.api import Request, Response
from aiokafka.protocol.commit import OffsetFetchRequest
from aiokafka.protocol.coordination import FindCoordinatorRequest
from aiokafka.protocol.metadata import MetadataRequest
from aiokafka.structs import OffsetAndMetadata, TopicPartition

from .config_resource import ConfigResource, ConfigResourceType
from .new_partitions import NewPartitions
from .new_topic import NewTopic
from .records_to_delete import RecordsToDelete

log = logging.getLogger(__name__)


class AIOKafkaAdminClient:

    def __init__(
        self,
        *,
        loop=None,
        bootstrap_servers: str | list[str] = "localhost",
        client_id: str = "aiokafka-" + __version__,
        request_timeout_ms: int = 40000,
        connections_max_idle_ms: int = 540000,
        retry_backoff_ms: int = 100,
        metadata_max_age_ms: int = 300000,
        security_protocol: str = "PLAINTEXT",
        ssl_context: SSLContext | None = None,
        api_version: None = None,
        sasl_mechanism: str = "PLAIN",
        sasl_plain_username: str | None = None,
        sasl_plain_password: str | None = None,
        sasl_kerberos_service_name: str = "kafka",
        sasl_kerberos_domain_name: str | None = None,
        sasl_oauth_token_provider: AbstractTokenProvider | None = None,
    ):
        if api_version is not None:
            warnings.warn(
                "The `api_version` parameter has been deprecated since 0.13.0. "
                "It is now a no-op and will be removed in a future release. ",
                DeprecationWarning,
                stacklevel=2,
            )

        self._closed = False
        self._started = False
        self._version_info = {}
        self._request_timeout_ms = request_timeout_ms
        self._client = AIOKafkaClient(
            loop=loop,
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            metadata_max_age_ms=metadata_max_age_ms,
            request_timeout_ms=request_timeout_ms,
            retry_backoff_ms=retry_backoff_ms,
            ssl_context=ssl_context,
            security_protocol=security_protocol,
            connections_max_idle_ms=connections_max_idle_ms,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            sasl_kerberos_service_name=sasl_kerberos_service_name,
            sasl_kerberos_domain_name=sasl_kerberos_domain_name,
            sasl_oauth_token_provider=sasl_oauth_token_provider,
        )

    async def close(self):
        """Close the AIOKafkaAdminClient connection to the Kafka broker."""
        if not hasattr(self, "_closed") or self._closed:
            log.info("AIOKafkaAdminClient already closed.")
            return

        await self._client.close()
        self._closed = True
        log.debug("AIOKafkaAdminClient is now closed.")






    async def create_topics(
        self,
        new_topics: list[NewTopic],
        timeout_ms: int | None = None,
        validate_only: bool = False,
    ) -> Response:
        pass

    async def delete_topics(
        self,
        topics: list[str],
        timeout_ms: int | None = None,
    ) -> Response:
        pass

    async def _get_cluster_metadata(
        self,
        topics: list[str] | None = None,
    ) -> Response:
        pass




    async def describe_configs(
        self,
        config_resources: list[ConfigResource],
        include_synonyms: bool = False,
    ) -> list[Response]:
        pass

    async def alter_configs(
        self, config_resources: list[ConfigResource]
    ) -> list[Response]:
        pass





    async def create_partitions(
        self,
        topic_partitions: dict[str, NewPartitions],
        timeout_ms: int | None = None,
        validate_only: bool = False,
    ) -> Response:
        pass

    async def describe_consumer_groups(
        self,
        group_ids: list[str],
        group_coordinator_id: int | None = None,
        include_authorized_operations: bool = False,
    ) -> list[Response]:
        pass

    async def list_consumer_groups(
        self,
        broker_ids: list[int] | None = None,
    ) -> list[tuple[Any, ...]]:
        pass

    async def find_coordinator(self, group_id: str, coordinator_type: int = 0) -> int:
        pass

    async def list_consumer_group_offsets(
        self,
        group_id: str,
        group_coordinator_id: int | None = None,
        partitions: list[TopicPartition] | None = None,
    ) -> dict[TopicPartition, OffsetAndMetadata]:
        pass

    async def delete_records(
        self,
        records_to_delete: dict[TopicPartition, RecordsToDelete],
        timeout_ms: int | None = None,
    ) -> dict[TopicPartition, int]:
        pass

