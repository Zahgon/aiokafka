import asyncio
import logging
import re
import sys
import traceback
import warnings

from aiokafka import __version__
from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.client import AIOKafkaClient
from aiokafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from aiokafka.errors import (
    ConsumerStoppedError,
    IllegalOperation,
    IllegalStateError,
    RecordTooLargeError,
)
from aiokafka.structs import ConsumerRecord, TopicPartition
from aiokafka.util import commit_structure_validate, get_running_loop

from .fetcher import Fetcher, OffsetResetStrategy
from .group_coordinator import GroupCoordinator, NoGroupCoordinator
from .subscription_state import SubscriptionState

log = logging.getLogger(__name__)


class AIOKafkaConsumer:

    _closed = None  # Serves as an uninitialized flag for __del__
    _source_traceback = None

    def __init__(
        self,
        *topics,
        loop=None,
        bootstrap_servers="localhost",
        client_id="aiokafka-" + __version__,
        group_id=None,
        group_instance_id=None,
        key_deserializer=None,
        value_deserializer=None,
        fetch_max_wait_ms=500,
        fetch_max_bytes=52428800,
        fetch_min_bytes=1,
        max_partition_fetch_bytes=1 * 1024 * 1024,
        request_timeout_ms=40 * 1000,
        retry_backoff_ms=100,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        check_crcs=True,
        metadata_max_age_ms=5 * 60 * 1000,
        partition_assignment_strategy=(RoundRobinPartitionAssignor,),
        max_poll_interval_ms=300000,
        rebalance_timeout_ms=None,
        session_timeout_ms=10000,
        heartbeat_interval_ms=3000,
        consumer_timeout_ms=200,
        max_poll_records=None,
        ssl_context=None,
        security_protocol="PLAINTEXT",
        api_version=None,
        exclude_internal_topics=True,
        connections_max_idle_ms=540000,
        isolation_level="read_uncommitted",
        sasl_mechanism="PLAIN",
        sasl_plain_password=None,
        sasl_plain_username=None,
        sasl_kerberos_service_name="kafka",
        sasl_kerberos_domain_name=None,
        sasl_oauth_token_provider=None,
        client_rack=None,
    ):
        if loop is None:
            loop = get_running_loop()
        else:
            warnings.warn(
                "The loop argument is deprecated since 0.7.1, "
                "and scheduled for removal in 0.9.0",
                DeprecationWarning,
                stacklevel=2,
            )

        if api_version is not None:
            warnings.warn(
                "The `api_version` parameter has been deprecated since 0.13.0. "
                "It is now a no-op and will be removed in a future release. ",
                DeprecationWarning,
                stacklevel=2,
            )

        if max_poll_records is not None and (
            not isinstance(max_poll_records, int) or max_poll_records < 1
        ):
            raise ValueError("`max_poll_records` should be positive Integer")

        if rebalance_timeout_ms is None:
            rebalance_timeout_ms = session_timeout_ms

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

        self._group_id = group_id
        self._group_instance_id = group_instance_id
        self._heartbeat_interval_ms = heartbeat_interval_ms
        self._session_timeout_ms = session_timeout_ms
        self._retry_backoff_ms = retry_backoff_ms
        self._auto_offset_reset = auto_offset_reset
        self._request_timeout_ms = request_timeout_ms
        self._enable_auto_commit = enable_auto_commit
        self._auto_commit_interval_ms = auto_commit_interval_ms
        self._partition_assignment_strategy = partition_assignment_strategy
        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer
        self._fetch_min_bytes = fetch_min_bytes
        self._fetch_max_bytes = fetch_max_bytes
        self._fetch_max_wait_ms = fetch_max_wait_ms
        self._max_partition_fetch_bytes = max_partition_fetch_bytes
        self._exclude_internal_topics = exclude_internal_topics
        self._max_poll_records = max_poll_records
        self._consumer_timeout = consumer_timeout_ms / 1000
        self._isolation_level = isolation_level
        self._rebalance_timeout_ms = rebalance_timeout_ms
        self._max_poll_interval_ms = max_poll_interval_ms
        self._client_rack = client_rack

        self._check_crcs = check_crcs
        self._subscription = SubscriptionState(loop=loop)
        self._fetcher = None
        self._coordinator = None
        self._loop = loop

        if loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))
        self._closed = False

        if topics:
            topics = self._validate_topics(topics)
            self._client.set_topics(topics)
            self._subscription.subscribe(topics=topics)

    def __del__(self, _warnings=warnings):
        if self._closed is False:
            _warnings.warn(
                f"Unclosed AIOKafkaConsumer {self!r}",
                ResourceWarning,
                source=self,
            )
            context = {
                "consumer": self,
                "message": "Unclosed AIOKafkaConsumer",
            }
            if self._source_traceback is not None:
                context["source_traceback"] = self._source_traceback
            self._loop.call_exception_handler(context)

    async def start(self):
        pass



    def assign(self, partitions):
        pass

    def assignment(self):
        pass

    async def stop(self):
        pass

    async def commit(self, offsets=None):
        pass

    async def committed(self, partition):
        pass

    async def topics(self):
        pass

    def partitions_for_topic(self, topic):
        """Get metadata about the partitions for a given topic.

        This method will return `None` if Consumer does not already have
        metadata for this topic.

        Arguments:
            topic (str): topic to check

        Returns:
            set: partition ids
        """
        return self._client.cluster.partitions_for_topic(topic)

    async def position(self, partition):
        pass

    def highwater(self, partition):
        pass

    def last_stable_offset(self, partition):
        pass

    def last_poll_timestamp(self, partition):
        pass

    def seek(self, partition, offset):
        """Manually specify the fetch offset for a :class:`.TopicPartition`.

        Overrides the fetch offsets that the consumer will use on the next
        :meth:`getmany`/:meth:`getone` call. If this API is invoked for the same
        partition more than once, the latest offset will be used on the next
        fetch.

        Note:
            You may lose data if this API is arbitrarily used in the middle
            of consumption to reset the fetch offsets. Use it either on
            rebalance listeners or after all pending messages are processed.

        Arguments:
            partition (TopicPartition): partition for seek operation
            offset (int): message offset in partition

        Raises:
            ValueError: if offset is not a positive integer
            IllegalStateError: partition is not currently assigned

        .. versionchanged:: 0.4.0

            Changed :exc:`AssertionError` to
            :exc:`~aiokafka.errors.IllegalStateError` and :exc:`ValueError` in
            respective cases.
        """
        if not isinstance(offset, int) or offset < 0:
            raise ValueError("Offset must be a positive integer")
        log.debug("Seeking to offset %s for partition %s", offset, partition)
        self._fetcher.seek_to(partition, offset)

    async def seek_to_beginning(self, *partitions):
        pass

    async def seek_to_end(self, *partitions):
        pass

    async def seek_to_committed(self, *partitions):
        pass

    async def offsets_for_times(self, timestamps):
        pass

    async def beginning_offsets(self, partitions):
        pass

    async def end_offsets(self, partitions):
        pass

    def subscribe(self, topics=(), pattern=None, listener=None):
        pass

    def subscription(self):
        pass

    def unsubscribe(self):
        pass

    async def getone(self, *partitions) -> ConsumerRecord:
        pass

    async def getmany(
        self, *partitions, timeout_ms=0, max_records=None
    ) -> dict[TopicPartition, list[ConsumerRecord]]:
        pass

    def pause(self, *partitions):
        pass

    def paused(self):
        pass

    def resume(self, *partitions):
        pass

    def __aiter__(self):
        if self._closed:
            raise ConsumerStoppedError()
        return self

    async def __anext__(self) -> ConsumerRecord:
        """Asyncio iterator interface for consumer

        Note:
            TopicAuthorizationFailedError and OffsetOutOfRangeError
            exceptions can be raised in iterator.
            All other KafkaError exceptions will be logged and not raised
        """
        while True:
            try:
                return await self.getone()
            except ConsumerStoppedError:  # noqa: PERF203
                raise StopAsyncIteration from None
            except RecordTooLargeError:
                log.exception("error in consumer iterator: %s")

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()
