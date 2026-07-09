import asyncio
import collections
import copy
import time
from collections.abc import Sequence

from aiokafka.errors import (
    KafkaTimeoutError,
    LeaderNotAvailableError,
    NotLeaderForPartitionError,
    ProducerClosed,
)
from aiokafka.record.default_records import DefaultRecordBatchBuilder
from aiokafka.structs import RecordMetadata
from aiokafka.util import create_future, get_running_loop


class BatchBuilder:
    def __init__(
        self,
        batch_size,
        compression_type,
        *,
        is_transactional=0,
        key_serializer=None,
        value_serializer=None,
    ):
        self._builder = DefaultRecordBatchBuilder(
            2,
            compression_type,
            is_transactional=is_transactional,
            producer_id=-1,
            producer_epoch=-1,
            base_sequence=0,
            batch_size=batch_size,
        )
        self._relative_offset = 0
        self._buffer = None
        self._closed = False
        self._key_serializer = key_serializer
        self._value_serializer = value_serializer

    def _serialize(self, key, value):
        if self._key_serializer is None:
            serialized_key = key
        else:
            serialized_key = self._key_serializer(key)
        if self._value_serializer is None:
            serialized_value = value
        else:
            serialized_value = self._value_serializer(value)

        return serialized_key, serialized_value

    def append(self, *, timestamp, key, value, headers: Sequence = []):
        """Add a message to the batch.

        Arguments:
            timestamp (float or None): epoch timestamp in seconds. If None,
                the timestamp will be set to the current time.
            key (bytes or None): the message key. `key` and `value` may not
                both be None.
            value (bytes or None): the message value. `key` and `value` may not
                both be None.

        Returns:
            If the message was successfully added, returns a metadata object
            with crc, offset, size, and timestamp fields. If the batch is full
            or closed, returns None.
        """
        if headers is None:
            headers = []
        if self._closed:
            return None

        key_bytes, value_bytes = self._serialize(key, value)
        metadata = self._builder.append(
            self._relative_offset,
            timestamp,
            key=key_bytes,
            value=value_bytes,
            headers=headers,
        )

        if metadata is None:
            self.close()
            return None

        self._relative_offset += 1
        return metadata

    def close(self):
        """Close the batch to further updates.

        Closing the batch before submitting to the producer ensures that no
        messages are added via the ``producer.send()`` interface. To gracefully
        support both the batch and individual message interfaces, leave the
        batch open. For complete control over the batch's contents, close
        before submission. Closing a batch has no effect on when it's sent to
        the broker.

        A batch may not be reopened after it's closed.
        """
        if self._closed:
            return
        self._closed = True



    def size(self):
        pass

    def record_count(self):
        pass

    def closed(self):
        pass


class MessageBatch:

    def __init__(self, tp, builder, ttl, linger_time):
        self._builder = builder
        self._tp = tp
        self._ttl = ttl
        self._linger_time = linger_time
        self._ctime = time.monotonic()

        self.future = create_future()
        self._msg_futures = []
        self._drain_waiter = create_future()
        self._retry_count = 0



    def append(
        self,
        key,
        value,
        timestamp_ms,
        _create_future=create_future,
        headers: Sequence = [],
    ):
        """Append message (key and value) to batch

        Returns:
            None if batch is full
              or
            asyncio.Future that will resolved when message is delivered
        """
        metadata = self._builder.append(
            timestamp=timestamp_ms, key=key, value=value, headers=headers
        )
        if metadata is None:
            return None

        future = _create_future()
        self._msg_futures.append((future, metadata))
        return future

    def done(
        self,
        base_offset,
        timestamp=None,
        log_start_offset=None,
        _record_metadata_class=RecordMetadata,
    ):
        """Resolve all pending futures"""
        tp = self._tp
        topic = tp.topic
        partition = tp.partition
        if timestamp == -1:
            timestamp_type = 0
        else:
            timestamp_type = 1

        if not self.future.done():
            self.future.set_result(
                _record_metadata_class(
                    topic,
                    partition,
                    tp,
                    base_offset,
                    timestamp,
                    timestamp_type,
                    log_start_offset,
                )
            )

        for future, metadata in self._msg_futures:
            if future.done():
                continue
            if timestamp == -1:
                timestamp = metadata.timestamp
            offset = base_offset + metadata.offset
            future.set_result(
                _record_metadata_class(
                    topic,
                    partition,
                    tp,
                    offset,
                    timestamp,
                    timestamp_type,
                    log_start_offset,
                )
            )

    def done_noack(self):
        pass


    async def wait_drain(self, timeout=None):
        """Wait until all message from this batch is processed"""
        waiter = self._drain_waiter
        await asyncio.wait([waiter], timeout=timeout)
        if waiter.done():
            waiter.result()  # Check for exception

    def remaining_linger(self):
        pass

    def expired(self):
        pass

    def drain_ready(self):
        pass

    def reset_drain(self):
        pass






class MessageAccumulator:

    def __init__(
        self,
        cluster,
        batch_size,
        compression_type,
        batch_ttl,
        *,
        txn_manager=None,
        loop=None,
        linger_ms=0,
    ):
        if loop is None:
            loop = get_running_loop()
        self._loop = loop
        self._batches = collections.defaultdict(collections.deque)
        self._pending_batches = set()
        self._cluster = cluster
        self._batch_size = batch_size
        self._compression_type = compression_type
        self._batch_ttl = batch_ttl
        self._waiter_future = loop.create_future()
        self._wakeup_handle = None
        self._closed = False
        self._txn_manager = txn_manager
        self._linger_time = linger_ms / 1000

        self._exception = None  # Critical exception

    async def flush(self):
        waiters = [
            batch.future for batches in self._batches.values() for batch in batches
        ]
        waiters += [batch.future for batch in self._pending_batches]
        if waiters:
            await asyncio.wait(waiters)



    async def close(self):
        self._closed = True
        await self.flush()

    async def add_message(
        self,
        tp,
        key,
        value,
        timeout,
        timestamp_ms=None,
        headers: Sequence = [],
    ):
        """Add message to batch by topic-partition
        If batch is already full this method waits (`timeout` seconds maximum)
        until batch is drained by send task
        """
        while True:
            if self._closed:
                raise ProducerClosed()
            if self._exception is not None:
                raise copy.copy(self._exception)

            pending_batches = self._batches.get(tp)
            if not pending_batches:
                builder = self.create_builder()
                batch = self._append_batch(builder, tp)
            else:
                batch = pending_batches[-1]

            future = batch.append(key, value, timestamp_ms, headers=headers)
            if future is not None:
                return future
            if not self._waiter_future.done():
                self._waiter_future.set_result(None)

            start = time.monotonic()
            await batch.wait_drain(timeout)
            timeout -= time.monotonic() - start
            if timeout <= 0:
                raise KafkaTimeoutError()

    def waiter(self):
        pass



    def drain_by_nodes(self, ignore_nodes, muted_partitions=frozenset()):
        pass


    def create_builder(self, key_serializer=None, value_serializer=None):
        is_transactional = False
        if (
            self._txn_manager is not None
            and self._txn_manager.transactional_id is not None
        ):
            is_transactional = True
        return BatchBuilder(
            self._batch_size,
            self._compression_type,
            is_transactional=is_transactional,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
        )

    def _append_batch(self, builder, tp):
        if self._txn_manager is not None:
            self._txn_manager.maybe_add_partition_to_txn(tp)

        batch = MessageBatch(tp, builder, self._batch_ttl, self._linger_time)
        self._batches[tp].append(batch)
        if not self._waiter_future.done():
            self._waiter_future.set_result(None)
        return batch

    async def add_batch(self, builder, tp, timeout):
        pass
