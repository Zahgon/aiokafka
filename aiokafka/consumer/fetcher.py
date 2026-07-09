import asyncio
import collections
import contextlib
import logging
import random
import time
from itertools import chain

import async_timeout

import aiokafka.errors as Errors
from aiokafka.errors import ConsumerStoppedError, KafkaTimeoutError, RecordTooLargeError
from aiokafka.protocol.fetch import FetchRequest
from aiokafka.protocol.offset import OffsetRequest
from aiokafka.record.control_record import ABORT_MARKER, ControlRecord
from aiokafka.record.memory_records import MemoryRecords
from aiokafka.structs import ConsumerRecord, OffsetAndTimestamp, TopicPartition
from aiokafka.util import create_future, create_task

log = logging.getLogger(__name__)

UNKNOWN_OFFSET = -1

READ_UNCOMMITTED = 0
READ_COMMITTED = 1


class OffsetResetStrategy:
    LATEST = -1
    EARLIEST = -2
    NONE = 0




class FetchResult:
    def __init__(self, tp, *, assignment, partition_records, backoff):
        self._topic_partition = tp
        self._partition_records = partition_records

        self._created = time.monotonic()
        self._backoff = backoff

        self._assignment = assignment







    def __repr__(self):
        return f"<FetchResult position={self._partition_records.next_fetch_offset!r}>"


class FetchError:
    def __init__(self, *, error, backoff):
        self._error = error
        self._created = time.monotonic()
        self._backoff = backoff


    def check_raise(self):
        raise self._error

    def __repr__(self):
        return f"<FetchError error={self._error!r}>"


class PartitionRecords:
    def __init__(
        self,
        tp,
        records,
        aborted_transactions,
        fetch_offset,
        key_deserializer,
        value_deserializer,
        check_crcs,
        isolation_level,
    ):
        self._tp = tp
        self._records = records
        self._aborted_transactions = sorted(
            aborted_transactions or [], key=lambda x: x[1]
        )
        self._aborted_producers = set()
        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer
        self._check_crcs = check_crcs
        self._isolation_level = isolation_level

        self.next_fetch_offset = fetch_offset

        self._records_iterator = self._unpack_records()

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._records_iterator)
        except StopIteration:
            self._records_iterator = None
            raise






class Fetcher:

    def __init__(
        self,
        client,
        subscriptions,
        *,
        key_deserializer=None,
        value_deserializer=None,
        fetch_min_bytes=1,
        fetch_max_bytes=52428800,
        fetch_max_wait_ms=500,
        max_partition_fetch_bytes=1048576,
        check_crcs=True,
        fetcher_timeout=0.2,
        prefetch_backoff=0.1,
        retry_backoff_ms=100,
        auto_offset_reset="latest",
        isolation_level="read_uncommitted",
        client_rack=None,
    ):
        self._client = client
        self._loop = client._loop
        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer
        self._fetch_min_bytes = fetch_min_bytes
        self._fetch_max_bytes = fetch_max_bytes
        self._fetch_max_wait_ms = fetch_max_wait_ms
        self._max_partition_fetch_bytes = max_partition_fetch_bytes
        self._check_crcs = check_crcs
        self._fetcher_timeout = fetcher_timeout
        self._prefetch_backoff = prefetch_backoff
        self._retry_backoff = retry_backoff_ms / 1000
        self._subscriptions = subscriptions
        self._default_reset_strategy = OffsetResetStrategy.from_str(auto_offset_reset)
        self._client_rack = client_rack
        self._preferred_read_replica: dict[TopicPartition, tuple[int, float]] = {}
        self._preferred_replica_ttl = client._metadata_max_age_ms / 1000
        self._rack_warning_logged = False

        if isolation_level == "read_uncommitted":
            self._isolation_level = READ_UNCOMMITTED
        elif isolation_level == "read_committed":
            self._isolation_level = READ_COMMITTED
        else:
            raise ValueError(f"Incorrect isolation level {isolation_level}")

        self._records = collections.OrderedDict()
        self._in_flight = set()
        self._pending_tasks = set()

        self._wait_consume_future = None
        self._fetch_waiters = set()

        self._subscriptions.register_fetch_waiters(self._fetch_waiters)

        self._fetch_task = create_task(self._fetch_requests_routine())

        self._closed = False

    async def close(self):
        self._closed = True

        self._fetch_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._fetch_task

        for waiter in self._fetch_waiters:
            self._notify(waiter)

        for x in self._pending_tasks:
            x.cancel()
            await x

    def _notify(self, future):
        if future is not None and not future.done():
            future.set_result(None)



    async def _fetch_requests_routine(self):
        pass

    def _get_actions_per_node(self, assignment):
        pass

    def _select_read_replica(self, tp):
        pass

    def _update_preferred_read_replica(self, tp, preferred_read_replica: int):
        pass

    def _invalidate_preferred_read_replica_for_node(self, node_id, request):
        pass



    async def _update_fetch_positions(self, assignment, node_id, tps):
        pass

    async def _retrieve_offsets(self, timestamps, timeout_ms=None):
        pass

    async def _proc_offset_requests(self, timestamps):
        pass


    async def next_record(self, partitions):
        pass

    async def fetched_records(self, partitions, timeout=0, max_records=None):
        pass




    def request_offset_reset(self, tps, strategy):
        pass

    def seek_to(self, tp, offset):
        """Force a position change to specific offset. Called from
        `Consumer.seek()` API.
        """
        self._subscriptions.seek(tp, offset)
        if tp in self._records:
            del self._records[tp]

        self._notify(self._wait_consume_future)
