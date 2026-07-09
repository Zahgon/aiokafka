import asyncio
import logging
import sys
import traceback
import warnings

from aiokafka.client import AIOKafkaClient
from aiokafka.codec import has_gzip, has_lz4, has_snappy, has_zstd
from aiokafka.errors import (
    IllegalOperation,
    MessageSizeTooLargeError,
)
from aiokafka.partitioner import DefaultPartitioner
from aiokafka.record.default_records import (
    DefaultRecordBatch,
    DefaultRecordBatchBuilder,
)
from aiokafka.structs import TopicPartition
from aiokafka.util import (
    INTEGER_MAX_VALUE,
    commit_structure_validate,
    create_task,
    get_running_loop,
)

from .message_accumulator import MessageAccumulator
from .sender import Sender
from .transaction_manager import TransactionManager

log = logging.getLogger(__name__)

_missing = object()


_DEFAULT_PARTITIONER = DefaultPartitioner()


class AIOKafkaProducer:

    _PRODUCER_CLIENT_ID_SEQUENCE = 0

    _COMPRESSORS = {
        "gzip": (has_gzip, DefaultRecordBatch.CODEC_GZIP),
        "snappy": (has_snappy, DefaultRecordBatch.CODEC_SNAPPY),
        "lz4": (has_lz4, DefaultRecordBatch.CODEC_LZ4),
        "zstd": (has_zstd, DefaultRecordBatch.CODEC_ZSTD),
    }

    _closed = None  # Serves as an uninitialized flag for __del__
    _source_traceback = None

    def __init__(
        self,
        *,
        loop=None,
        bootstrap_servers="localhost",
        client_id=None,
        metadata_max_age_ms=300000,
        request_timeout_ms=40000,
        api_version=None,
        acks=_missing,
        key_serializer=None,
        value_serializer=None,
        compression_type=None,
        max_batch_size=16384,
        partitioner=_DEFAULT_PARTITIONER,
        max_request_size=1048576,
        linger_ms=0,
        retry_backoff_ms=100,
        security_protocol="PLAINTEXT",
        ssl_context=None,
        connections_max_idle_ms=540000,
        enable_idempotence=False,
        transactional_id=None,
        transaction_timeout_ms=60000,
        sasl_mechanism="PLAIN",
        sasl_plain_password=None,
        sasl_plain_username=None,
        sasl_kerberos_service_name="kafka",
        sasl_kerberos_domain_name=None,
        sasl_oauth_token_provider=None,
    ):
        if loop is None:
            loop = get_running_loop()
        else:
            warnings.warn(
                "The `loop` parameter has been deprecated since 0.7.1 "
                "and will be removed in a future release.",
                DeprecationWarning,
                stacklevel=2,
            )
        if loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))
        self._loop = loop

        if api_version is not None:
            warnings.warn(
                "The `api_version` parameter has been deprecated since 0.13.0. "
                "It is now a no-op and will be removed in a future release. ",
                DeprecationWarning,
                stacklevel=2,
            )

        if acks not in (0, 1, -1, "all", _missing):
            raise ValueError("Invalid ACKS parameter")
        if compression_type not in ("gzip", "snappy", "lz4", "zstd", None):
            raise ValueError("Invalid compression type!")
        if compression_type:
            checker, compression_attrs = self._COMPRESSORS[compression_type]
            if not checker():
                raise RuntimeError(
                    f"Compression library for {compression_type} not found"
                )
        else:
            compression_attrs = 0

        if transactional_id is not None:
            enable_idempotence = True
        else:
            transaction_timeout_ms = INTEGER_MAX_VALUE

        if enable_idempotence:
            if acks is _missing:
                acks = -1
            elif acks not in ("all", -1):
                raise ValueError(
                    f"acks={acks} not supported if enable_idempotence=True"
                )
            self._txn_manager = TransactionManager(
                transactional_id, transaction_timeout_ms
            )
        else:
            self._txn_manager = None

        if acks is _missing:
            acks = 1
        elif acks == "all":
            acks = -1

        AIOKafkaProducer._PRODUCER_CLIENT_ID_SEQUENCE += 1
        if client_id is None:
            client_id = (
                f"aiokafka-producer-{AIOKafkaProducer._PRODUCER_CLIENT_ID_SEQUENCE}"
            )

        self._key_serializer = key_serializer
        self._value_serializer = value_serializer
        self._compression_type = compression_type
        self._partitioner = partitioner
        self._max_request_size = max_request_size
        self._request_timeout_ms = request_timeout_ms

        self.client = AIOKafkaClient(
            loop=loop,
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            metadata_max_age_ms=metadata_max_age_ms,
            request_timeout_ms=request_timeout_ms,
            retry_backoff_ms=retry_backoff_ms,
            security_protocol=security_protocol,
            ssl_context=ssl_context,
            connections_max_idle_ms=connections_max_idle_ms,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            sasl_kerberos_service_name=sasl_kerberos_service_name,
            sasl_kerberos_domain_name=sasl_kerberos_domain_name,
            sasl_oauth_token_provider=sasl_oauth_token_provider,
        )
        self._metadata = self.client.cluster
        self._message_accumulator = MessageAccumulator(
            self._metadata,
            max_batch_size,
            compression_attrs,
            self._request_timeout_ms / 1000,
            txn_manager=self._txn_manager,
            loop=loop,
            linger_ms=linger_ms,
        )
        self._sender = Sender(
            self.client,
            acks=acks,
            txn_manager=self._txn_manager,
            retry_backoff_ms=retry_backoff_ms,
            message_accumulator=self._message_accumulator,
            request_timeout_ms=request_timeout_ms,
        )

        self._closed = False

    def __del__(self, _warnings=warnings):
        if self._closed is False:
            _warnings.warn(
                f"Unclosed AIOKafkaProducer {self!r}",
                ResourceWarning,
                source=self,
            )
            context = {
                "producer": self,
                "message": "Unclosed AIOKafkaProducer",
            }
            if self._source_traceback is not None:
                context["source_traceback"] = self._source_traceback
            self._loop.call_exception_handler(context)

    async def start(self):
        pass

    async def flush(self):
        """Wait until all batches are Delivered and futures resolved"""
        await self._message_accumulator.flush()

    async def stop(self):
        pass

    async def partitions_for(self, topic):
        pass

    def _serialize(self, key, value, headers):
        if self._key_serializer is None:
            serialized_key = key
        else:
            serialized_key = self._key_serializer(key)
        if self._value_serializer is None:
            serialized_value = value
        else:
            serialized_value = self._value_serializer(value)

        message_size = DefaultRecordBatchBuilder.estimate_size_in_bytes(
            serialized_key, serialized_value, headers
        )
        if message_size > self._max_request_size:
            raise MessageSizeTooLargeError(
                f"The message is {message_size} bytes when serialized which is "
                "larger than the maximum request size you have configured with "
                "the max_request_size configuration"
            )

        return serialized_key, serialized_value

    def _partition(
        self, topic, partition, key, value, serialized_key, serialized_value
    ):
        if partition is not None:
            assert partition >= 0
            assert partition in self._metadata.partitions_for_topic(topic), (
                "Unrecognized partition"
            )
            return partition

        all_partitions = list(self._metadata.partitions_for_topic(topic))
        available = list(self._metadata.available_partitions_for_topic(topic))
        return self._partitioner(serialized_key, all_partitions, available)

    async def send(
        self,
        topic,
        value=None,
        key=None,
        partition=None,
        timestamp_ms=None,
        headers=None,
    ):
        """Publish a message to a topic.

        Arguments:
            topic (str): topic where the message will be published
            value (Optional): message value. Must be type :class:`bytes`, or be
                serializable to :class:`bytes` via configured `value_serializer`. If
                value is :data:`None`, key is required and message acts as a
                ``delete``.

                See `Kafka compaction documentation
                <https://kafka.apache.org/documentation.html#compaction>`__ for
                more details. (compaction requires kafka >= 0.8.1)
            partition (int, Optional): optionally specify a partition. If not
                set, the partition will be selected using the configured
                `partitioner`.
            key (Optional): a key to associate with the message. Can be used to
                determine which partition to send the message to. If partition
                is :data:`None` (and producer's partitioner config is left as default),
                then messages with the same key will be delivered to the same
                partition (but if key is :data:`None`, partition is chosen randomly).
                Must be type :class:`bytes`, or be serializable to bytes via configured
                `key_serializer`.
            timestamp_ms (int, Optional): epoch milliseconds (from Jan 1 1970
                UTC) to use as the message timestamp. Defaults to current time.
            headers (Optional): Kafka headers to be included in the message using
                the format ``[("key", b"value")]``. Iterable of tuples where key
                is a normal string and value is a byte string.

        Returns:
            asyncio.Future: object that will be set when message is
            processed

        Raises:
            ~aiokafka.errors.KafkaTimeoutError: if we can't schedule this record
                (pending buffer is full) in up to `request_timeout_ms`
                milliseconds.

        Note:
            The returned future will wait based on `request_timeout_ms`
            setting. Cancelling the returned future **will not** stop event
            from being sent, but cancelling the :meth:`send` coroutine itself
            **will**.
        """
        assert not (value is None and key is None), "Need at least one: key or value"

        await self.client._wait_on_metadata(topic)

        if self._txn_manager is not None:
            txn_manager = self._txn_manager
            if (
                txn_manager.transactional_id is not None
                and not self._txn_manager.is_in_transaction()
            ):
                raise IllegalOperation("Can't send messages while not in transaction")

        headers = headers or []

        key_bytes, value_bytes = self._serialize(key, value, headers)
        partition = self._partition(
            topic, partition, key, value, key_bytes, value_bytes
        )

        tp = TopicPartition(topic, partition)
        log.debug("Sending (key=%s value=%s) to %s", key, value, tp)

        fut = await self._message_accumulator.add_message(
            tp,
            key_bytes,
            value_bytes,
            self._request_timeout_ms / 1000,
            timestamp_ms=timestamp_ms,
            headers=headers,
        )
        return fut

    async def send_and_wait(
        self,
        topic,
        value=None,
        key=None,
        partition=None,
        timestamp_ms=None,
        headers=None,
    ):
        pass

    def create_batch(self):
        pass

    async def send_batch(self, batch, topic, *, partition):
        pass





    def transaction(self):
        pass


    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()


class TransactionContext:
    def __init__(self, producer):
        self._producer = producer

    async def __aenter__(self):
        await self._producer.begin_transaction()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            if self._producer._txn_manager.is_fatal_error():
                return
            await self._producer.abort_transaction()
        else:
            await self._producer.commit_transaction()
