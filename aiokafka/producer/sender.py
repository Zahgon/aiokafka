import asyncio
import collections
import logging

import aiokafka.errors as Errors
from aiokafka.client import ConnectionGroup, CoordinationType
from aiokafka.errors import (
    ConcurrentTransactions,
    CoordinatorLoadInProgressError,
    CoordinatorNotAvailableError,
    DuplicateSequenceNumber,
    GroupAuthorizationFailedError,
    IncompatibleBrokerVersion,
    InvalidProducerEpoch,
    InvalidProducerIdMapping,
    InvalidTxnState,
    KafkaError,
    NotCoordinatorError,
    OperationNotAttempted,
    OutOfOrderSequenceNumber,
    ProducerFenced,
    RequestTimedOutError,
    TopicAuthorizationFailedError,
    TransactionalIdAuthorizationFailed,
    UnknownTopicOrPartitionError,
)
from aiokafka.protocol.produce import ProduceRequest
from aiokafka.protocol.transaction import (
    AddOffsetsToTxnRequest,
    AddPartitionsToTxnRequest,
    EndTxnRequest,
    InitProducerIdRequest,
    TxnOffsetCommitRequest,
)
from aiokafka.structs import TopicPartition
from aiokafka.util import create_task

log = logging.getLogger(__name__)

BACKOFF_OVERRIDE = 0.02  # 20ms wait between transactions is better than 100ms.


class Sender:

    def __init__(
        self,
        client,
        *,
        acks,
        txn_manager,
        message_accumulator,
        retry_backoff_ms,
        request_timeout_ms,
    ):
        self.client = client
        self._txn_manager = txn_manager
        self._acks = acks

        self._message_accumulator = message_accumulator
        self._sender_task = None
        self._in_flight = set()
        self._muted_partitions = set()
        self._coordinators = {}
        self._retry_backoff = retry_backoff_ms / 1000
        self._request_timeout_ms = request_timeout_ms

    pass

    def _fail_all(self, task):
        pass

    pass

    async def close(self):
        if self._sender_task is not None and not self._sender_task.done():
            self._sender_task.cancel()
            await self._sender_task

    async def _sender_routine(self):
        pass

    pass

    pass

    pass

    pass


    async def _send_produce_req(self, node_id, batches):
        pass


    pass

    pass

    pass

    pass

    async def _do_txn_commit(self, commit_result):
        pass


class BaseHandler:
    group = ConnectionGroup.DEFAULT

    def __init__(self, sender):
        self._sender = sender
        self._default_backoff = sender._retry_backoff

    pass

    def create_request(self):
        raise NotImplementedError  # pragma: no cover

    def handle_response(self, response):
        raise NotImplementedError  # pragma: no cover

    def handle_error(self):
        raise NotImplementedError  # pragma: no cover


class InitPIDHandler(BaseHandler):
    pass

    pass

    pass


class AddPartitionsToTxnHandler(BaseHandler):
    group = ConnectionGroup.COORDINATION

    def __init__(self, sender, topic_partitions):
        super().__init__(sender)
        self._tps = topic_partitions

    pass

    pass

    pass


class AddOffsetsToTxnHandler(BaseHandler):
    group = ConnectionGroup.COORDINATION

    def __init__(self, sender, group_id):
        super().__init__(sender)
        self._group_id = group_id

    pass

    pass

    pass


class TxnOffsetCommitHandler(BaseHandler):
    group = ConnectionGroup.COORDINATION

    def __init__(self, sender, offsets, group_id):
        super().__init__(sender)
        self._offsets = offsets
        self._group_id = group_id

    pass

    pass

    pass


class EndTxnHandler(BaseHandler):
    group = ConnectionGroup.COORDINATION

    def __init__(self, sender, commit_result):
        super().__init__(sender)
        self._commit_result = commit_result

    pass

    pass

    pass


class SendProduceReqHandler(BaseHandler):
    def __init__(self, sender, batches):
        super().__init__(sender)
        self._batches = batches
        self._client = sender.client
        self._to_reenqueue = []

    pass

    pass

    pass

    pass

    pass
