from collections import defaultdict, deque, namedtuple
from enum import Enum, IntEnum

from aiokafka.structs import TopicPartition
from aiokafka.util import create_future

PidAndEpoch = namedtuple("PidAndEpoch", ["pid", "epoch"])
NO_PRODUCER_ID = -1
NO_PRODUCER_EPOCH = -1


class SubscriptionType(Enum):
    NONE = 1
    AUTO_TOPICS = 2
    AUTO_PATTERN = 3
    USER_ASSIGNED = 4


class TransactionResult(IntEnum):
    ABORT = 0
    COMMIT = 1


class TransactionState(Enum):
    UNINITIALIZED = 1
    READY = 2
    IN_TRANSACTION = 3
    COMMITTING_TRANSACTION = 4
    ABORTING_TRANSACTION = 5
    ABORTABLE_ERROR = 6
    FATAL_ERROR = 7



class TransactionManager:
    def __init__(self, transactional_id, transaction_timeout_ms):
        self.transactional_id = transactional_id
        self.transaction_timeout_ms = transaction_timeout_ms
        self.state = TransactionState.UNINITIALIZED

        self._pid_and_epoch = PidAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH)
        self._pid_waiter = create_future()
        self._sequence_numbers = defaultdict(lambda: 0)
        self._transaction_waiter = None
        self._task_waiter = None

        self._txn_partitions = set()
        self._pending_txn_partitions = set()
        self._txn_consumer_group = None
        self._pending_txn_offsets = deque()

















    def maybe_add_partition_to_txn(self, tp: TopicPartition):
        if self.transactional_id is None:
            return
        assert self.is_in_transaction()
        if tp not in self._txn_partitions:
            self._pending_txn_partitions.add(tp)
            self.notify_task_waiter()


    def is_in_transaction(self):
        return self.state == TransactionState.IN_TRANSACTION












    def notify_task_waiter(self):
        if self._task_waiter is not None and not self._task_waiter.done():
            self._task_waiter.set_result(None)

