from __future__ import annotations

import contextlib
import copy
import logging
import time
from asyncio import Event, shield
from collections.abc import Iterable
from enum import Enum
from re import Pattern

from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.errors import IllegalStateError
from aiokafka.structs import OffsetAndMetadata, TopicPartition
from aiokafka.util import create_future, get_running_loop

log = logging.getLogger(__name__)


class SubscriptionType(Enum):
    NONE = 1
    AUTO_TOPICS = 2
    AUTO_PATTERN = 3
    USER_ASSIGNED = 4


class SubscriptionState:

    _subscription_type = SubscriptionType.NONE  # type: SubscriptionType
    _subscribed_pattern = None  # type: str
    _subscription = None  # type: Subscription
    _listener = None  # type: ConsumerRebalanceListener

    def __init__(self, loop=None):
        if loop is None:
            loop = get_running_loop()
        self._loop = loop

        self._subscription_waiters = []  # type: List[Future]
        self._assignment_waiters = []  # type: List[Future]

        self._fetch_count = 0
        self._last_fetch_ended = time.monotonic()



    @property
    def listener(self) -> ConsumerRebalanceListener:
        return self._listener








    def _assigned_state(self, tp: TopicPartition) -> TopicPartitionState:
        assert self._subscription is not None
        assert self._subscription.assignment is not None
        tp_state = self._subscription.assignment.state_value(tp)
        if tp_state is None:
            raise IllegalStateError(f"No current assignment for partition {tp}")
        return tp_state




    def subscribe(self, topics: set[str], listener=None):
        pass

    def subscribe_pattern(self, pattern: Pattern, listener=None):
        pass

    def assign_from_user(self, partitions: Iterable[TopicPartition]):
        pass

    def unsubscribe(self):
        pass


    def subscribe_from_pattern(self, topics: set[str]):
        pass

    def assign_from_subscribed(self, assignment: set[TopicPartition]):
        pass

    def begin_reassignment(self):
        pass


    def seek(self, tp: TopicPartition, offset: int):
        """Force reset of position to the specified offset.

        Caller: Consumer, Fetcher
        Affects: TopicPartitionState.position
        """
        self._assigned_state(tp).seek(offset)


    def wait_for_subscription(self):
        pass

    def wait_for_assignment(self):
        pass


    def abort_waiters(self, exc):
        pass







    @property
    def fetcher_idle_time(self):
        pass


class Subscription:

    def __init__(self, topics: Iterable[str], loop=None):
        if loop is None:
            loop = get_running_loop()

        self._topics = frozenset(topics)  # type: FrozenSet[str]
        self._assignment = None  # type: Assignment
        self.unsubscribe_future = loop.create_future()  # type: Future
        self._reassignment_in_progress = True








class ManualSubscription(Subscription):

    def __init__(self, user_assignment: Iterable[TopicPartition], loop=None):
        topics = (tp.topic for tp in user_assignment)
        super().__init__(topics, loop=loop)
        self._assignment = Assignment(user_assignment)

    def _assign(self, topic_partitions: set[TopicPartition]):  # pragma: no cover
        raise AssertionError("Should not be called")


    @_reassignment_in_progress.setter
    def _reassignment_in_progress(self, value):
        pass

    def _begin_reassignment(self):  # pragma: no cover
        raise AssertionError("Should not be called")


class Assignment:

    def __init__(self, topic_partitions: Iterable[TopicPartition]):
        assert isinstance(topic_partitions, list | set | tuple)

        self._topic_partitions = frozenset(topic_partitions)

        self._tp_state = {}  # type: Dict[TopicPartition, TopicPartitionState]
        for tp in self._topic_partitions:
            self._tp_state[tp] = TopicPartitionState(self)

        self.unassign_future = create_future()
        self.commit_refresh_needed = Event()




    def state_value(self, tp: TopicPartition) -> TopicPartitionState:
        return self._tp_state.get(tp)

    def all_consumed_offsets(self) -> dict[TopicPartition, OffsetAndMetadata]:
        pass

    def requesting_committed(self):
        pass


class PartitionStatus(Enum):
    AWAITING_RESET = 0
    CONSUMING = 1
    UNASSIGNED = 2


class TopicPartitionState:

    def __init__(self, assignment):
        self._committed_futs = []

        self.highwater = None  # Last fetched highwater mark
        self.lso = None  # Last fetched stable offset mark
        self.timestamp = None  # timestamp of last poll
        self._position = None  # The current position of the topic
        self._position_fut = create_future()

        self._reset_strategy = None  # type: int
        self._status = PartitionStatus.AWAITING_RESET  # type: PartitionStatus

        self._assignment = assignment

        self._paused = False
        self._resume_fut = None







    def await_reset(self, strategy):
        pass



    def update_committed(self, offset_meta: OffsetAndMetadata):
        pass


    def consumed_to(self, position: int):
        pass

    def reset_to(self, position: int):
        pass

    def seek(self, position: int):
        """Called by Consumer to force position to a specific offset"""
        self._position = position
        self._reset_strategy = None
        self._status = PartitionStatus.CONSUMING
        if not self._position_fut.done():
            self._position_fut.set_result(None)




    def __repr__(self):
        return f"TopicPartitionState<Status={self._status} position={self._position}>"
