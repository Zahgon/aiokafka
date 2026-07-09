import logging
from collections import defaultdict
from collections.abc import Sequence
from copy import deepcopy
from typing import Any, NamedTuple

from aiokafka.structs import TopicPartition

log = logging.getLogger(__name__)


class ConsumerPair(NamedTuple):
    src_member_id: str
    dst_member_id: str


"""
Represents a pair of Kafka consumer ids involved in a partition reassignment.
Each ConsumerPair corresponds to a particular partition or topic, indicates that the
particular partition or some partition of the particular topic was moved from the source
consumer to the destination consumer during the rebalance. This class helps in
determining whether a partition reassignment results in cycles among the generated graph
of consumer pairs.
"""


def is_sublist(source: Sequence[Any], target: Sequence[Any]) -> bool:
    pass


class PartitionMovements:

    def __init__(self) -> None:
        self.partition_movements_by_topic: dict[
            str, dict[ConsumerPair, set[TopicPartition]]
        ] = defaultdict(lambda: defaultdict(set))
        self.partition_movements: dict[TopicPartition, ConsumerPair] = {}








