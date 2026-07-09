import collections
import logging
from collections.abc import Iterable, Mapping

from aiokafka.cluster import ClusterMetadata
from aiokafka.coordinator.assignors.abstract import AbstractPartitionAssignor
from aiokafka.coordinator.protocol import (
    ConsumerProtocolMemberAssignment,
    ConsumerProtocolMemberMetadata,
)

log = logging.getLogger(__name__)


class RangePartitionAssignor(AbstractPartitionAssignor):

    name = "range"
    version = 0



    @classmethod
    def on_assignment(cls, assignment: ConsumerProtocolMemberAssignment) -> None:
        pass
