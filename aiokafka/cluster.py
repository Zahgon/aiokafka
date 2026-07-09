import collections
import copy
import logging
import threading
import time
from concurrent.futures import Future

from aiokafka import errors as Errors
from aiokafka.conn import collect_hosts
from aiokafka.structs import BrokerMetadata, PartitionMetadata, TopicPartition

log = logging.getLogger(__name__)


class ClusterMetadata:

    DEFAULT_CONFIG = {
        "retry_backoff_ms": 100,
        "metadata_max_age_ms": 300000,
        "bootstrap_servers": [],
    }

    def __init__(self, **configs):
        self._brokers = {}  # node_id -> BrokerMetadata
        self._partitions = {}  # topic -> partition -> PartitionMetadata
        self._broker_partitions = collections.defaultdict(set)
        self._last_refresh_ms = 0
        self._last_successful_refresh_ms = 0
        self._need_update = True
        self._future = None
        self._listeners = set()
        self._lock = threading.Lock()
        self.need_all_topic_metadata = False
        self.unauthorized_topics = set()
        self.internal_topics = set()
        self.controller = None

        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        self._bootstrap_brokers = self._generate_bootstrap_brokers()



    def brokers(self):
        """Get all BrokerMetadata

        Returns:
            set: {BrokerMetadata, ...}
        """
        return set(self._brokers.values()) or set(self._bootstrap_brokers.values())

    def broker_metadata(self, broker_id):
        """Get BrokerMetadata

        Arguments:
            broker_id (int): node_id for a broker to check

        Returns:
            BrokerMetadata or None if not found
        """
        return self._brokers.get(broker_id) or self._bootstrap_brokers.get(broker_id)

    def partitions_for_topic(self, topic: str) -> set[int] | None:
        """Return set of all partitions for topic (whether available or not)

        Arguments:
            topic (str): topic to check for partitions

        Returns:
            set: {partition (int), ...}
        """
        if topic not in self._partitions:
            return None
        return set(self._partitions[topic].keys())

    def available_partitions_for_topic(self, topic):
        """Return set of partitions with known leaders

        Arguments:
            topic (str): topic to check for partitions

        Returns:
            set: {partition (int), ...}
            None if topic not found.
        """
        if topic not in self._partitions:
            return None
        return {
            partition
            for partition, metadata in self._partitions[topic].items()
            if metadata.leader != -1
        }

    def leader_for_partition(self, partition):
        pass

    def partitions_for_broker(self, broker_id):
        pass

    def request_update(self):
        pass

    def topics(self, exclude_internal_topics=True):
        pass

    def failed_update(self, exception):
        """Update cluster state given a failed MetadataRequest."""
        f = None
        with self._lock:
            if self._future:
                f = self._future
                self._future = None
        if f:
            f.set_exception(exception)
        self._last_refresh_ms = time.time() * 1000

    def update_metadata(self, metadata):
        """Update cluster state given a MetadataResponse.

        Arguments:
            metadata (MetadataResponse): broker response to a metadata request

        Returns: None
        """
        if not metadata.brokers:
            log.warning("No broker metadata found in MetadataResponse -- ignoring.")
            self.failed_update(Errors.MetadataEmptyBrokerList(metadata))
            return

        _new_brokers = {}
        for broker in metadata.brokers:
            if metadata.API_VERSION == 0:
                node_id, host, port = broker
                rack = None
            else:
                node_id, host, port, rack = broker
            _new_brokers.update({node_id: BrokerMetadata(node_id, host, port, rack)})

        if metadata.API_VERSION == 0:
            _new_controller = None
        else:
            _new_controller = _new_brokers.get(metadata.controller_id)

        _new_partitions = {}
        _new_broker_partitions = collections.defaultdict(set)
        _new_unauthorized_topics = set()
        _new_internal_topics = set()

        for topic_data in metadata.topics:
            if metadata.API_VERSION == 0:
                error_code, topic, partitions = topic_data
                is_internal = False
            else:
                error_code, topic, is_internal, partitions = topic_data
            if is_internal:
                _new_internal_topics.add(topic)
            error_type = Errors.for_code(error_code)
            if error_type is Errors.NoError:
                _new_partitions[topic] = {}
                for p_error, partition, leader, replicas, isr, *_ in partitions:
                    _new_partitions[topic][partition] = PartitionMetadata(
                        topic=topic,
                        partition=partition,
                        leader=leader,
                        replicas=replicas,
                        isr=isr,
                        error=p_error,
                    )
                    if leader != -1:
                        _new_broker_partitions[leader].add(
                            TopicPartition(topic, partition)
                        )

            elif self.need_all_topic_metadata:
                continue

            elif error_type is Errors.LeaderNotAvailableError:
                log.warning(
                    "Topic %s is not available during auto-create initialization",
                    topic,
                )
            elif error_type is Errors.UnknownTopicOrPartitionError:
                log.error("Topic %s not found in cluster metadata", topic)
            elif error_type is Errors.TopicAuthorizationFailedError:
                log.error("Topic %s is not authorized for this client", topic)
                _new_unauthorized_topics.add(topic)
            elif error_type is Errors.InvalidTopicError:
                log.error("'%s' is not a valid topic name", topic)
            else:
                log.error("Error fetching metadata for topic %s: %s", topic, error_type)

        with self._lock:
            self._brokers = _new_brokers
            self.controller = _new_controller
            self._partitions = _new_partitions
            self._broker_partitions = _new_broker_partitions
            self.unauthorized_topics = _new_unauthorized_topics
            self.internal_topics = _new_internal_topics
            f = None
            if self._future:
                f = self._future
            self._future = None
            self._need_update = False

        now = time.time() * 1000
        self._last_refresh_ms = now
        self._last_successful_refresh_ms = now

        if f:
            f.set_result(self)
        log.debug("Updated cluster metadata to %s", self)

        for listener in self._listeners:
            listener(self)

        if self.need_all_topic_metadata:
            self._need_update = False

    def add_listener(self, listener):
        pass

    def remove_listener(self, listener):
        pass

    def with_partitions(self, partitions_to_add):
        pass

    def __str__(self):
        return (
            f"ClusterMetadata(brokers: {len(self._brokers)}, topics: "
            f"{len(self._partitions)})"
        )
