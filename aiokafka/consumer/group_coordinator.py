import asyncio
import collections
import copy
import logging
import time

import aiokafka.errors as Errors
from aiokafka.client import ConnectionGroup, CoordinationType
from aiokafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from aiokafka.coordinator.protocol import ConsumerProtocol
from aiokafka.protocol.api import Response
from aiokafka.protocol.commit import OffsetCommitRequest, OffsetFetchRequest
from aiokafka.protocol.group import (
    HeartbeatRequest,
    JoinGroupRequest,
    LeaveGroupRequest,
    SyncGroupRequest,
)
from aiokafka.structs import OffsetAndMetadata, TopicPartition
from aiokafka.util import create_future, create_task

log = logging.getLogger(__name__)

UNKNOWN_OFFSET = -1


class BaseCoordinator:
    def __init__(
        self,
        client,
        subscription,
        *,
        exclude_internal_topics=True,
    ):
        self._client = client
        self._exclude_internal_topics = exclude_internal_topics
        self._subscription = subscription

        self._metadata_snapshot = {}  # Is updated by metadata listener
        self._cluster = client.cluster

        self._handle_metadata_update(self._cluster)
        self._cluster.add_listener(self._handle_metadata_update)




class NoGroupCoordinator(BaseCoordinator):

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self._reset_committed_task = create_task(self._reset_committed_routine())


    def assign_all_partitions(self, check_unknown=False):
        pass

    async def _reset_committed_routine(self):
        pass


    async def close(self):
        self._reset_committed_task.cancel()
        await self._reset_committed_task
        self._reset_committed_task = None



class GroupCoordinator(BaseCoordinator):

    def __init__(
        self,
        client,
        subscription,
        *,
        group_id="aiokafka-default-group",
        group_instance_id=None,
        session_timeout_ms=10000,
        heartbeat_interval_ms=3000,
        retry_backoff_ms=100,
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        assignors=(RoundRobinPartitionAssignor,),
        exclude_internal_topics=True,
        max_poll_interval_ms=300000,
        rebalance_timeout_ms=30000,
    ):
        """Initialize the coordination manager.

        Parameters (see AIOKafkaConsumer)
        """
        self._group_subscription = None

        super().__init__(
            client,
            subscription,
            exclude_internal_topics=exclude_internal_topics,
        )

        self._session_timeout_ms = session_timeout_ms
        self._heartbeat_interval_ms = heartbeat_interval_ms
        self._max_poll_interval = max_poll_interval_ms / 1000
        self._rebalance_timeout_ms = rebalance_timeout_ms
        self._retry_backoff_ms = retry_backoff_ms
        self._assignors = assignors
        self._enable_auto_commit = enable_auto_commit
        self._auto_commit_interval_ms = auto_commit_interval_ms

        self.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID
        self.member_id = JoinGroupRequest.UNKNOWN_MEMBER_ID
        self.group_id = group_id
        self._group_instance_id = group_instance_id
        self.coordinator_id = None

        self._performed_join_prepare = False
        self._rejoin_needed_fut = create_future()
        self._coordinator_dead_fut = create_future()

        self._coordination_task = create_task(self._coordination_routine())

        self._heartbeat_task = None
        self._commit_refresh_task = None

        self._pending_exception = None
        self._error_consumed_fut = None

        self._coordinator_lookup_lock = asyncio.Lock()
        self._commit_lock = asyncio.Lock()

        self._next_autocommit_deadline = (
            time.monotonic() + auto_commit_interval_ms / 1000
        )

        self._closing = create_future()


    async def _send_req(self, request):
        """Send request to coordinator node. In case the coordinator is not
        ready a respective error will be raised.
        """
        node_id = self.coordinator_id
        if node_id is None:
            raise Errors.GroupCoordinatorNotAvailableError()
        try:
            resp = await self._client.send(
                node_id, request, group=ConnectionGroup.COORDINATION
            )
        except Errors.KafkaError as err:
            log.error(
                "Error sending %s to node %s [%s] -- marking coordinator dead",
                request.__class__.__name__,
                node_id,
                err,
            )
            self.coordinator_dead()
            raise
        return resp

    def check_errors(self):
        pass

    def _push_error_to_user(self, exc):
        pass

    async def close(self):
        """Close the coordinator, leave the current group
        and reset local generation/memberId."""
        if self._closing.done():
            return

        self._closing.set_result(None)
        if not self._coordination_task.done():
            await self._coordination_task
        await self._stop_heartbeat_task()
        await self._stop_commit_offsets_refresh_task()

        await self._maybe_leave_group()


    async def _maybe_leave_group(self):
        if self.generation > 0 and self._group_instance_id is None:
            request = LeaveGroupRequest(self.group_id, self.member_id)
            try:
                await self._send_req(request)
            except Errors.KafkaError as err:
                log.error("LeaveGroup request failed: %s", err)
            else:
                log.info("LeaveGroup request succeeded")
        self.reset_generation()





    def coordinator_dead(self):
        """Mark the current coordinator as dead.
        NOTE: this will not force a group rejoin. If new coordinator is able to
        recognize this member we will just continue with current generation.
        """
        if self.coordinator_id is not None:
            log.warning(
                "Marking the coordinator dead (node %s)for group %s.",
                self.coordinator_id,
                self.group_id,
            )
            self.coordinator_id = None
            self._coordinator_dead_fut.set_result(None)

    def reset_generation(self):
        """Coordinator did not recognize either generation or member_id. Will
        need to re-join the group.
        """
        self.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID
        self.member_id = JoinGroupRequest.UNKNOWN_MEMBER_ID
        self.request_rejoin()

    def request_rejoin(self):
        if not self._rejoin_needed_fut.done():
            self._rejoin_needed_fut.set_result(None)

    def need_rejoin(self, subscription):
        pass

    async def ensure_coordinator_known(self):
        pass


    async def __coordination_routine(self):
        pass



    async def _stop_heartbeat_task(self):
        if self._heartbeat_task is not None:
            if not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
                await self._heartbeat_task
            self._heartbeat_task = None




    async def _stop_commit_offsets_refresh_task(self):
        if self._commit_refresh_task is not None:
            if not self._commit_refresh_task.done():
                self._commit_refresh_task.cancel()
                await self._commit_refresh_task
            self._commit_refresh_task = None

    async def _commit_refresh_routine(self, assignment):
        pass





    async def commit_offsets(self, assignment, offsets):
        pass



    async def fetch_committed_offsets(self, partitions):
        pass



class CoordinatorGroupRebalance:

    def __init__(
        self,
        coordinator,
        group_id,
        coordinator_id,
        subscription,
        assignors,
        session_timeout_ms,
        retry_backoff_ms,
    ):
        self._coordinator = coordinator
        self.group_id = group_id
        self.coordinator_id = coordinator_id

        self._subscription = subscription
        self._assignors = assignors
        self._session_timeout_ms = session_timeout_ms
        self._retry_backoff_ms = retry_backoff_ms
        self._rebalance_timeout_ms = self._coordinator._rebalance_timeout_ms

    async def perform_group_join(self):
        pass


    async def _on_join_leader(self, response):
        pass

