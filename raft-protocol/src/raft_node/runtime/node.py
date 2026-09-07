import asyncio
import logging
import random
from collections.abc import Callable, Coroutine
from typing import Any

from raft_node.core import (
    AppendCommandResult,
    LogEntry,
    LogRequest,
    LogResponse,
    NodeSnapshot,
    NotLeaderError,
    RaftCore,
    Role,
    VoteRequest,
    VoteResponse,
)
from raft_node.runtime.errors import CommitTimeoutError, NodeStoppedError
from raft_node.transport import PeerTransport

LOGGER = logging.getLogger(__name__)
RandomSource = Callable[[float, float], float]


class RaftNode:
    """
    Asynchronous execution environment for a Raft protocol state machine.
    """

    def __init__(
        self,
        core: RaftCore,
        transport: PeerTransport,
        *,
        heartbeat_interval: float,
        election_timeout_range: tuple[float, float],
        client_commit_timeout: float,
        random_source: RandomSource = random.uniform,
    ) -> None:
        election_min, election_max = election_timeout_range

        if heartbeat_interval <= 0 or client_commit_timeout <= 0:
            raise ValueError("runtime intervals must be strictly positive")

        if election_min <= heartbeat_interval or election_min >= election_max:
            raise ValueError("election timeout range must be ordered and exceed heartbeat interval")

        self.core = core
        self.transport = transport
        self.heartbeat_interval = heartbeat_interval
        self.election_timeout_range = election_timeout_range
        self.client_commit_timeout = client_commit_timeout
        self.random_source = random_source

        self._condition = asyncio.Condition()
        self._election_reset = asyncio.Event()
        self._replication_events = {
            peer_id: asyncio.Event()
            for peer_id in core.state.members
            if peer_id != core.state.node_id
        }
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._rpc_tasks: set[asyncio.Task[None]] = set()
        self._election_generation = 0
        self._running = False

    @property
    def is_running(self) -> bool:
        """
        Return True if the node's background event loops are active.
        """

        return self._running

    async def start(self) -> None:
        if self._running:
            return

        async with self._condition:
            self._apply_committed_entries()
            self._running = True

        self._background_tasks = {
            asyncio.create_task(self._election_loop(), name="raft-election"),
            asyncio.create_task(self._heartbeat_loop(), name="raft-heartbeat"),
            *(
                asyncio.create_task(
                    self._replication_worker(peer_id),
                    name=f"raft-replication-{peer_id}"
                )
                for peer_id in self._replication_events
            )
        }

    async def stop(self) -> None:
        if not self._running:
            return

        async with self._condition:
            self._running = False
            self._condition.notify_all()

        self._election_reset.set()
        for event in self._replication_events.values():
            event.set()

        tasks = tuple(self._background_tasks | self._rpc_tasks)
        for task in tasks:
            task.cancel()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self._background_tasks.clear()
        self._rpc_tasks.clear()

        await self.transport.close()

    async def snapshot(self) -> NodeSnapshot:
        """
        Retrieve a consistent snapshot while holding the asyncio condition.
        """

        async with self._condition:
            return self.core.snapshot()

    async def submit_command(self, command: str) -> AppendCommandResult:
        """
        Append a command and wait until it is committed and applied, or leadership is lost.

        Raises:
            NodeStoppedError: If the node is shutting down.
            NotLeaderError: If the node is not the leader.
            CommitTimeoutError: If the command is not committed before the deadline.
        """

        if not self._running:
            raise NodeStoppedError("node runtime is not running")

        async with self._condition:
            result = self.core.append_command(command)
            self._apply_committed_entries()

            target_index = result.index
            target_entry = result.entry

            self._signal_replication()
            self._condition.notify_all()

            if self.core.state.applied_length > target_index:
                return result

        try:
            await asyncio.wait_for(
                self._wait_for_commit(target_index, target_entry),
                timeout=self.client_commit_timeout
            )
        except TimeoutError as error:
            raise CommitTimeoutError(target_index, target_entry.term) from error

        return result

    async def receive_vote_request(self, request: VoteRequest) -> VoteResponse:
        """
        Serialize an inbound RequestVote RPC with other state transitions.
        """

        async with self._condition:
            result = self.core.handle_vote_request(request)

            if result.reset_election_timer:
                self._reset_election_timeout()

            self._condition.notify_all()
            return result.response

    async def receive_log_request(self, request: LogRequest) -> LogResponse:
        """
        Serialize an inbound AppendEntries RPC with other state transitions.
        """

        async with self._condition:
            result = self.core.handle_log_request(request)
            self._apply_committed_entries()

            if result.reset_election_timer:
                self._reset_election_timeout()

            self._condition.notify_all()
            return result.response

    async def _wait_for_commit(self, index: int, entry: LogEntry) -> None:
        """
        Wait for this exact entry to be applied, or for leadership to be lost.
        """

        async with self._condition:
            while True:
                state = self.core.state

                if state.applied_length > index:
                    if index < len(state.log) and state.log[index] == entry:
                        return

                    raise NotLeaderError(state.leader_id)

                if not self._running:
                    raise NodeStoppedError("node stopped before the command committed")

                if state.role is not Role.LEADER or state.current_term != entry.term:
                    raise NotLeaderError(state.leader_id)

                await self._condition.wait()

    async def _election_loop(self) -> None:
        """
        Continuously monitor the heartbeat timeout to trigger elections.
        """

        while self._running:
            self._election_reset.clear()

            election_generation = self._election_generation
            timeout = self.random_source(*self.election_timeout_range)

            try:
                await asyncio.wait_for(self._election_reset.wait(), timeout=timeout)
            except TimeoutError:
                await self._start_election_round(election_generation)

    async def _start_election_round(self, election_generation: int) -> None:
        """
        Transition to candidate and fan out RequestVote RPCs to peers.
        """

        async with self._condition:
            if (
                not self._running
                or self.core.state.role is Role.LEADER
                or election_generation != self._election_generation
            ):
                return

            self._election_generation += 1

            requests = self.core.start_election()
            peers = tuple(self._replication_events)
            became_leader = self.core.state.leader_id == self.core.state.node_id

            if became_leader:
                self._signal_replication()

            self._condition.notify_all()

        for peer_id, request in zip(peers, requests, strict=True):
            self._spawn_rpc(self._request_vote(peer_id, request), f"raft-vote-{peer_id}")

    async def _request_vote(self, peer_id: str, request: VoteRequest) -> None:
        """
        Execute a single outgoing RequestVote RPC.
        """

        response = await self.transport.request_vote(peer_id, request)
        if response is None:
            return

        async with self._condition:
            result = self.core.handle_vote_response(response)

            if result.became_leader:
                LOGGER.info(f"node {self.core.state.node_id} became leader for term {self.core.state.current_term}")
                self._signal_replication()

            if result.stepped_down:
                self._reset_election_timeout()

            self._condition.notify_all()

    async def _heartbeat_loop(self) -> None:
        """
        Periodically awaken replication workers to broadcast heartbeats.
        """

        while self._running:
            await asyncio.sleep(self.heartbeat_interval)

            async with self._condition:
                if self.core.state.role is Role.LEADER:
                    self._signal_replication()

    async def _replication_worker(self, peer_id: str) -> None:
        """
        Manage outgoing AppendEntries RPCs for a single follower.
        """

        event = self._replication_events[peer_id]

        while self._running:
            await event.wait()
            event.clear()

            while self._running:
                async with self._condition:
                    if self.core.state.role is not Role.LEADER:
                        break

                    request = self.core.make_log_request(peer_id)

                response = await self.transport.append_entries(peer_id, request)
                if response is None:
                    break

                async with self._condition:
                    result = self.core.handle_log_response(response)
                    self._apply_committed_entries()

                    if result.stepped_down:
                        self._reset_election_timeout()

                    self._condition.notify_all()
                    retry = result.retry_peer == peer_id

                if not retry:
                    break

    def _apply_committed_entries(self) -> None:
        """
        Apply each committed command once, in order, while holding the condition.
        """

        state = self.core.state

        while state.applied_length < state.commit_length:
            entry = state.log[state.applied_length]

            state.applied_commands.append(entry.command)
            state.applied_length += 1

    def _signal_replication(self) -> None:
        """
        Awaken all replication workers to sync logs or broadcast heartbeats.
        """

        for event in self._replication_events.values():
            event.set()

    def _spawn_rpc(self, coroutine: Coroutine[Any, Any, None], name: str) -> None:
        """
        Fire and forget an outgoing RPC task safely.
        """

        if not self._running:
            coroutine.close()
            return

        task: asyncio.Task[None] = asyncio.create_task(coroutine, name=name)
        self._rpc_tasks.add(task)

        task.add_done_callback(self._rpc_tasks.discard)

    def _reset_election_timeout(self) -> None:
        """
        Invalidate the current election timer and initiate a fresh timeout cycle.
        """

        self._election_generation += 1
        self._election_reset.set()
