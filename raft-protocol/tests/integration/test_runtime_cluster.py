import asyncio
from collections.abc import Callable

import pytest
from raft_node.core import (LogRequest, LogResponse, RaftCore, Role,
                            VoteRequest, VoteResponse)
from raft_node.runtime import CommitTimeoutError, RaftNode

MEMBERS = ("node1", "node2", "node3")


class InMemoryNetwork:
    """
    A simulated network broker that can drop packets to simulate partitions.
    """

    def __init__(self) -> None:
        self.nodes: dict[str, RaftNode] = {}
        self.isolated: set[str] = set()

    def transport(self, source_id: str) -> "InMemoryTransport":
        """
        Create a transport endpoint bound to a specific node.
        """

        return InMemoryTransport(self, source_id)

    def add(self, node: RaftNode) -> None:
        """
        Register a node with the network switch.
        """

        self.nodes[node.core.state.node_id] = node

    def partition(self, node_id: str) -> None:
        """
        Sever a node's connection, causing all incoming and outgoing RPCs to drop.
        """

        self.isolated.add(node_id)

    def heal(self, node_id: str) -> None:
        """
        Restore a node's connection to the network.
        """

        self.isolated.discard(node_id)

    def can_deliver(self, source_id: str, target_id: str) -> bool:
        """
        Check if a message can safely traverse the simulated network.
        """

        return (
            source_id not in self.isolated
            and target_id not in self.isolated
            and target_id in self.nodes
        )


class InMemoryTransport:
    """
    A mock PeerTransport that routes messages through the InMemoryNetwork.
    """

    def __init__(self, network: InMemoryNetwork, source_id: str) -> None:
        self.network = network
        self.source_id = source_id
        self.closed = False

    async def request_vote(self, peer_id: str, request: VoteRequest) -> VoteResponse | None:
        """
        Deliver a RequestVote RPC if the network path is healthy.
        """

        await asyncio.sleep(0)
        if not self.network.can_deliver(self.source_id, peer_id):
            return None

        return await self.network.nodes[peer_id].receive_vote_request(request)

    async def append_entries(self, peer_id: str, request: LogRequest) -> LogResponse | None:
        """
        Deliver an AppendEntries RPC if the network path is healthy.
        """

        await asyncio.sleep(0)
        if not self.network.can_deliver(self.source_id, peer_id):
            return None

        return await self.network.nodes[peer_id].receive_log_request(request)

    async def close(self) -> None:
        self.closed = True


def fixed_timeout(value: float) -> Callable[[float, float], float]:
    """
    Return a mocked random source that always yields the exact same timeout.
    """

    def choose_timeout(_minimum: float, _maximum: float) -> float:
        return value

    return choose_timeout


def make_node(network: InMemoryNetwork, node_id: str, election_timeout: float) -> RaftNode:
    """
    Instantiate a RaftNode with deterministic election timeouts.
    """

    node = RaftNode(
        core=RaftCore.fresh(node_id, MEMBERS),
        transport=network.transport(node_id),
        heartbeat_interval=0.005,
        election_timeout_range=(0.02, 0.15),
        client_commit_timeout=0.05,
        random_source=fixed_timeout(election_timeout)
    )
    network.add(node)

    return node


async def wait_until(predicate: Callable[[], bool], timeout: float = 1.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_runtime_executes_the_complete_partition_and_recovery_scenario() -> None:
    """
    Run the assignment's acceptance scenario purely in-memory.
    """

    network = InMemoryNetwork()
    nodes: list[RaftNode] = []

    async def spawn_node(node_id: str, timeout: float) -> RaftNode:
        node = make_node(network, node_id, timeout)
        nodes.append(node)

        await node.start()
        return node

    try:
        node1 = await spawn_node("node1", 0.03)
        node2 = await spawn_node("node2", 0.07)

        await wait_until(lambda: node1.core.state.role is Role.LEADER)

        assert (await node1.submit_command("msg1")).entry.command == "msg1"
        assert (await node1.submit_command("msg2")).entry.command == "msg2"

        await wait_until(lambda: node2.core.state.commit_length == 2)

        node3 = await spawn_node("node3", 0.11)
        await wait_until(lambda: node3.core.state.commit_length == 2)

        old_term = node1.core.state.current_term
        network.partition("node1")

        await wait_until(lambda: node2.core.state.role is Role.LEADER)
        assert node2.core.state.current_term > old_term

        await node2.submit_command("msg3")
        await node2.submit_command("msg4")
        await wait_until(lambda: node3.core.state.commit_length == 4)

        with pytest.raises(CommitTimeoutError):
            await node1.submit_command("msg5")

        assert node1.core.state.commit_length == 2
        assert node1.core.state.log[-1].command == "msg5"

        network.heal("node1")
        await wait_until(
            lambda: (
                node1.core.state.role is Role.FOLLOWER
                and node1.core.state.commit_length == 4
            )
        )

        expected_commands = ["msg1", "msg2", "msg3", "msg4"]

        assert [entry.command for entry in node1.core.state.log] == expected_commands
        assert [entry.command for entry in node2.core.state.log] == expected_commands
        assert [entry.command for entry in node3.core.state.log] == expected_commands

    finally:
        await asyncio.gather(*(node.stop() for node in reversed(nodes)))


@pytest.mark.asyncio
async def test_start_and_stop_are_idempotent() -> None:
    """
    Verify that calling start/stop multiple times does not raise errors or leak tasks.
    """

    network = InMemoryNetwork()
    node = make_node(network, "node1", 0.03)

    await node.start()
    await node.start()
    assert node.is_running is True

    await node.stop()
    await node.stop()

    assert node.is_running is False
    assert isinstance(node.transport, InMemoryTransport)
    assert node.transport.closed is True
