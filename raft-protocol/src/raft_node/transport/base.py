from typing import Protocol

from raft_node.core import LogRequest, LogResponse, VoteRequest, VoteResponse


class PeerTransport(Protocol):
    """
    Deliver Raft RPCs to statically configured peers.
    """

    async def request_vote(self, peer_id: str, request: VoteRequest) -> VoteResponse | None:
        """
        Send a RequestVote RPC to a specific peer.

        Args:
            peer_id: The target node's identifier.
            request: The election vote request payload.

        Returns:
            The peer's vote response or None if the request failed.
        """

    async def append_entries(self, peer_id: str, request: LogRequest) -> LogResponse | None:
        """
        Send an AppendEntries RPC to a specific peer.

        Args:
            peer_id: The target node's identifier.
            request: The log replication or heartbeat payload.

        Returns:
            The peer's replication response or None if the request failed.
        """

    async def close(self) -> None:
        """
        Release underlying network resources gracefully.
        """
