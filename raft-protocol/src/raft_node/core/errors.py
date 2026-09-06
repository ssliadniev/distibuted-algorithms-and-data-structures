class RaftError(Exception):
    """
    Base class for all internal Raft domain errors.
    """


class InvalidPeerError(RaftError):
    """
    Raised when a network operation targets a node outside the static cluster membership.
    """


class InvariantViolation(RaftError):
    """
    Raised when an internal state transition contradicts a strict Raft safety invariant.
    """


class NotLeaderError(RaftError):
    """
    Raised when a leader-only operation is attempted on a follower or candidate.

    Attributes:
        leader_id: The ID of the current leader if known, or None if the cluster
                   is currently without a leader or in an election phase.
    """

    def __init__(self, leader_id: str | None) -> None:
        self.leader_id = leader_id

        if leader_id:
            message = f"Node is not the leader. Current leader is {leader_id!r}."
        else:
            message = "Node is not the leader and the current leader is unknown."

        super().__init__(message)
