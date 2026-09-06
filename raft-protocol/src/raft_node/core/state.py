from dataclasses import dataclass, field

from raft_node.core.errors import InvariantViolation
from raft_node.core.models import LogEntry, NodeId, Role


@dataclass(slots=True)
class RaftState:
    node_id: NodeId
    members: tuple[NodeId, ...]
    current_term: int = 0
    voted_for: NodeId | None = None
    log: list[LogEntry] = field(default_factory=list)
    commit_length: int = 0
    role: Role = Role.FOLLOWER
    leader_id: NodeId | None = None
    votes_received: set[NodeId] = field(default_factory=set)
    sent_length: dict[NodeId, int] = field(default_factory=dict)
    acked_length: dict[NodeId, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.members = tuple(self.members)
        self.validate()

    @property
    def quorum_size(self) -> int:
        """
        Return the fixed majority size for the configured membership.
        """

        return len(self.members) // 2 + 1

    @property
    def last_log_term(self) -> int:
        """
        Return zero for an empty log.
        """

        return self.log[-1].term if self.log else 0

    def validate(self) -> None:
        """
        Fail fast when local state violates an internal Raft invariant.
        """

        if not self.node_id:
            raise InvariantViolation("node ID cannot be empty")

        if not self.members or len(set(self.members)) != len(self.members):
            raise InvariantViolation("cluster membership must be non-empty and unique")

        if self.node_id not in self.members:
            raise InvariantViolation("local node must be part of the cluster membership")

        if self.current_term < 0:
            raise InvariantViolation("current term cannot be negative")

        if self.voted_for is not None and self.voted_for not in self.members:
            raise InvariantViolation("vote must refer to a configured member")

        if not 0 <= self.commit_length <= len(self.log):
            raise InvariantViolation("commit length must be within the local log")

        if any(entry.term > self.current_term for entry in self.log):
            raise InvariantViolation("log entry term cannot exceed the current term")

        if any(left.term > right.term for left, right in zip(self.log, self.log[1:], strict=False)):
            raise InvariantViolation("log terms must be monotonically non-decreasing")

        if not self.votes_received.issubset(self.members):
            raise InvariantViolation("received votes must come from configured members")

        if self.role is Role.LEADER and self.leader_id != self.node_id:
            raise InvariantViolation("a leader must identify itself as current leader")

        if self.leader_id is not None and self.leader_id not in self.members:
            raise InvariantViolation("current leader must be a configured member")
