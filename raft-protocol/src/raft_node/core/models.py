from dataclasses import dataclass
from enum import StrEnum

type NodeId = str


class Role(StrEnum):
    """
    A Raft node's current consensus role.
    """

    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass(frozen=True, slots=True)
class LogEntry:
    """
    One state machine command and the leader term in which it was appended.
    """

    command: str
    term: int

    def __post_init__(self) -> None:
        if self.term < 0:
            raise ValueError("log entry term cannot be negative")


@dataclass(frozen=True, slots=True)
class VoteRequest:
    """
    RequestVote RPC payload requesting a vote from a peer.
    """

    candidate_id: NodeId
    term: int
    log_length: int
    last_log_term: int

    def __post_init__(self) -> None:
        if self.term < 0 or self.log_length < 0 or self.last_log_term < 0:
            raise ValueError("vote request counters cannot be negative")


@dataclass(frozen=True, slots=True)
class VoteResponse:
    """
    Response payload to a RequestVote RPC.
    """

    voter_id: NodeId
    term: int
    granted: bool

    def __post_init__(self) -> None:
        if self.term < 0:
            raise ValueError("vote response term cannot be negative")


@dataclass(frozen=True, slots=True)
class LogRequest:
    """
    AppendEntries RPC payload for replicating logs and broadcasting heartbeats.
    """

    leader_id: NodeId
    term: int
    prefix_length: int
    prefix_term: int
    leader_commit: int
    entries: tuple[LogEntry, ...] = ()

    def __post_init__(self) -> None:
        if self.term < 0 or self.prefix_length < 0 or self.prefix_term < 0 or self.leader_commit < 0:
            raise ValueError("log request counters cannot be negative")


@dataclass(frozen=True, slots=True)
class LogResponse:
    """
    Response payload to an AppendEntries RPC.
    """

    follower_id: NodeId
    term: int
    acknowledged_length: int
    success: bool

    def __post_init__(self) -> None:
        if self.term < 0 or self.acknowledged_length < 0:
            raise ValueError("log response counters cannot be negative")


@dataclass(frozen=True, slots=True)
class VoteRequestResult:
    """
    The local state changes and response required after handling a VoteRequest.
    """

    response: VoteResponse
    reset_election_timer: bool


@dataclass(frozen=True, slots=True)
class VoteResponseResult:
    """
    The local state changes triggered by receiving a VoteResponse.
    """

    became_leader: bool = False
    stepped_down: bool = False


@dataclass(frozen=True, slots=True)
class LogRequestResult:
    """
    The local state changes and response required after handling a LogRequest.
    """

    response: LogResponse
    reset_election_timer: bool
    committed_entries: tuple[LogEntry, ...] = ()


@dataclass(frozen=True, slots=True)
class LogResponseResult:
    """
    The local state changes triggered by receiving a LogResponse.
    """

    retry_peer: NodeId | None = None
    stepped_down: bool = False
    committed_entries: tuple[LogEntry, ...] = ()


@dataclass(frozen=True, slots=True)
class AppendCommandResult:
    """
    The outcome of successfully appending a client command to the leader's log.
    """

    index: int
    entry: LogEntry
    committed_entries: tuple[LogEntry, ...] = ()


@dataclass(frozen=True, slots=True)
class NodeSnapshot:
    """
    Immutable point-in-time snapshot of the node's internal state machine.
    """

    node_id: NodeId
    members: tuple[NodeId, ...]
    role: Role
    current_term: int
    voted_for: NodeId | None
    leader_id: NodeId | None
    commit_length: int
    log: tuple[LogEntry, ...]
