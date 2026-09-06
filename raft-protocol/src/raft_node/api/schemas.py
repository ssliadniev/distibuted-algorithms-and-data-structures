from typing import Self

from pydantic import BaseModel, ConfigDict, Field

from raft_node.core import (LogEntry, LogRequest, LogResponse, VoteRequest,
                            VoteResponse)


class CommandRequestBody(BaseModel):
    """
    Client request to append a new command to the state machine.
    """

    model_config = ConfigDict(frozen=True)

    command: str = Field(min_length=1, description="The state machine command to execute.")


class CommandResponseBody(BaseModel):
    """
    Result of a successfully committed client command.
    """

    model_config = ConfigDict(frozen=True)

    index: int = Field(description="The log index where the command was appended.")
    term: int = Field(description="The leader's term when the command was appended.")
    command: str = Field(description="The executed command string.")
    committed: bool = Field(description="Indicates whether the command reached a quorum.")


class StatusLogEntry(BaseModel):
    """
    Representation of a single log entry for status reporting.
    """

    model_config = ConfigDict(frozen=True)

    index: int
    term: int
    command: str
    committed: bool


class NodeStatusBody(BaseModel):
    """
    Comprehensive snapshot of a Raft node's current state.
    """

    model_config = ConfigDict(frozen=True)

    node_id: str
    members: list[str]
    role: str
    current_term: int
    voted_for: str | None
    leader_id: str | None
    commit_length: int
    log: list[StatusLogEntry]


class HealthBody(BaseModel):
    model_config = ConfigDict(frozen=True)

    node_id: str
    status: str


class LogEntryBody(BaseModel):
    """
    Wire representation of a Raft log entry.
    """

    model_config = ConfigDict(frozen=True)

    command: str
    term: int = Field(ge=0)

    def to_domain(self) -> LogEntry:
        return LogEntry(command=self.command, term=self.term)

    @classmethod
    def from_domain(cls, entry: LogEntry) -> Self:
        return cls(command=entry.command, term=entry.term)


class VoteRequestBody(BaseModel):
    """
    Payload for a RequestVote RPC.
    """

    model_config = ConfigDict(frozen=True)

    candidate_id: str = Field(description="The node ID requesting the vote.")
    term: int = Field(ge=0, description="The candidate's current term.")
    log_length: int = Field(ge=0, description="The length of the candidate's log.")
    last_log_term: int = Field(ge=0, description="The term of the candidate's last log entry.")

    def to_domain(self) -> VoteRequest:
        return VoteRequest(
            candidate_id=self.candidate_id,
            term=self.term,
            log_length=self.log_length,
            last_log_term=self.last_log_term,
        )


class VoteResponseBody(BaseModel):
    """
    Response payload for a RequestVote RPC.
    """

    model_config = ConfigDict(frozen=True)

    voter_id: str = Field(description="The node ID casting the vote.")
    term: int = Field(description="The voter's current term.")
    granted: bool = Field(description="True if the candidate received the vote.")

    @classmethod
    def from_domain(cls, response: VoteResponse) -> Self:
        return cls(
            voter_id=response.voter_id,
            term=response.term,
            granted=response.granted,
        )


class LogRequestBody(BaseModel):
    """Payload for an AppendEntries (Log) RPC."""

    model_config = ConfigDict(frozen=True)

    leader_id: str = Field(description="The node ID of the current leader.")
    term: int = Field(ge=0, description="The leader's current term.")
    prefix_length: int = Field(ge=0, description="The log index preceding the new entries.")
    prefix_term: int = Field(ge=0, description="The term of the entry at prefix_length.")
    leader_commit: int = Field(ge=0, description="The leader's current commit length.")
    entries: list[LogEntryBody] = Field(default_factory=list, description="New entries to append.")

    def to_domain(self) -> LogRequest:
        return LogRequest(
            leader_id=self.leader_id,
            term=self.term,
            prefix_length=self.prefix_length,
            prefix_term=self.prefix_term,
            leader_commit=self.leader_commit,
            entries=tuple(entry.to_domain() for entry in self.entries),
        )


class LogResponseBody(BaseModel):
    """
    Response payload for an AppendEntries (Log) RPC.
    """

    model_config = ConfigDict(frozen=True)

    follower_id: str = Field(description="The node ID responding to the request.")
    term: int = Field(description="The follower's current term.")
    acknowledged_length: int = Field(description="The length of the follower's matching log.")
    success: bool = Field(description="True if the follower accepted the entries.")

    @classmethod
    def from_domain(cls, response: LogResponse) -> Self:
        return cls(
            follower_id=response.follower_id,
            term=response.term,
            acknowledged_length=response.acknowledged_length,
            success=response.success,
        )
