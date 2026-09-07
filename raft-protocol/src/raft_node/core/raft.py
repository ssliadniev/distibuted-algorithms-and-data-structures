from typing import Self

from raft_node.core.errors import (InvalidPeerError, InvariantViolation,
                                   NotLeaderError)
from raft_node.core.models import (AppendCommandResult, LogEntry, LogRequest,
                                   LogRequestResult, LogResponse,
                                   LogResponseResult, NodeId, NodeSnapshot,
                                   Role, VoteRequest, VoteRequestResult,
                                   VoteResponse, VoteResponseResult)
from raft_node.core.state import RaftState


class RaftCore:
    """
    Pure protocol engine for one node.
    """

    def __init__(self, state: RaftState) -> None:
        self.state = state

    @classmethod
    def fresh(cls, node_id: NodeId, members: tuple[NodeId, ...]) -> Self:
        """
        Create an empty follower in a static cluster.
        """

        return cls(RaftState(node_id=node_id, members=members))

    def snapshot(self) -> NodeSnapshot:
        """
        Return an immutable point-in-time view of local state for status reporting.
        """

        state = self.state

        return NodeSnapshot(
            node_id=state.node_id,
            members=state.members,
            role=state.role,
            current_term=state.current_term,
            voted_for=state.voted_for,
            leader_id=state.leader_id,
            commit_length=state.commit_length,
            log=tuple(state.log),
            applied_length=state.applied_length,
            applied_commands=tuple(state.applied_commands)
        )

    def start_election(self) -> tuple[VoteRequest, ...]:
        """
        Transition to candidate, vote for self, and request votes from peers.

        Returns:
            A tuple of VoteRequest payloads to be broadcast to all other members.
        """

        state = self.state

        state.current_term += 1
        state.role = Role.CANDIDATE
        state.leader_id = None
        state.voted_for = state.node_id
        state.votes_received = {state.node_id}

        state.sent_length.clear()
        state.acked_length.clear()

        request = VoteRequest(
            candidate_id=state.node_id,
            term=state.current_term,
            log_length=len(state.log),
            last_log_term=state.last_log_term
        )
        requests = tuple(request for member in state.members if member != state.node_id)

        if len(state.votes_received) >= state.quorum_size:
            self._become_leader()

        state.validate()
        return requests

    def handle_vote_request(self, request: VoteRequest) -> VoteRequestResult:
        """
        Evaluate an incoming RequestVote RPC.
        """

        state = self.state
        if request.candidate_id not in state.members:
            return VoteRequestResult(
                response=VoteResponse(state.node_id, state.current_term, False),
                reset_election_timer=False
            )

        if request.term > state.current_term:
            self._become_follower(request.term)

        if request.term < state.current_term:
            return VoteRequestResult(
                response=VoteResponse(state.node_id, state.current_term, False),
                reset_election_timer=False
            )

        log_is_current = (request.last_log_term > state.last_log_term) or (
            request.last_log_term == state.last_log_term and request.log_length >= len(state.log)
        )
        can_vote = state.voted_for in (None, request.candidate_id)
        granted = log_is_current and can_vote

        if granted:
            state.role = Role.FOLLOWER
            state.leader_id = None
            state.voted_for = request.candidate_id

            state.votes_received.clear()
            state.sent_length.clear()
            state.acked_length.clear()

        state.validate()

        return VoteRequestResult(
            response=VoteResponse(state.node_id, state.current_term, granted),
            reset_election_timer=granted
        )

    def handle_vote_response(self, response: VoteResponse) -> VoteResponseResult:
        """
        Process a peer's vote and transition to leader if quorum is reached.
        """

        state = self.state
        if response.voter_id not in state.members:
            return VoteResponseResult()

        if response.term > state.current_term:
            self._become_follower(response.term)
            state.validate()

            return VoteResponseResult(stepped_down=True)

        if (
            state.role is not Role.CANDIDATE
            or response.term != state.current_term
            or not response.granted
        ):
            return VoteResponseResult()

        state.votes_received.add(response.voter_id)
        if len(state.votes_received) < state.quorum_size:
            state.validate()

            return VoteResponseResult()

        self._become_leader()
        state.validate()

        return VoteResponseResult(became_leader=True)

    def append_command(self, command: str) -> AppendCommandResult:
        """
        Append a client command to the leader's local log.

        Raises:
            NotLeaderError: If the node is currently a follower or candidate.
        """

        state = self.state
        if state.role is not Role.LEADER:
            raise NotLeaderError(state.leader_id)

        entry = LogEntry(command=command, term=state.current_term)
        state.log.append(entry)

        state.sent_length[state.node_id] = len(state.log)
        state.acked_length[state.node_id] = len(state.log)

        committed_entries = self._advance_commit_length()
        state.validate()

        return AppendCommandResult(index=len(state.log) - 1, entry=entry, committed_entries=committed_entries)

    def make_log_request(self, follower_id: NodeId) -> LogRequest:
        """
        Construct the next AppendEntries RPC payload for a specific follower.
        """

        state = self.state
        if state.role is not Role.LEADER:
            raise NotLeaderError(state.leader_id)

        self._require_other_member(follower_id)

        prefix_length = state.sent_length[follower_id]
        if not 0 <= prefix_length <= len(state.log):
            raise InvariantViolation("sent length must be within the leader log")

        prefix_term = state.log[prefix_length - 1].term if prefix_length > 0 else 0

        return LogRequest(
            leader_id=state.node_id,
            term=state.current_term,
            prefix_length=prefix_length,
            prefix_term=prefix_term,
            leader_commit=state.commit_length,
            entries=tuple(state.log[prefix_length:]),
        )

    def handle_log_request(self, request: LogRequest) -> LogRequestResult:
        """
        Process an AppendEntries RPC and enforce the Log Matching property.
        """

        state = self.state
        if request.leader_id not in state.members:
            return self._failed_log_request(reset_election_timer=False)

        if request.term < state.current_term:
            return self._failed_log_request(reset_election_timer=False)

        if request.term > state.current_term:
            self._become_follower(request.term, leader_id=request.leader_id)
        elif state.role is not Role.FOLLOWER:
            self._become_follower(request.term, leader_id=request.leader_id)
        else:
            state.leader_id = request.leader_id

        prefix_matches = request.prefix_length <= len(state.log) and (
            request.prefix_length == 0
            or state.log[request.prefix_length - 1].term == request.prefix_term
        )

        if not prefix_matches:
            state.validate()
            return self._failed_log_request(reset_election_timer=True)

        self._reconcile_log(request.prefix_length, request.entries)

        acknowledged_length = request.prefix_length + len(request.entries)
        previous_commit_length = state.commit_length
        state.commit_length = max(
            state.commit_length,
            min(request.leader_commit, acknowledged_length)
        )

        committed_entries = tuple(state.log[previous_commit_length : state.commit_length])

        state.validate()

        return LogRequestResult(
            response=LogResponse(
                follower_id=state.node_id,
                term=state.current_term,
                acknowledged_length=acknowledged_length,
                success=True
            ),
            reset_election_timer=True,
            committed_entries=committed_entries
        )

    def handle_log_response(self, response: LogResponse) -> LogResponseResult:
        """
        Process an AppendEntries response and update peer tracking.
        """

        state = self.state
        if response.follower_id not in state.members or response.follower_id == state.node_id:
            return LogResponseResult()

        if response.term > state.current_term:
            self._become_follower(response.term)
            state.validate()

            return LogResponseResult(stepped_down=True)

        if state.role is not Role.LEADER or response.term != state.current_term:
            return LogResponseResult()

        follower_id = response.follower_id
        if response.success:
            if response.acknowledged_length > len(state.log):
                raise InvariantViolation("follower acknowledged beyond the leader log")

            if response.acknowledged_length < state.acked_length[follower_id]:
                return LogResponseResult()

            state.sent_length[follower_id] = response.acknowledged_length
            state.acked_length[follower_id] = response.acknowledged_length

            committed_entries = self._advance_commit_length()
            retry_peer = follower_id if response.acknowledged_length < len(state.log) else None

            state.validate()

            return LogResponseResult(retry_peer=retry_peer, committed_entries=committed_entries)

        if state.sent_length[follower_id] == 0:
            return LogResponseResult()

        state.sent_length[follower_id] -= 1
        state.validate()

        return LogResponseResult(retry_peer=follower_id)

    def _become_follower(self, term: int, leader_id: NodeId | None = None) -> None:
        """
        Transition safely to the follower state for a specific term.
        """

        state = self.state
        if term < state.current_term:
            raise InvariantViolation("cannot move to an earlier term")

        if term > state.current_term:
            state.current_term = term
            state.voted_for = None

        state.role = Role.FOLLOWER
        state.leader_id = leader_id

        state.votes_received.clear()
        state.sent_length.clear()
        state.acked_length.clear()

    def _become_leader(self) -> None:
        """
        Initialize leader state upon winning an election.
        """

        state = self.state
        state.role = Role.LEADER
        state.leader_id = state.node_id

        state.votes_received.clear()

        state.sent_length = {member: len(state.log) for member in state.members}
        state.acked_length = {member: 0 for member in state.members}
        state.acked_length[state.node_id] = len(state.log)

    def _failed_log_request(self, *, reset_election_timer: bool) -> LogRequestResult:
        """
        Generate a failure response for an AppendEntries request.
        """

        state = self.state

        return LogRequestResult(
            response=LogResponse(
                follower_id=state.node_id,
                term=state.current_term,
                acknowledged_length=0,
                success=False
            ),
            reset_election_timer=reset_election_timer
        )

    def _reconcile_log(self, prefix_length: int, entries: tuple[LogEntry, ...]) -> None:
        """
        Safely append new entries, truncating existing logs on term conflicts.
        """

        state = self.state
        append_from = len(entries)

        for offset, incoming in enumerate(entries):
            index = prefix_length + offset

            if index >= len(state.log):
                append_from = offset
                break

            existing = state.log[index]
            if existing.term == incoming.term:
                if existing.command != incoming.command:
                    raise InvariantViolation("entries at the same index and term must contain the same command")

                continue

            if index < state.commit_length:
                raise InvariantViolation("a leader cannot overwrite a committed log entry")

            del state.log[index:]
            append_from = offset

            break

        state.log.extend(entries[append_from:])

    def _advance_commit_length(self) -> tuple[LogEntry, ...]:
        """
        Commit entries from the current term once safely replicated to a majority.
        """

        state = self.state
        if state.role is not Role.LEADER:
            return ()

        previous_commit_length = state.commit_length
        for length in range(len(state.log), previous_commit_length, -1):
            if state.log[length - 1].term != state.current_term:
                continue

            acknowledgements = sum(
                1 for member in state.members if state.acked_length.get(member, 0) >= length
            )

            if acknowledgements >= state.quorum_size:
                state.commit_length = length
                return tuple(state.log[previous_commit_length:length])

        return ()

    def _require_other_member(self, peer_id: NodeId) -> None:
        """
        Validate that a peer ID is a valid external cluster member.
        """

        state = self.state
        if peer_id not in state.members or peer_id == state.node_id:
            raise InvalidPeerError(f"{peer_id!r} is not another configured cluster member")
