import pytest
from raft_node.core import (InvariantViolation, LogEntry, RaftCore, RaftState,
                            Role, VoteRequest, VoteResponse)

MEMBERS = ("node1", "node2", "node3")


def test_fresh_node_is_an_empty_follower() -> None:
    core = RaftCore.fresh("node1", MEMBERS)

    snapshot = core.snapshot()

    assert snapshot.node_id == "node1"
    assert snapshot.members == MEMBERS
    assert snapshot.role is Role.FOLLOWER
    assert snapshot.current_term == 0
    assert snapshot.voted_for is None
    assert snapshot.leader_id is None
    assert snapshot.commit_length == 0
    assert snapshot.log == ()
    assert core.state.quorum_size == 2
    assert core.state.last_log_term == 0


def test_start_election_votes_for_self_and_builds_requests() -> None:
    core = RaftCore.fresh("node1", MEMBERS)
    core.state.current_term = 2
    core.state.log.append(LogEntry("previous", 2))

    requests = core.start_election()

    assert core.state.role is Role.CANDIDATE
    assert core.state.current_term == 3
    assert core.state.voted_for == "node1"
    assert core.state.votes_received == {"node1"}

    assert len(requests) == 2
    assert all(request.candidate_id == "node1" for request in requests)
    assert all(request.term == 3 for request in requests)
    assert all(request.log_length == 1 for request in requests)
    assert all(request.last_log_term == 2 for request in requests)


def test_single_node_cluster_elects_itself_immediately() -> None:
    core = RaftCore.fresh("only", ("only",))

    requests = core.start_election()

    assert requests == ()
    assert core.state.role is Role.LEADER
    assert core.state.leader_id == "only"


def test_vote_request_from_newer_term_updates_term_even_when_log_is_stale() -> None:
    state = RaftState(
        node_id="node1",
        members=MEMBERS,
        current_term=3,
        voted_for="node1",
        log=[LogEntry("committed", 3)],
        role=Role.LEADER,
        leader_id="node1"
    )
    core = RaftCore(state)

    result = core.handle_vote_request(
        VoteRequest(candidate_id="node2", term=4, log_length=0, last_log_term=0)
    )

    assert result.response.granted is False
    assert result.response.term == 4
    assert result.reset_election_timer is False
    assert state.current_term == 4
    assert state.role is Role.FOLLOWER
    assert state.voted_for is None
    assert state.leader_id is None


def test_vote_is_granted_to_an_up_to_date_candidate() -> None:
    core = RaftCore.fresh("node1", MEMBERS)

    result = core.handle_vote_request(
        VoteRequest(candidate_id="node2", term=1, log_length=0, last_log_term=0)
    )

    assert result.response == VoteResponse(voter_id="node1", term=1, granted=True)
    assert result.reset_election_timer is True
    assert core.state.voted_for == "node2"
    assert core.state.role is Role.FOLLOWER


def test_node_votes_at_most_once_per_term_but_repeats_same_vote() -> None:
    core = RaftCore.fresh("node1", MEMBERS)
    request = VoteRequest(candidate_id="node2", term=1, log_length=0, last_log_term=0)

    first = core.handle_vote_request(request)
    duplicate = core.handle_vote_request(request)
    competitor = core.handle_vote_request(
        VoteRequest(candidate_id="node3", term=1, log_length=0, last_log_term=0)
    )

    assert first.response.granted is True
    assert duplicate.response.granted is True
    assert competitor.response.granted is False
    assert core.state.voted_for == "node2"


@pytest.mark.parametrize(
    ("candidate_term", "candidate_length", "expected"),
    [
        (3, 1, True),
        (2, 3, True),
        (2, 2, False),
        (1, 10, False)
    ]
)
def test_vote_uses_last_term_then_log_length(
    candidate_term: int,
    candidate_length: int,
    expected: bool
) -> None:
    state = RaftState(
        node_id="node1",
        members=MEMBERS,
        current_term=3,
        log=[LogEntry("one", 1), LogEntry("two", 2), LogEntry("three", 2)]
    )
    core = RaftCore(state)

    result = core.handle_vote_request(
        VoteRequest(
            candidate_id="node2",
            term=3,
            log_length=candidate_length,
            last_log_term=candidate_term
        )
    )

    assert result.response.granted is expected


def test_candidate_becomes_leader_after_quorum_and_duplicate_votes_do_not_count() -> None:
    core = RaftCore.fresh("node1", MEMBERS)
    core.start_election()

    ignored_duplicate = core.handle_vote_response(VoteResponse("node1", 1, True))
    elected = core.handle_vote_response(VoteResponse("node2", 1, True))

    assert ignored_duplicate.became_leader is False
    assert elected.became_leader is True
    assert core.state.role is Role.LEADER
    assert core.state.leader_id == "node1"
    assert core.state.sent_length == {member: 0 for member in MEMBERS}
    assert core.state.acked_length == {member: 0 for member in MEMBERS}


def test_candidate_ignores_stale_negative_and_unknown_vote_responses() -> None:
    core = RaftCore.fresh("node1", MEMBERS)
    core.start_election()

    assert core.handle_vote_response(VoteResponse("node2", 0, True)).became_leader is False
    assert core.handle_vote_response(VoteResponse("node2", 1, False)).became_leader is False
    assert core.handle_vote_response(VoteResponse("outsider", 9, True)).stepped_down is False

    assert core.state.role is Role.CANDIDATE
    assert core.state.current_term == 1


def test_higher_term_vote_response_forces_candidate_to_step_down() -> None:
    core = RaftCore.fresh("node1", MEMBERS)
    core.start_election()

    result = core.handle_vote_response(VoteResponse("node2", 4, False))

    assert result.stepped_down is True
    assert core.state.role is Role.FOLLOWER
    assert core.state.current_term == 4
    assert core.state.voted_for is None


@pytest.mark.parametrize("state", [RaftState(node_id="node1", members=MEMBERS)])
def test_state_validation_can_be_called_after_construction(state: RaftState) -> None:
    state.validate()


def test_state_rejects_invalid_membership() -> None:
    with pytest.raises(InvariantViolation, match="local node"):
        RaftState(node_id="node1", members=("node2", "node3"))


def test_state_rejects_a_commit_beyond_the_log() -> None:
    with pytest.raises(InvariantViolation, match="commit length"):
        RaftState(node_id="node1", members=MEMBERS, commit_length=1)
