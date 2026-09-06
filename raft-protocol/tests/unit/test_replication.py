import pytest
from raft_node.core import (InvalidPeerError, InvariantViolation, LogEntry,
                            LogRequest, LogResponse, NotLeaderError, RaftCore,
                            RaftState, Role, VoteResponse)

MEMBERS = ("node1", "node2", "node3")


def elect_node1(*, term: int = 1, log: list[LogEntry] | None = None) -> RaftCore:
    state = RaftState(
        node_id="node1",
        members=MEMBERS,
        current_term=term - 1,
        log=list(log or []),
    )

    core = RaftCore(state)
    core.start_election()
    result = core.handle_vote_response(VoteResponse("node2", term, True))

    assert result.became_leader is True
    return core


def test_nonleader_cannot_append_or_build_replication_request() -> None:
    core = RaftCore.fresh("node1", MEMBERS)

    with pytest.raises(NotLeaderError) as append_error:
        core.append_command("msg1")
    with pytest.raises(NotLeaderError):
        core.make_log_request("node2")

    assert append_error.value.leader_id is None


def test_leader_appends_locally_and_builds_log_request() -> None:
    leader = elect_node1()

    appended = leader.append_command("msg1")
    request = leader.make_log_request("node2")

    assert appended.index == 0
    assert appended.entry == LogEntry("msg1", 1)
    assert appended.committed_entries == ()
    assert leader.state.commit_length == 0
    assert leader.state.acked_length["node1"] == 1
    assert request == LogRequest(
        leader_id="node1",
        term=1,
        prefix_length=0,
        prefix_term=0,
        leader_commit=0,
        entries=(LogEntry("msg1", 1),)
    )


def test_invalid_replication_target_is_rejected() -> None:
    leader = elect_node1()

    with pytest.raises(InvalidPeerError):
        leader.make_log_request("node1")
    with pytest.raises(InvalidPeerError):
        leader.make_log_request("outsider")


def test_follower_appends_entries_idempotently_and_resets_its_timer() -> None:
    follower = RaftCore.fresh("node2", MEMBERS)
    request = LogRequest(
        leader_id="node1",
        term=1,
        prefix_length=0,
        prefix_term=0,
        leader_commit=0,
        entries=(LogEntry("msg1", 1), LogEntry("msg2", 1))
    )

    first = follower.handle_log_request(request)
    duplicate = follower.handle_log_request(request)

    assert first.response == LogResponse("node2", 1, 2, True)
    assert first.reset_election_timer is True
    assert duplicate.response.success is True
    assert follower.state.log == [LogEntry("msg1", 1), LogEntry("msg2", 1)]
    assert follower.state.leader_id == "node1"


def test_follower_rejects_stale_term_and_unknown_leader() -> None:
    follower = RaftCore(RaftState(node_id="node2", members=MEMBERS, current_term=3))
    stale = follower.handle_log_request(LogRequest("node1", 2, 0, 0, 0))
    outsider = follower.handle_log_request(LogRequest("outsider", 9, 0, 0, 0))

    assert stale.response == LogResponse("node2", 3, 0, False)
    assert stale.reset_election_timer is False
    assert outsider.response == LogResponse("node2", 3, 0, False)
    assert follower.state.current_term == 3


def test_valid_leader_with_mismatched_prefix_is_known_but_request_is_rejected() -> None:
    follower = RaftCore(
        RaftState(
            node_id="node2",
            members=MEMBERS,
            current_term=1,
            log=[LogEntry("msg1", 1)]
        )
    )

    result = follower.handle_log_request(LogRequest("node1", 2, 2, 1, 0))

    assert result.response == LogResponse("node2", 2, 0, False)
    assert result.reset_election_timer is True
    assert follower.state.role is Role.FOLLOWER
    assert follower.state.leader_id == "node1"


def test_candidate_steps_down_for_log_request_in_the_same_term() -> None:
    follower = RaftCore.fresh("node2", MEMBERS)
    follower.start_election()
    assert follower.state.role is Role.CANDIDATE

    result = follower.handle_log_request(LogRequest("node1", 1, 0, 0, 0))

    assert result.response.success is True
    assert follower.state.role is Role.FOLLOWER
    assert follower.state.leader_id == "node1"
    assert follower.state.voted_for == "node2"


def test_empty_heartbeat_does_not_truncate_an_uncommitted_suffix() -> None:
    follower = RaftCore(
        RaftState(
            node_id="node2",
            members=MEMBERS,
            current_term=2,
            log=[LogEntry("msg1", 1), LogEntry("extra", 1)],
            commit_length=1,
        )
    )

    result = follower.handle_log_request(LogRequest("node1", 2, 1, 1, 1))

    assert result.response.success is True
    assert follower.state.log == [LogEntry("msg1", 1), LogEntry("extra", 1)]
    assert follower.state.commit_length == 1


def test_new_leader_replaces_old_leaders_uncommitted_msg5() -> None:
    old_leader = RaftCore(
        RaftState(
            node_id="node1",
            members=MEMBERS,
            current_term=1,
            voted_for="node1",
            log=[
                LogEntry("msg1", 1),
                LogEntry("msg2", 1),
                LogEntry("msg5", 1),
            ],
            commit_length=2,
            role=Role.LEADER,
            leader_id="node1"
        )
    )

    result = old_leader.handle_log_request(
        LogRequest(
            leader_id="node2",
            term=2,
            prefix_length=2,
            prefix_term=1,
            leader_commit=4,
            entries=(LogEntry("msg3", 2), LogEntry("msg4", 2))
        )
    )

    assert result.response == LogResponse("node1", 2, 4, True)
    assert result.committed_entries == (LogEntry("msg3", 2), LogEntry("msg4", 2))
    assert old_leader.state.role is Role.FOLLOWER
    assert old_leader.state.log == [
        LogEntry("msg1", 1),
        LogEntry("msg2", 1),
        LogEntry("msg3", 2),
        LogEntry("msg4", 2)
    ]
    assert old_leader.state.commit_length == 4


def test_follower_never_commits_past_entries_it_has_received() -> None:
    follower = RaftCore.fresh("node2", MEMBERS)

    result = follower.handle_log_request(
        LogRequest(
            leader_id="node1",
            term=1,
            prefix_length=0,
            prefix_term=0,
            leader_commit=5,
            entries=(LogEntry("msg1", 1),)
        )
    )

    assert result.committed_entries == (LogEntry("msg1", 1),)
    assert follower.state.commit_length == 1


def test_committed_entry_cannot_be_overwritten() -> None:
    follower = RaftCore(
        RaftState(
            node_id="node2",
            members=MEMBERS,
            current_term=2,
            log=[LogEntry("msg1", 1)],
            commit_length=1
        )
    )

    with pytest.raises(InvariantViolation, match="committed"):
        follower.handle_log_request(
            LogRequest(
                leader_id="node1",
                term=2,
                prefix_length=0,
                prefix_term=0,
                leader_commit=0,
                entries=(LogEntry("different", 2),)
            )
        )


def test_same_index_and_term_cannot_contain_a_different_command() -> None:
    follower = RaftCore(
        RaftState(
            node_id="node2",
            members=MEMBERS,
            current_term=1,
            log=[LogEntry("msg1", 1)]
        )
    )

    with pytest.raises(InvariantViolation, match="same command"):
        follower.handle_log_request(
            LogRequest(
                leader_id="node1",
                term=1,
                prefix_length=0,
                prefix_term=0,
                leader_commit=0,
                entries=(LogEntry("different", 1),)
            )
        )


def test_leader_commits_current_term_entry_after_quorum_acknowledges_it() -> None:
    leader = elect_node1()
    leader.append_command("msg1")

    result = leader.handle_log_response(LogResponse("node2", 1, 1, True))

    assert result.committed_entries == (LogEntry("msg1", 1),)
    assert result.retry_peer is None
    assert leader.state.commit_length == 1
    assert leader.state.sent_length["node2"] == 1
    assert leader.state.acked_length["node2"] == 1


def test_old_term_entry_is_committed_only_with_a_current_term_entry() -> None:
    leader = elect_node1(term=2, log=[LogEntry("old", 1)])

    old_ack = leader.handle_log_response(LogResponse("node2", 2, 1, True))
    appended = leader.append_command("current")
    current_ack = leader.handle_log_response(LogResponse("node2", 2, 2, True))

    assert old_ack.committed_entries == ()
    assert appended.committed_entries == ()
    assert leader.state.commit_length == 2
    assert current_ack.committed_entries == (
        LogEntry("old", 1),
        LogEntry("current", 2)
    )


def test_failed_log_response_backs_up_and_requests_an_immediate_retry() -> None:
    leader = elect_node1(log=[LogEntry("msg1", 0)])
    assert leader.state.sent_length["node2"] == 1

    retry = leader.handle_log_response(LogResponse("node2", 1, 0, False))
    exhausted = leader.handle_log_response(LogResponse("node2", 1, 0, False))

    assert retry.retry_peer == "node2"
    assert leader.state.sent_length["node2"] == 0
    assert exhausted.retry_peer is None


def test_successful_partial_ack_requests_more_entries() -> None:
    leader = elect_node1()
    leader.append_command("msg1")
    leader.append_command("msg2")

    result = leader.handle_log_response(LogResponse("node2", 1, 1, True))

    assert result.retry_peer == "node2"
    assert leader.state.commit_length == 1


def test_stale_acknowledgement_cannot_move_progress_backwards() -> None:
    leader = elect_node1()
    leader.append_command("msg1")
    leader.append_command("msg2")
    leader.handle_log_response(LogResponse("node2", 1, 2, True))

    stale = leader.handle_log_response(LogResponse("node2", 1, 1, True))

    assert stale.retry_peer is None
    assert leader.state.sent_length["node2"] == 2
    assert leader.state.acked_length["node2"] == 2


def test_acknowledgement_beyond_leader_log_is_an_invariant_violation() -> None:
    leader = elect_node1()

    with pytest.raises(InvariantViolation, match="beyond"):
        leader.handle_log_response(LogResponse("node2", 1, 1, True))


def test_higher_term_log_response_forces_leader_to_step_down() -> None:
    leader = elect_node1()

    result = leader.handle_log_response(LogResponse("node2", 5, 0, False))

    assert result.stepped_down is True
    assert leader.state.role is Role.FOLLOWER
    assert leader.state.current_term == 5
    assert leader.state.voted_for is None


def test_leader_ignores_unknown_self_and_stale_responses() -> None:
    leader = elect_node1()

    assert leader.handle_log_response(LogResponse("outsider", 9, 0, False)).stepped_down is False
    assert leader.handle_log_response(LogResponse("node1", 9, 0, False)).stepped_down is False
    assert leader.handle_log_response(LogResponse("node2", 0, 0, True)).retry_peer is None
    assert leader.state.role is Role.LEADER
    assert leader.state.current_term == 1


def test_single_node_leader_commits_its_append_immediately() -> None:
    core = RaftCore.fresh("only", ("only",))
    core.start_election()

    result = core.append_command("msg1")

    assert result.committed_entries == (LogEntry("msg1", 1),)
    assert core.state.commit_length == 1
