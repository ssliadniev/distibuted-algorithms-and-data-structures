from raft_node.core import LogEntry, RaftCore, Role

MEMBERS = ("node1", "node2", "node3")


def replicate_once(leader: RaftCore, follower: RaftCore) -> None:
    """
    Drive one successful leader-to-follower exchange without transport.
    """

    request = leader.make_log_request(follower.state.node_id)
    request_result = follower.handle_log_request(request)
    response_result = leader.handle_log_response(request_result.response)

    assert request_result.response.success is True
    assert response_result.stepped_down is False


def test_complete_assignment_scenario_at_the_protocol_layer() -> None:
    node1 = RaftCore.fresh("node1", MEMBERS)
    node2 = RaftCore.fresh("node2", MEMBERS)

    vote_requests = node1.start_election()
    node2_vote = node2.handle_vote_request(vote_requests[0])
    election = node1.handle_vote_response(node2_vote.response)

    assert election.became_leader is True

    node1.append_command("msg1")
    node1.append_command("msg2")

    replicate_once(node1, node2)
    replicate_once(node1, node2)

    assert node1.state.commit_length == 2
    assert node2.state.commit_length == 2

    node3 = RaftCore.fresh("node3", MEMBERS)
    replicate_once(node1, node3)

    assert node3.state.log == [LogEntry("msg1", 1), LogEntry("msg2", 1)]
    assert node3.state.commit_length == 2

    node2_requests = node2.start_election()
    node3_request = next(request for request in node2_requests if request.candidate_id == "node2")
    node3_vote = node3.handle_vote_request(node3_request)
    new_election = node2.handle_vote_response(node3_vote.response)

    assert new_election.became_leader is True
    assert node2.state.current_term == 2

    node2.append_command("msg3")
    replicate_once(node2, node3)
    replicate_once(node2, node3)
    node2.append_command("msg4")
    replicate_once(node2, node3)
    replicate_once(node2, node3)

    assert node2.state.commit_length == 4
    assert node3.state.commit_length == 4

    node1.append_command("msg5")

    assert node1.state.role is Role.LEADER
    assert node1.state.log[-1] == LogEntry("msg5", 1)
    assert node1.state.commit_length == 2

    replicate_once(node2, node1)
    expected_log = [
        LogEntry("msg1", 1),
        LogEntry("msg2", 1),
        LogEntry("msg3", 2),
        LogEntry("msg4", 2)
    ]

    assert node1.state.role is Role.FOLLOWER
    assert node1.state.current_term == 2
    assert node1.state.log == expected_log
    assert node1.state.commit_length == 4
    assert node2.state.log == expected_log
    assert node3.state.log == expected_log
