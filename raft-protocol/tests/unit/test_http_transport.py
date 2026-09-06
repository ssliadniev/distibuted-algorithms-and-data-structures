import json

import httpx
import pytest

from raft_node.core import LogEntry, LogRequest, LogResponse, VoteRequest, VoteResponse
from raft_node.transport import HttpPeerTransport


@pytest.mark.asyncio
async def test_http_transport_serializes_vote_request_and_parses_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == httpx.URL("http://node2:8000/raft/vote")
        assert json.loads(request.content) == {
            "candidate_id": "node1",
            "term": 3,
            "log_length": 2,
            "last_log_term": 2
        }
        return httpx.Response(200, json={"voter_id": "node2", "term": 3, "granted": True})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = HttpPeerTransport({"node2": "http://node2:8000/"}, request_timeout=1, client=client)

    response = await transport.request_vote("node2", VoteRequest("node1", 3, 2, 2))
    await transport.close()

    assert response == VoteResponse("node2", 3, True)
    assert client.is_closed is False
    await client.aclose()


@pytest.mark.asyncio
async def test_http_transport_serializes_log_request_and_parses_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == httpx.URL("http://node2:8000/raft/log")
        assert json.loads(request.content) == {
            "leader_id": "node1",
            "term": 4,
            "prefix_length": 1,
            "prefix_term": 3,
            "leader_commit": 1,
            "entries": [{"command": "msg2", "term": 4}]
        }

        return httpx.Response(
            200,
            json={
                "follower_id": "node2",
                "term": 4,
                "acknowledged_length": 2,
                "success": True
            }
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = HttpPeerTransport({"node2": "http://node2:8000"}, request_timeout=1, client=client)
    request = LogRequest("node1", 4, 1, 3, 1, (LogEntry("msg2", 4),))

    response = await transport.append_entries("node2", request)

    assert response == LogResponse("node2", 4, 2, True)
    await client.aclose()


@pytest.mark.asyncio
async def test_http_transport_treats_errors_and_invalid_payloads_as_missing_responses() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/raft/vote":
            return httpx.Response(
                200,
                json={"voter_id": "node2", "term": 1, "granted": "false"}
            )

        return httpx.Response(503)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = HttpPeerTransport({"node2": "http://node2:8000"}, request_timeout=1, client=client)

    invalid_vote = await transport.request_vote("node2", VoteRequest("node1", 1, 0, 0))
    failed_log = await transport.append_entries("node2", LogRequest("node1", 1, 0, 0, 0))
    unknown_peer = await transport.request_vote("missing", VoteRequest("node1", 1, 0, 0))

    assert invalid_vote is None
    assert failed_log is None
    assert unknown_peer is None

    await client.aclose()
