import asyncio

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import httpx
import pytest
from fastapi import FastAPI, status

from raft_node.api import create_app
from raft_node.config import Settings
from raft_node.core import LogRequest, LogResponse, VoteRequest, VoteResponse
from raft_node.transport import PeerTransport


class NullTransport(PeerTransport):
    """
    Mock transport that simulates a completely isolated node by dropping all RPCs.
    """

    def __init__(self) -> None:
        self.closed = False

    async def request_vote(self, peer_id: str, request: VoteRequest) -> VoteResponse | None:
        return None

    async def append_entries(self, peer_id: str, request: LogRequest) -> LogResponse | None:
        return None

    async def close(self) -> None:
        self.closed = True


@asynccontextmanager
async def configured_node_client(
    members: dict[str, str],
    transport: PeerTransport,
    commit_timeout_ms: int = 100,
    random_source: Callable[[float, float], float] = lambda _minimum, _maximum: 0.04
) -> AsyncIterator[tuple[FastAPI, httpx.AsyncClient]]:

    settings = Settings(
        node_id="node1",
        cluster_members=members,
        heartbeat_interval_ms=5,
        election_timeout_min_ms=20,
        election_timeout_max_ms=40,
        client_commit_timeout_ms=commit_timeout_ms
    )

    app = create_app(settings=settings, transport=transport, random_source=random_source)

    async with app.router.lifespan_context(app):
        transport_adapter = httpx.ASGITransport(app=app)

        async with httpx.AsyncClient(transport=transport_adapter, base_url="http://test") as client:
            yield app, client


THREE_NODE_CLUSTER = {
    "node1": "http://node1:8000",
    "node2": "http://node2:8000",
    "node3": "http://node3:8000"
}


@pytest.mark.asyncio
async def test_single_node_public_api_elects_appends_and_reports_committed_log() -> None:
    transport = NullTransport()
    members = {"node1": "http://node1:8000"}
    fast_random = lambda _minimum, _maximum: 0.02

    async with configured_node_client(members, transport, random_source=fast_random) as (_, client):
        async with asyncio.timeout(0.5):
            while (await client.get("/")).json()["role"] != "leader":
                await asyncio.sleep(0.005)

        response = await client.post("/", json={"command": "msg1"})
        status_response = await client.get("/")
        health_response = await client.get("/health")

    assert response.status_code == status.HTTP_201_CREATED
    assert response.json() == {
        "index": 0,
        "term": 1,
        "command": "msg1",
        "committed": True
    }

    assert status_response.json()["commit_length"] == 1
    assert status_response.json()["log"] == [
        {"index": 0, "term": 1, "command": "msg1", "committed": True}
    ]

    assert health_response.json() == {"node_id": "node1", "status": "ok"}
    assert transport.closed is True


@pytest.mark.asyncio
async def test_public_post_rejects_follower_with_leader_hint() -> None:
    transport = NullTransport()

    async with configured_node_client(THREE_NODE_CLUSTER, transport) as (_, client):
        response = await client.post("/", json={"command": "msg1"})

    assert response.status_code == status.HTTP_409_CONFLICT
    assert response.json()["detail"] == {"error": "not_a_leader", "leader_id": None}


@pytest.mark.asyncio
async def test_isolated_leader_post_times_out_and_remains_uncommitted() -> None:
    transport = NullTransport()

    async with configured_node_client(THREE_NODE_CLUSTER, transport, commit_timeout_ms=20) as (app, client):
        node = app.state.raft_node
        node.core.start_election()
        node.core.handle_vote_response(VoteResponse("node2", 1, True))

        response = await client.post("/", json={"command": "msg5"})
        local_status = (await client.get("/")).json()

    assert response.status_code == status.HTTP_504_GATEWAY_TIMEOUT
    assert response.json()["detail"] == {
        "error": "commit_timeout",
        "index": 0,
        "term": 1
    }

    assert local_status["commit_length"] == 0
    assert local_status["log"][0]["command"] == "msg5"
    assert local_status["log"][0]["committed"] is False


@pytest.mark.asyncio
async def test_internal_rpc_endpoints_validate_and_apply_requests() -> None:
    transport = NullTransport()

    async with configured_node_client(THREE_NODE_CLUSTER, transport) as (_, client):
        vote = await client.post(
            "/raft/vote",
            json={
                "candidate_id": "node2",
                "term": 1,
                "log_length": 0,
                "last_log_term": 0
            }
        )

        append = await client.post(
            "/raft/log",
            json={
                "leader_id": "node2",
                "term": 1,
                "prefix_length": 0,
                "prefix_term": 0,
                "leader_commit": 1,
                "entries": [{"command": "msg1", "term": 1}]
            }
        )

        invalid = await client.post(
            "/raft/vote",
            json={
                "candidate_id": "node2",
                "term": -1,
                "log_length": 0,
                "last_log_term": 0
            }
        )

    assert vote.json() == {"voter_id": "node1", "term": 1, "granted": True}
    assert append.json() == {
        "follower_id": "node1",
        "term": 1,
        "acknowledged_length": 1,
        "success": True
    }

    assert invalid.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
