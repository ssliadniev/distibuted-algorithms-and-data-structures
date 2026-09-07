import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from raft_node.api.routes import api_router
from raft_node.config import Settings
from raft_node.core import RaftCore
from raft_node.runtime import RaftNode
from raft_node.transport import HttpPeerTransport, PeerTransport


@asynccontextmanager
async def app_lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    transport: PeerTransport | None = app.state._injected_transport
    random_source: Callable[[float, float], float] | None = app.state._injected_random_source

    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    resolved_transport = transport or HttpPeerTransport(
        settings.peer_urls,
        request_timeout=settings.http_request_timeout,
    )

    core = RaftCore.fresh(settings.node_id, settings.member_ids)

    node_kwargs: dict[str, Any] = {
        "heartbeat_interval": settings.heartbeat_interval,
        "election_timeout_range": settings.election_timeout_range,
        "client_commit_timeout": settings.client_commit_timeout,
    }

    if random_source is not None:
        node_kwargs["random_source"] = random_source

    node = RaftNode(core, resolved_transport, **node_kwargs)
    app.state.raft_node = node

    await node.start()
    try:
        yield
    finally:
        await node.stop()


def create_app(
    settings: Settings | None = None,
    transport: PeerTransport | None = None,
    random_source: Callable[[float, float], float] | None = None,
) -> FastAPI:
    """
    Initialize and configure the FastAPI application for a single Raft node.
    """

    app = FastAPI(title="Raft Protocol", version="0.1.0", lifespan=app_lifespan)

    app.state.settings = settings or Settings()
    app.state._injected_transport = transport
    app.state._injected_random_source = random_source

    app.include_router(api_router)
    return app
