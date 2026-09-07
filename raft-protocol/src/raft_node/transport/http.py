import logging
from collections.abc import Mapping
from typing import Any

import httpx
from raft_node.core import LogRequest, LogResponse, VoteRequest, VoteResponse

LOGGER = logging.getLogger(__name__)


class HttpPeerTransport:
    """
    Send Raft RPCs through a shared asynchronous HTTP client.
    """

    def __init__(
        self,
        peer_urls: Mapping[str, str],
        *,
        request_timeout: float,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._peer_urls = {peer_id: url.rstrip("/") for peer_id, url in peer_urls.items()}
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=request_timeout, trust_env=False)

    async def request_vote(self, peer_id: str, request: VoteRequest) -> VoteResponse | None:
        """
        Send a RequestVote payload as a POST request to a peer.
        """

        payload = {
            "candidate_id": request.candidate_id,
            "term": request.term,
            "log_length": request.log_length,
            "last_log_term": request.last_log_term
        }

        data = await self._post(peer_id, "/raft/vote", payload)
        if data is None:
            return None

        try:
            return VoteResponse(
                voter_id=str(data["voter_id"]),
                term=int(data["term"]),
                granted=_require_boolean(data["granted"])
            )
        except (KeyError, TypeError, ValueError) as error:
            LOGGER.warning(msg=f"Invalid VoteResponse from {peer_id}: {error}")
            return None

    async def append_entries(self, peer_id: str, request: LogRequest) -> LogResponse | None:
        """
        Send an AppendEntries payload as a POST request to a peer.
        """

        payload = {
            "leader_id": request.leader_id,
            "term": request.term,
            "prefix_length": request.prefix_length,
            "prefix_term": request.prefix_term,
            "leader_commit": request.leader_commit,
            "entries": [
                {"command": entry.command, "term": entry.term} for entry in request.entries
            ]
        }

        data = await self._post(peer_id, "/raft/log", payload)
        if data is None:
            return None

        try:
            return LogResponse(
                follower_id=str(data["follower_id"]),
                term=int(data["term"]),
                acknowledged_length=int(data["acknowledged_length"]),
                success=_require_boolean(data["success"])
            )
        except (KeyError, TypeError, ValueError) as error:
            LOGGER.warning(msg=f"Invalid LogResponse from {peer_id}: {error}")
            return None

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def _post(
        self,
        peer_id: str,
        path: str,
        payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        peer_url = self._peer_urls.get(peer_id)
        if peer_url is None:
            LOGGER.warning(msg=f"Refusing to send RPC to unknown peer {peer_id}")
            return None

        try:
            response = await self._client.post(f"{peer_url}{path}", json=payload)
            response.raise_for_status()

            data = response.json()
            if not isinstance(data, dict):
                raise TypeError("Response body must be a JSON object")

            return data
        except (httpx.HTTPError, TypeError, ValueError) as error:
            LOGGER.debug(msg=f"RPC to {peer_id}{path} failed: {error}")
            return None


def _require_boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise TypeError("Expected a JSON boolean")

    return value
