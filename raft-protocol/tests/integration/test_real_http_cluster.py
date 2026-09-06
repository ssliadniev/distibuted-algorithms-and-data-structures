import asyncio
import json
import os
import socket
import sys

from collections.abc import Callable
from typing import Any, cast

import httpx
import pytest

from fastapi import status


def unused_port() -> int:
    """
    Bind to an ephemeral port to find a free port number, then release it.
    """

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


async def start_node(node_id: str, port: int, members: dict[str, str]) -> asyncio.subprocess.Process:
    environment = {
        **os.environ,
        "RAFT_NODE_ID": node_id,
        "RAFT_BIND_HOST": "127.0.0.1",
        "RAFT_BIND_PORT": str(port),
        "RAFT_CLUSTER_MEMBERS": json.dumps(members),
        "RAFT_HEARTBEAT_INTERVAL_MS": "20",
        "RAFT_ELECTION_TIMEOUT_MIN_MS": "100",
        "RAFT_ELECTION_TIMEOUT_MAX_MS": "250",
        "RAFT_HTTP_REQUEST_TIMEOUT_MS": "60",
        "RAFT_CLIENT_COMMIT_TIMEOUT_MS": "1000",
        "RAFT_LOG_LEVEL": "WARNING"
    }

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "raft_node",
        env=environment,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL
    )

    await wait_until_http(
        lambda: get_json(f"http://127.0.0.1:{port}/health"),
        lambda result: result is not None
    )

    return process


async def stop_node(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return

    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=1.0)
    except TimeoutError:
        process.kill()
        await process.wait()
    except ProcessLookupError:
        pass


async def get_json(url: str) -> dict[str, Any] | None:
    try:
        async with httpx.AsyncClient(timeout=0.2, trust_env=False) as client:
            response = await client.get(url)
            response.raise_for_status()

            payload = response.json()
            return payload if isinstance(payload, dict) else None
    except httpx.HTTPError:
        return None


async def wait_until_http(
    operation: Callable[[], object],
    predicate: Callable[[Any], bool],
    *,
    timeout: float = 3.0
) -> Any:
    async with asyncio.timeout(timeout):
        while True:
            result = await operation()

            if predicate(result):
                return result

            await asyncio.sleep(0.02)


@pytest.mark.asyncio
async def test_real_http_nodes_elect_replicate_and_catch_up_a_late_member() -> None:
    """
    Verify that actual OS processes can form a cluster, elect a leader, and sync logs.
    """

    ports = {node_id: unused_port() for node_id in ("node1", "node2", "node3")}
    members = {node_id: f"http://127.0.0.1:{port}" for node_id, port in ports.items()}

    processes: list[asyncio.subprocess.Process] = []
    try:
        processes.append(await start_node("node1", ports["node1"], members))
        processes.append(await start_node("node2", ports["node2"], members))

        async def current_leader() -> str | None:
            for node_id in ("node1", "node2"):
                status_data = await get_json(members[node_id])

                if status_data is not None and status_data.get("role") == "leader":
                    return node_id

            return None

        leader = await wait_until_http(current_leader, lambda result: result is not None)
        assert isinstance(leader, str)

        async with httpx.AsyncClient(timeout=2.0, trust_env=False) as client:
            for command in ("msg1", "msg2"):
                response = await client.post(members[leader], json={"command": command})
                assert response.status_code == status.HTTP_201_CREATED

        processes.append(await start_node("node3", ports["node3"], members))

        node3_status = await wait_until_http(
            lambda: get_json(members["node3"]),
            lambda result: isinstance(result, dict) and result.get("commit_length") == 2
        )
        assert isinstance(node3_status, dict)

        log_entries = cast(list[dict[str, Any]], node3_status.get("log", []))
        assert [entry.get("command") for entry in log_entries] == ["msg1", "msg2"]

    finally:
        await asyncio.gather(*(stop_node(process) for process in reversed(processes)))
