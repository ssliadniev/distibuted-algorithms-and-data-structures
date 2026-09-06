import json
import logging
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request

from collections.abc import Callable
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
LOGGER = logging.getLogger(__name__)

PROJECT_DIR = Path(__file__).resolve().parent.parent

COMPOSE = (
    "docker",
    "compose",
    "--profile",
    "full",
    "--project-directory",
    str(PROJECT_DIR),
    "-f",
    str(PROJECT_DIR / "docker-compose.yaml")
)

NODE_URLS = {
    "node1": f"http://127.0.0.1:{os.getenv('RAFT_NODE1_HOST_PORT', '8001')}",
    "node2": f"http://127.0.0.1:{os.getenv('RAFT_NODE2_HOST_PORT', '8002')}",
    "node3": f"http://127.0.0.1:{os.getenv('RAFT_NODE3_HOST_PORT', '8003')}"
}


class SelfTestError(RuntimeError):
    """
    The running cluster failed an acceptance condition.
    """



def compose(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    """
    Execute a docker compose command in the project directory.
    """

    return subprocess.run(
        (*COMPOSE, *arguments),
        check=check,
        cwd=PROJECT_DIR,
        text=True,
        capture_output=True
    )


def run_helper(name: str, node_id: str) -> None:
    """
    Execute a shell helper script for network manipulation.
    """

    subprocess.run(
        ("sh", str(PROJECT_DIR / "scripts" / name), node_id),
        check=True,
        cwd=PROJECT_DIR,
        text=True,
        capture_output=True
    )


def request_json(
    method: str, url: str, payload: dict[str, object] | None = None, timeout: float = 6
) -> tuple[int | None, dict[str, Any] | None]:
    data = json.dumps(payload).encode() if payload is not None else None

    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"}
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            decoded = json.loads(response.read())
            return response.status, decoded if isinstance(decoded, dict) else None

    except urllib.error.HTTPError as error:
        try:
            body = json.loads(error.read())
        except (json.JSONDecodeError, UnicodeDecodeError):
            body = None

        return error.code, body
    except (TimeoutError, urllib.error.URLError, ConnectionError):
        return None, None


def status(node_id: str) -> dict[str, Any] | None:
    response_status, payload = request_json("GET", NODE_URLS[node_id], timeout=1)
    return payload if response_status == 200 else None


def wait_for(description: str, predicate: Callable[[], Any], *, timeout: float = 15) -> Any:
    """
    Poll a predicate function until it evaluates to truthy or times out.
    """

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()

        if result:
            return result

        time.sleep(0.1)

    raise SelfTestError(f"timed out waiting for {description}")


def find_leader(candidates: tuple[str, ...]) -> str | None:
    leaders = [node_id for node_id in candidates if (status(node_id) or {}).get("role") == "leader"]
    return leaders[0] if len(leaders) == 1 else None


def post_command(node_id: str, command: str) -> tuple[int | None, dict[str, Any] | None]:
    return request_json("POST", NODE_URLS[node_id], {"command": command})


def has_committed_log(node_id: str, commands: list[str]) -> bool:
    """
    Verify that a node has exactly the expected committed sequence of commands.
    """

    node_status = status(node_id)
    if node_status is None or node_status.get("commit_length") != len(commands):
        return False

    entries = node_status.get("log")
    return (
        isinstance(entries, list)
        and all(isinstance(entry, dict) for entry in entries)
        and [entry.get("command") for entry in entries] == commands
    )


def main() -> int:
    """
    Execute the end-to-end Raft cluster acceptance tests.
    """

    if shutil.which("docker") is None:
        LOGGER.error("Self-test requires Docker with the Compose v2 plugin.")
        return 2

    partitioned_node: str | None = None
    compose("down", "--remove-orphans", check=False)

    try:
        LOGGER.info("1. Starting node1 and node2")
        compose("up", "--build", "--detach", "node1", "node2")
        old_leader = wait_for(
            description="the initial leader",
            predicate=lambda: find_leader(("node1", "node2")),
        )
        assert isinstance(old_leader, str)
        LOGGER.info(f"   Initial leader: {old_leader}")

        LOGGER.info("2. Posting msg1 and msg2")
        for command in ("msg1", "msg2"):
            response_status, _ = post_command(old_leader, command)
            if response_status != 201:
                raise SelfTestError(f"{command} returned HTTP {response_status}")

        LOGGER.info("3. Starting the preconfigured node3 and waiting for catch-up")
        compose("up", "--build", "--detach", "node3")
        wait_for(
            description="node3 to receive msg1 and msg2",
            predicate=lambda: has_committed_log(node_id="node3", commands=["msg1", "msg2"])
        )

        LOGGER.info(f"4. Isolating the old leader {old_leader}")
        run_helper("partition-node.sh", old_leader)
        partitioned_node = old_leader
        majority_nodes = tuple(node_id for node_id in NODE_URLS if node_id != old_leader)

        new_leader = wait_for(
            description="a new leader in the majority partition",
            predicate=lambda: find_leader(majority_nodes),
        )
        assert isinstance(new_leader, str)
        LOGGER.info(f"   New leader: {new_leader}")

        LOGGER.info("5. Posting msg3 and msg4 through the new leader")
        for command in ("msg3", "msg4"):
            response_status, _ = post_command(new_leader, command)
            if response_status != 201:
                raise SelfTestError(f"{command} returned HTTP {response_status}")

        LOGGER.info("6. Posting msg5 through the isolated old leader")
        response_status, _ = post_command(old_leader, "msg5")
        if response_status not in {409, 504, None}:
            raise SelfTestError(f"Old leader unexpectedly returned HTTP {response_status}")

        LOGGER.info("7. Healing the partition and waiting for log convergence")
        run_helper("heal-node.sh", old_leader)
        partitioned_node = None
        expected = ["msg1", "msg2", "msg3", "msg4"]
        wait_for(
            description="all logs to converge without msg5",
            predicate=lambda: all(has_committed_log(node_id, expected) for node_id in NODE_URLS)
        )
        LOGGER.info("Self-test passed: all nodes committed msg1 through msg4 and removed msg5")
        return 0

    except (SelfTestError, subprocess.CalledProcessError) as error:
        LOGGER.error(f"Self-test failed: {error}")
        return 1

    finally:
        if partitioned_node is not None:
            try:
                run_helper("heal-node.sh", partitioned_node)
            except subprocess.CalledProcessError as error:
                LOGGER.warning(f"Failed to heal {partitioned_node}: {error}")

        compose("down", "--remove-orphans", check=False)


if __name__ == "__main__":
    raise SystemExit(main())
