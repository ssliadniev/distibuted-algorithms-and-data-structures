#!/usr/bin/env python3
"""Run the assignment's seven steps against the real Docker/HTTP cluster."""

import json
import logging
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)
PROJECT_DIR = Path(__file__).resolve().parents[1]
COMPOSE = ("docker", "compose", "--profile", "full")
NODE_IDS = ("node1", "node2", "node3")
NODE_URLS: dict[str, str] = {}
COMMAND_TIMEOUT = 6.0
WAIT_TIMEOUT = 15.0
STABILITY_INTERVAL = 0.5


class SelfTestError(RuntimeError):
    """
    The scenario did not satisfy an assignment requirement.
    """


class RequestFailure(SelfTestError):
    """
    No HTTP response was received; this never proves a command was rejected.
    """


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*COMPOSE, *args],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
        check=check,
        timeout=300 if "--build" in args else 30
    )


def run_helper(name: str, node_id: str) -> None:
    subprocess.run(
        ["sh", str(PROJECT_DIR / "scripts" / name), node_id],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
        check=True,
        timeout=30
    )


def configure_from_compose() -> dict[str, str]:
    """
    Use resolved ports and timeouts, including Compose environment overrides.
    """

    global COMMAND_TIMEOUT, WAIT_TIMEOUT, STABILITY_INTERVAL

    config = json.loads(compose("config", "--format", "json").stdout)
    services = config["services"]
    peer_urls: dict[str, str] = {}
    client_timeouts = []
    election_timeouts = []
    heartbeats = []
    NODE_URLS.clear()

    for node_id in NODE_IDS:
        service = services[node_id]
        environment = service["environment"]
        members = json.loads(environment["RAFT_CLUSTER_MEMBERS"])

        if sorted(members) != list(NODE_IDS) or environment["RAFT_NODE_ID"] != node_id:
            raise SelfTestError("The scenario requires three consistently configured members")

        if peer_urls and peer_urls != members:
            raise SelfTestError("Cluster membership differs between services")

        peer_urls = members
        target_port = int(environment["RAFT_BIND_PORT"])
        ports = [p for p in service["ports"] if int(p["target"]) == target_port]

        if len(ports) != 1 or not str(ports[0].get("published", "")).isdigit():
            raise SelfTestError(f"{node_id} requires exactly one fixed published HTTP port")

        port = ports[0]
        host = port.get("host_ip", "127.0.0.1")

        if host in ("", "0.0.0.0", "::"):
            host = "127.0.0.1"
        if ":" in host:
            host = f"[{host}]"

        NODE_URLS[node_id] = f"http://{host}:{port['published']}"
        client_timeouts.append(float(environment["RAFT_CLIENT_COMMIT_TIMEOUT_MS"]) / 1000)
        election_timeouts.append(float(environment["RAFT_ELECTION_TIMEOUT_MAX_MS"]) / 1000)
        heartbeats.append(float(environment["RAFT_HEARTBEAT_INTERVAL_MS"]) / 1000)

    COMMAND_TIMEOUT = max(client_timeouts) + 3.0
    WAIT_TIMEOUT = max(15.0, 5 * max(election_timeouts))
    STABILITY_INTERVAL = max(0.5, 3 * max(heartbeats))

    return peer_urls


def request_json(
    node_id: str,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    *,
    timeout: float = 1.0,
) -> tuple[int, dict[str, Any]]:
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(
        NODE_URLS[node_id] + "/",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"}
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            code, raw = response.status, response.read()
    except urllib.error.HTTPError as error:
        with error:
            code, raw = error.code, error.read()
    except (urllib.error.URLError, OSError) as error:
        raise RequestFailure(f"{method} {node_id}: no HTTP response: {error}") from error

    try:
        result = json.loads(raw)
    except (ValueError, UnicodeError) as error:
        raise SelfTestError(f"{method} {node_id}: HTTP {code} returned invalid JSON") from error

    if not isinstance(result, dict):
        raise SelfTestError(f"{method} {node_id}: expected a JSON object")

    return code, result


def validate_snapshot(node_id: str, snapshot: dict[str, Any]) -> None:
    """
    Reject inconsistent status counters, entry metadata, or application state.
    """

    if snapshot.get("node_id") != node_id or sorted(snapshot.get("members", [])) != list(NODE_IDS):
        raise SelfTestError(f"{node_id}: incorrect identity or membership")

    if snapshot.get("role") not in ("follower", "candidate", "leader"):
        raise SelfTestError(f"{node_id}: invalid role")

    log = snapshot.get("log")
    if not isinstance(log, list):
        raise SelfTestError(f"{node_id}: log is not an array")

    for field in ("current_term", "commit_length", "applied_length"):
        if type(snapshot.get(field)) is not int or snapshot[field] < 0:
            raise SelfTestError(f"{node_id}: invalid {field}")

    commit, applied = snapshot["commit_length"], snapshot["applied_length"]
    if not 0 <= applied <= commit <= len(log):
        raise SelfTestError(f"{node_id}: invalid commit/application bounds")

    previous_term = 0
    for index, entry in enumerate(log):
        if (
            not isinstance(entry, dict)
            or entry.get("index") != index
            or type(entry.get("term")) is not int
            or not previous_term <= entry["term"] <= snapshot["current_term"]
            or not isinstance(entry.get("command"), str)
            or entry.get("committed") is not (index < commit)
        ):
            raise SelfTestError(f"{node_id}: invalid metadata at log index {index}")

        previous_term = entry["term"]

    if snapshot.get("applied_commands") != [entry["command"] for entry in log[:applied]]:
        raise SelfTestError(f"{node_id}: application state differs from the applied log prefix")


def status(node_id: str) -> dict[str, Any] | None:
    try:
        code, snapshot = request_json(node_id)
    except RequestFailure:
        return None

    if code != 200:
        return None
    validate_snapshot(node_id, snapshot)

    return snapshot


def require_status(node_id: str) -> dict[str, Any]:
    snapshot = status(node_id)
    if snapshot is None:
        raise SelfTestError(f"{node_id}: status API is not accessible")

    return snapshot


def wait_for(description: str, predicate: Callable[[], Any], timeout: float | None = None) -> Any:
    deadline = time.monotonic() + (WAIT_TIMEOUT if timeout is None else timeout)

    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result

        time.sleep(0.1)

    raise SelfTestError(f"Timed out waiting for {description}")


def find_leader(node_ids: Sequence[str], *, after_term: int = -1) -> dict[str, Any]:
    def check() -> dict[str, Any] | None:
        snapshots = [status(node_id) for node_id in node_ids]

        if any(snapshot is None for snapshot in snapshots):
            return None

        available = [snapshot for snapshot in snapshots if snapshot is not None]
        leaders = [snapshot for snapshot in available if snapshot["role"] == "leader"]

        if (
            len(leaders) == 1
            and leaders[0]["current_term"] > after_term
            and len({snapshot["current_term"] for snapshot in available}) == 1
            and all(snapshot["role"] != "candidate" for snapshot in available)
        ):
            return leaders[0]

        return None

    return wait_for(f"one leader among {tuple(node_ids)} after term {after_term}", check)


def snapshot_matches(snapshot: dict[str, Any], commands: list[str]) -> bool:
    return (
        snapshot["commit_length"] == snapshot["applied_length"] == len(commands)
        and snapshot["applied_commands"] == commands
        and [entry["command"] for entry in snapshot["log"]] == commands
    )


def committed_on(node_ids: Sequence[str], commands: list[str]) -> bool:
    snapshots = [status(node_id) for node_id in node_ids]
    return all(
        snapshot is not None and snapshot_matches(snapshot, commands) for snapshot in snapshots
    )


def post_committed(node_id: str, command: str) -> None:
    code, result = request_json(node_id, "POST", {"command": command}, timeout=COMMAND_TIMEOUT)
    if (
        code != 201
        or result.get("command") != command
        or result.get("committed") is not True
        or result.get("applied") is not True
    ):
        raise SelfTestError(
            f"{node_id}: {command} was not committed/applied: HTTP {code}: {result}"
        )


PROBE_PROGRAM = """
import json
import socket
import sys
from urllib.parse import urlsplit

results = []
for peer, url in json.loads(sys.argv[1]).items():
    address = urlsplit(url)
    try:
        with socket.create_connection((address.hostname, address.port), timeout=0.5):
            reachable = True
    except OSError:
        reachable = False
    results.append({"peer": peer, "reachable": reachable})
print(json.dumps(results))
"""


def verify_peer_partition(old_leader: str, peer_urls: dict[str, str]) -> None:
    """
    Probe every cross-partition direction while checking the host-facing API.
    """

    require_status(old_leader)
    for source in NODE_IDS:
        targets = {
            peer: url
            for peer, url in peer_urls.items()
            if peer != source and (source == old_leader or peer == old_leader)
        }

        result = compose("exec", "-T", source, "python", "-c", PROBE_PROGRAM, json.dumps(targets))
        probes = json.loads(result.stdout)

        if (
            not isinstance(probes, list)
            or {probe["peer"] for probe in probes} != set(targets)
            or any(probe["reachable"] is not False for probe in probes)
        ):
            raise SelfTestError(f"Peer partition failed from {source}: {probes}")

    require_status(old_leader)


def check_msg5_result(code: int, payload: dict[str, Any], snapshot: dict[str, Any], old_term: int) -> str:
    commands = [entry["command"] for entry in snapshot["log"]]
    if snapshot["commit_length"] != 2 or snapshot["applied_commands"] != ["msg1", "msg2"]:
        raise SelfTestError("The old leader changed its committed/applied prefix during isolation")

    detail = payload.get("detail", {})
    if not isinstance(detail, dict):
        raise SelfTestError(f"msg5 returned an invalid error body: {payload}")

    if code == 504 and detail.get("error") == "commit_timeout":
        if (
            commands != ["msg1", "msg2", "msg5"]
            or snapshot["log"][2]["term"] != old_term
            or detail.get("index") != 2
            or detail.get("term") != old_term
        ):
            raise SelfTestError("msg5 timeout did not leave the expected uncommitted entry")

        return "uncommitted"

    if code == 409 and detail.get("error") == "not_a_leader":
        if snapshot["role"] == "leader" or commands != ["msg1", "msg2"]:
            raise SelfTestError("NotALeader was inconsistent with the old node's state")
        return "rejected"

    raise SelfTestError(
        f"msg5 must time out uncommitted or return NotALeader: HTTP {code}: {payload}"
    )


def final_state_matches(snapshots: list[dict[str, Any]], old_term: int) -> bool:
    expected = ["msg1", "msg2", "msg3", "msg4"]
    logs = [tuple((entry["term"], entry["command"]) for entry in s["log"]) for s in snapshots]
    terms = {snapshot["current_term"] for snapshot in snapshots}

    return (
        len(snapshots) == 3
        and all(snapshot_matches(snapshot, expected) for snapshot in snapshots)
        and len(set(logs)) == 1
        and len(terms) == 1
        and min(terms) > old_term
        and sum(snapshot["role"] == "leader" for snapshot in snapshots) == 1
        and all(snapshot["role"] in ("leader", "follower") for snapshot in snapshots)
    )


def wait_for_final_state(old_term: int) -> None:
    stable_since: float | None = None
    previous_roles: tuple[tuple[str, str, int], ...] | None = None

    def check() -> bool:
        nonlocal stable_since, previous_roles

        snapshots = [status(node_id) for node_id in NODE_IDS]

        if any(snapshot is None for snapshot in snapshots) or not final_state_matches(
            [snapshot for snapshot in snapshots if snapshot is not None], old_term
        ):
            stable_since = None
            return False

        roles = tuple(
            (snapshot["node_id"], snapshot["role"], snapshot["current_term"])
            for snapshot in snapshots
            if snapshot is not None
        )

        if stable_since is None or roles != previous_roles:
            stable_since = time.monotonic()

        previous_roles = roles
        return time.monotonic() - stable_since >= STABILITY_INTERVAL

    wait_for("stable identical committed/applied logs and one leader on all nodes", check)


def collect_diagnostics(error: Exception) -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    directory = PROJECT_DIR / "artifacts" / "self-test" / timestamp
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "failure.txt").write_text(repr(error) + "\n", encoding="utf-8")

    if isinstance(error, subprocess.SubprocessError):
        for name in ("stdout", "stderr"):
            value = getattr(error, name, None)

            if value:
                text = value.decode(errors="replace") if isinstance(value, bytes) else value
                (directory / f"failed-command-{name}.txt").write_text(text, encoding="utf-8")

    for name, args in (
        ("compose-logs.txt", ("logs", "--no-color", "--timestamps")),
        ("compose-ps.txt", ("ps", "--all", "--format", "json")),
    ):
        try:
            result = compose(*args, check=False)
            text = f"exit={result.returncode}\n{result.stdout}\nSTDERR:\n{result.stderr}"
        except (OSError, subprocess.SubprocessError) as diagnostic_error:
            text = repr(diagnostic_error)

        (directory / name).write_text(text, encoding="utf-8")

    snapshots = {}
    for node_id in NODE_IDS:
        try:
            snapshots[node_id] = request_json(node_id)
        except (SelfTestError, KeyError) as diagnostic_error:
            snapshots[node_id] = {"error": str(diagnostic_error)}

    (directory / "statuses.json").write_text(json.dumps(snapshots, indent=2), encoding="utf-8")
    return directory


def main() -> int:
    if shutil.which("docker") is None:
        LOGGER.error("Docker with Compose >= 2.33.1 is required for this acceptance test")
        return 2

    partitioned_node: str | None = None
    configured = False

    try:
        peer_urls = configure_from_compose()
        configured = True
        compose("down", "--remove-orphans")
        LOGGER.info("1/7 Start node1 and node2 with fixed three-member configuration")
        compose("up", "--build", "--detach", "node1", "node2")
        running = set(compose("ps", "--status", "running", "--services").stdout.split())

        if running != {"node1", "node2"}:
            raise SelfTestError(f"Expected only two running nodes, got {running}")

        leader = find_leader(NODE_IDS[:2])

        LOGGER.info("2/7 Commit and apply msg1 and msg2 on both initial nodes")
        for command in ("msg1", "msg2"):
            post_committed(leader["node_id"], command)

        wait_for(
            "both initial nodes to commit msg1/msg2",
            lambda: committed_on(NODE_IDS[:2], ["msg1", "msg2"]),
        )

        LOGGER.info("3/7 Start the preconfigured third member and verify catch-up")
        compose("up", "--build", "--detach", "node3")
        wait_for(
            "all three nodes to commit msg1/msg2",
            lambda: committed_on(NODE_IDS, ["msg1", "msg2"]),
        )

        leader = find_leader(NODE_IDS)
        old_leader, old_term = leader["node_id"], leader["current_term"]
        majority = tuple(node_id for node_id in NODE_IDS if node_id != old_leader)

        LOGGER.info("4/7 Isolate current leader %s in term %s", old_leader, old_term)
        partitioned_node = old_leader
        run_helper("partition-node.sh", old_leader)
        isolated = require_status(old_leader)

        if isolated["current_term"] != old_term or isolated["role"] != "leader":
            raise SelfTestError("Leadership changed during partition setup; rerun the scenario")

        verify_peer_partition(old_leader, peer_urls)
        new_leader = find_leader(majority, after_term=old_term)["node_id"]

        LOGGER.info("5/7 Commit and apply msg3 and msg4 on both majority nodes")
        for command in ("msg3", "msg4"):
            post_committed(new_leader, command)

        wait_for(
            "the majority to commit msg1 through msg4 before healing",
            lambda: committed_on(majority, ["msg1", "msg2", "msg3", "msg4"]),
        )

        LOGGER.info("6/7 Send msg5 through the isolated node's reachable HTTP API")
        require_status(old_leader)
        code, payload = request_json(
            old_leader, "POST", {"command": "msg5"}, timeout=COMMAND_TIMEOUT
        )

        outcome = check_msg5_result(code, payload, require_status(old_leader), old_term)
        deadline = time.monotonic() + STABILITY_INTERVAL

        while time.monotonic() < deadline:
            if check_msg5_result(code, payload, require_status(old_leader), old_term) != outcome:
                raise SelfTestError("msg5 changed state during isolation")
            if not committed_on(majority, ["msg1", "msg2", "msg3", "msg4"]):
                raise SelfTestError("The majority lost its expected committed/applied state")
            time.sleep(0.1)

        LOGGER.info("7/7 Heal the partition and verify complete log/application convergence")
        run_helper("heal-node.sh", old_leader)
        partitioned_node = None
        wait_for_final_state(old_term)

        if outcome == "uncommitted":
            LOGGER.info("PASS: msg5 stayed uncommitted and was replaced; all seven steps passed")
        else:
            LOGGER.info("PASS: msg5 was rejected; all nodes converged (no msg5 entry to replace)")
        return 0
    except Exception as error:
        LOGGER.error("FAIL: %s", error)
        stderr = getattr(error, "stderr", None)

        if stderr:
            LOGGER.error("Command stderr: %s", stderr)
        try:
            LOGGER.error("Diagnostics saved before cleanup: %s", collect_diagnostics(error))
        except OSError as diagnostic_error:
            LOGGER.error("Could not save diagnostics: %s", diagnostic_error)
        return 1
    finally:
        if partitioned_node is not None:
            try:
                run_helper("heal-node.sh", partitioned_node)
            except (OSError, subprocess.SubprocessError) as cleanup_error:
                LOGGER.warning("Cleanup could not heal %s: %s", partitioned_node, cleanup_error)
        if configured:
            try:
                result = compose("down", "--remove-orphans", check=False)
                if result.returncode:
                    LOGGER.warning("Cleanup failed: %s", result.stderr)
            except (OSError, subprocess.SubprocessError) as cleanup_error:
                LOGGER.warning("Cleanup failed: %s", cleanup_error)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    raise SystemExit(main())
