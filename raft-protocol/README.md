# Raft protocol

In-memory implementation of the Raft consensus algorithm of the supplied distributed-systems notes. 
The project targets Python 3.12 and runs one Raft node per Docker container.

Implemented behavior includes randomized leader elections, higher-term step-down, log-freshness vote checks, periodic heartbeats, per-follower log backtracking, conflict replacement, quorum commitment, and the current-term commit restriction. 
Cluster membership and all Raft state are intentionally kept static and in memory, as required by the assignment.

---

## Key Architectural & Performance Optimizations

*   **Algorithmic Efficiency:** Log validation operates in $O(1)$ time by checking term monotonicity at the boundary rather than iterating the entire log.
*   **Memory Safety:** Uses lazy evaluation (`itertools.islice`) and generators during state transitions to prevent $O(N)$ memory duplication when evaluating rapidly growing logs.
*   **Immutability:** Domain messages, configuration, and API payloads are strictly immutable (`frozen=True` dataclasses and Pydantic models) to guarantee thread safety during asynchronous cross-task RPCs.
*   **Dependency Injection:** The FastAPI layer utilizes strict dependency injection (`Depends`) to pass the Raft runtime state, decoupling HTTP routing from the core protocol logic for isolated testing.

---

## Project layout

```text
src/raft_node/
  api/          HTTP routes, dependency injection, and Pydantic schemas
  config/       Environment settings and static membership
  core/         Pure Raft state and transition logic
  runtime/      Timers, elections, replication, and lifecycle
  transport/    Async peer HTTP client
tests/
  unit/         Deterministic protocol-state tests
  integration/  HTTP and multi-node in-process tests
scripts/        Local partition and recovery helpers
```

---

## Local topology

All three members are configured from the start. The first command starts two available members; the third member is started later without changing cluster membership.

| Service | Host URL                | Raft-network address | Default startup |
|---------|-------------------------|----------------------|-----------------|
| `node1` | `http://localhost:8001` | `172.30.0.11:8000`   | Yes             |
| `node2` | `http://localhost:8002` | `172.30.0.12:8000`   | Yes             |
| `node3` | `http://localhost:8003` | `172.30.0.13:8000`   | No              |

---

## HTTP API

Every node exposes its public API on `/`:

```bash
curl http://localhost:8001/

curl --request POST http://localhost:8001/ \
  --header 'Content-Type: application/json' \
  --data '{"command":"msg1"}'
```

`GET /` returns the local role, term, known leader, static membership, commit boundary, and complete
local log. Each log item includes a `committed` flag.

`POST /` returns HTTP `201` only after the command is committed. A follower returns `409` with its
leader hint. An isolated leader that cannot form a quorum returns `504`; its entry may remain locally
uncommitted until a higher-term leader repairs the log.

Nodes use two internal endpoints:

- `POST /raft/vote` for `VoteRequest` and `VoteResponse` messages.
- `POST /raft/log` for `LogRequest` and `LogResponse` messages.

---

## Development commands

```bash
make install
make check
make compose-config
```

The Docker lifecycle commands are:

```bash
make cluster-up
make node3-up
./scripts/partition-node.sh node1
./scripts/heal-node.sh node1
make cluster-down
```

The partition command disconnects only peer traffic. The selected node remains reachable through its published host port, allowing the stale-leader behavior to be tested.

Run the complete seven-step assignment scenario with:
```bash
make self-test
```

The self-test starts two nodes, posts `msg1` and `msg2`, starts Node 3, partitions the original leader,
commits `msg3` and `msg4` through the new leader, attempts `msg5` on the old leader, heals the
partition, and verifies that all three logs converge without `msg5`.

---

## Design constraints

- Membership is immutable and contains all three nodes before any process starts.
- A majority is calculated from configured members, never currently reachable members.
- State is not persisted. Restarting a process loses its term, vote, log, and commit boundary.
- The public GET endpoint is diagnostic local state, not a linearizable state-machine read.
- Client-command deduplication and exactly-once semantics are outside the requested scope.
