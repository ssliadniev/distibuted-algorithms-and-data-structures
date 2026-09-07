# Raft protocol assignment

Python 3.12+ implementation of Raft with a static cluster and in-memory state.
The protocol follows sections 1–5 of the [Raft paper](https://raft.github.io/raft.pdf),
including the Figure 2 rules, and section 6.2 of the
[Cambridge distributed systems notes](https://www.cl.cam.ac.uk/teaching/2021/ConcDisSys/dist-sys-notes.pdf).

Implemented behavior: randomized elections, one vote per term, higher-term step-down,
log-freshness voting, heartbeats, prefix consistency checks, follower backtracking,
conflicting-suffix replacement, fixed-majority commitment, the current-term commitment
restriction, and ordered application of committed messages.

## Run the assignment scenario

Requirements: Python 3.12+, Docker Engine with Linux containers, Docker Compose **2.33.1+** and a POSIX shell. 

From this directory:

```bash
make self-test
```

The host-side acceptance script uses only Python's standard library. It builds the
application and installs its dependencies inside Docker. It resets this Compose project's
containers before starting and stops them afterward; their in-memory state is discarded.

All three members are configured before any process starts. Quorum is always **2 of 3**.
Starting node3 later makes an existing member available; it does not change membership.

| Step | Required observation before continuing                                                                                                               |
|------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1    | Only node1 and node2 run; one becomes leader.                                                                                                        |
| 2    | Both have committed and applied msg1, msg2 before node3 starts.                                                                                      |
| 3    | Node3 catches up to that committed and applied prefix.                                                                                               |
| 4    | The current leader is isolated in both peer directions, its client API remains reachable, and the other two nodes elect a higher-term leader.        |
| 5    | Both majority nodes commit and apply msg3, msg4 before healing.                                                                                      |
| 6    | POST msg5 reaches the old node and returns either a verified timeout with a local uncommitted entry, or a consistent NotALeader response.            |
| 7    | All nodes converge to identical entry terms and commands for msg1–msg4, commit and apply all four, and settle on one leader in a common higher term. |

For this implementation, an isolated leader normally keeps its old role until it learns
of a higher term. Therefore msg5 normally times out and leaves a conflicting entry, which
step 7 actually verifies is replaced. If msg5 is rejected before append, the script reports
that branch explicitly and does not claim it observed replacement of a nonexistent entry.
The offline runtime regression always exercises the conflicting-entry replacement branch.

Transport errors, missing responses, HTTP 5xx errors other than the expected commit timeout,
uncommitted majority entries, and unequal entry terms all fail the test. The script reads
published ports and timing values from resolved Compose configuration, including overrides.
On failure it saves container logs, process status, node snapshots, and command stderr in
`artifacts/self-test/<timestamp>/` **before** healing or stopping containers.

## Topology and manual operation

| Service | Default client URL    | Peer address     | Initial startup |
|---------|-----------------------|------------------|-----------------|
| node1   | http://localhost:8001 | 172.30.0.11:8000 | Yes             |
| node2   | http://localhost:8002 | 172.30.0.12:8000 | Yes             |
| node3   | http://localhost:8003 | 172.30.0.13:8000 | No              |

Nodes have two networks. Peer RPC URLs use fixed addresses on the internal `raft-cluster`
bridge. The `raft-client` bridge has the highest gateway priority and provides host access.
The partition helper disconnects only `raft-cluster`; the heal helper restores the node's
original static peer IP. The acceptance test probes actual cross-partition TCP paths and
checks the old node's published HTTP endpoint to validate this setup on the current host.

```bash
make cluster-up
make node3-up
curl http://localhost:8001/

sh scripts/partition-node.sh node1
sh scripts/heal-node.sh node1
make cluster-down
```

The example node1 argument is illustrative; leadership is randomized. Do not assume node1
is leader. The automated acceptance script chooses the current leader itself.

## HTTP API

```bash
curl http://localhost:8001/
curl --request POST http://localhost:8001/ \
  --header 'Content-Type: application/json' \
  --data '{"command":"msg1"}'
```

`GET /` reports identity, membership, role, term, vote, known leader, the full local log,
`commit_length`, `applied_length`, and `applied_commands`. Each log entry contains its
zero-based index, term, command, and committed flag. Counts are prefix lengths:
`commit_length = 2` means indices 0 and 1 are committed.

The application state machine is a message list. Applying a command appends its string
to `applied_commands`. The runtime applies only committed entries, in log order, once per
log index during a process lifetime. A repeated client POST is a new log entry, even when
its string is identical; client request deduplication is not implemented.

| Request result | Meaning                                                                                                                      |
|----------------|------------------------------------------------------------------------------------------------------------------------------|
| POST / → 201   | The exact entry is committed and applied locally; response includes index, term, command, committed=true, applied=true.      |
| POST / → 409   | NotALeader; body includes `detail.error = "not_a_leader"` and a possibly absent leader hint.                                 |
| POST / → 504   | Commit deadline expired; body includes `detail.error = "commit_timeout"`, index, and term. The entry can remain uncommitted. |
| POST / → 503   | The runtime has stopped.                                                                                                     |
| POST / → 422   | Invalid request body, such as an empty command.                                                                              |

A timeout is an uncertain client outcome: outside this controlled partition scenario,
the entry might commit later. It is not an automatic rollback or a deduplication token.

Internal endpoints are `POST /raft/vote` and `POST /raft/log`; `/health` checks HTTP liveness.
`GET /` is a diagnostic local snapshot, not a linearizable read protocol.

## Development and verification

```bash
make test-offline

python3 -m venv .venv
. .venv/bin/activate
make install
make format
make check
make self-test
```

`make test-offline` runs the standard-library suite against the actual core/runtime plus
regressions for the acceptance script. Runtime RPC delivery is simulated in memory, with
delays and bidirectional partitions. Its seven-step test uses the default assignment timings:
100 ms heartbeats, 450–900 ms elections, and a 3000 ms client commitment deadline.

`make check` runs Ruff, mypy, pytest with coverage, and Docker Compose configuration validation.
The original 90% coverage threshold is retained. HTTP integration tests exercise FastAPI
routes through ASGI and the peer client through httpx MockTransport. The Docker acceptance
test is the separate check for actual containers, published ports, and network disconnection.

Verification performed for this delivery: **36 offline tests passed**. Python compilation,
shell syntax, and local YAML/TOML parsing also passed, as recorded in CHANGES.md. Docker,
the HTTP dependencies, pytest, Ruff, mypy, and coverage were unavailable in the review
environment. Consequently the HTTP suite, coverage threshold, `make check`, and Docker
acceptance have **not** been certified as passing here. Run the commands above locally.

## Implementation notes and scope

- `core/` contains protocol state transitions; `runtime/` owns timers, serialized mutation,
  replication workers, and application; `api/` and `transport/` handle HTTP.
- Core/runtime mutation is serialized by an `asyncio.Condition` within one event loop.
  This is not a claim of safety under arbitrary OS-thread access.
- Domain messages and snapshots use frozen dataclasses and tuple collections. Pydantic
  `frozen=True` prevents field reassignment but does not deeply freeze nested lists/dicts.
- State validation scans the log: it is O(log length), not O(1). Building RPC suffixes and
  snapshots copies collections. There is no `itertools.islice` optimization in this version.
- Membership stays fixed. A reachable minority cannot elect a leader or commit new entries.
- No term, vote, log, commit boundary, or application state is persisted. Restarting an
  existing member discards its state; safe recovery after that loss is outside this assignment
  variant. The scenario keeps processes alive during partition and healing.
- Membership changes, snapshots/log compaction, durable crash recovery, linearizable reads,
  and client deduplication are outside the requested scope.

## Source layout

| Directory               | Contents                                                  |
|-------------------------|-----------------------------------------------------------|
| src/raft_node/core      | Protocol models, invariants, and transitions              |
| src/raft_node/runtime   | Async lifecycle, elections, replication, and application  |
| src/raft_node/api       | FastAPI routes and wire schemas                           |
| src/raft_node/transport | Transport protocol and HTTP implementation                |
| src/raft_node/config    | Environment settings and validation                       |
| scripts                 | Docker acceptance scenario, partition helper, heal helper |
| tests/offline           | Core/runtime, application, and acceptance regressions     |
| tests/integration       | HTTP API, peer transport, and settings tests              |
