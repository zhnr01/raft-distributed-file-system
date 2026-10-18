# Architecture

Design notes for the distributed file system. This describes the intended shape
of the system and the reasoning behind the main decisions.

## Overview

The system separates two concerns that have very different requirements:

- **Metadata** — which files exist, how they split into chunks, and where each
  chunk lives. This is small (kilobytes) and must stay consistent across every
  node, so it is replicated through the Raft consensus algorithm.
- **Chunk data** — the file bytes themselves. This is large and is replicated by
  direct copying between storage nodes, not through consensus.

A client asks a master where a chunk should go, then streams the bytes straight
to the storage nodes. The master never sits on the data path. This is the same
split used by the Google File System and HDFS, and the reason is cost: forcing a
64 MB chunk through a consensus round trip would multiply traffic for no
ordering benefit, since chunk contents only need per-chunk atomicity.

```
            placement request
   client ───────────────────▶ master (Raft group: 1 leader + followers)
      │                              │  replicates metadata via Raft
      │  chunk bytes (direct)        ▼
      └───────────────────▶ chunk servers ──▶ replicate chunk to peers
```

## Components

| Area | Responsibility |
|------|----------------|
| `raft`   | Consensus core: leader election, log replication, durable state |
| `meta`   | Metadata state machine applied on top of the Raft log |
| `chunk`  | Chunk storage, integrity checks, heartbeats, re-replication |
| `client` | Chunking, placement lookup, direct upload/download |
| `adapt`  | Adaptive replication: adjusts a file's replica count by access rate |
| `proto`  | Protocol Buffers / gRPC message and service definitions |

## Dependency direction

The consensus core is the center of the system and depends on nothing above it.
It reaches the outside world through two small interfaces — a clock and a
message transport — rather than talking to sockets or timers directly. That
indirection is deliberate: it lets the whole protocol run inside a deterministic
simulator, where time is virtual and the network is programmable, so a failing
run can be reproduced exactly from a seed.

Everything else depends inward toward the consensus core and the shared message
definitions; the core never depends on the storage, client, or adaptive layers.

### Code pointers — the two injected interfaces (U1)

| Interface | Declared in | Simulated implementation | Production implementation |
|---|---|---|---|
| Clock | `src/dfs/clock.py` | `src/dfs/sim/clock.py` (`SimClock`) | not yet written (U7) |
| Network | `src/dfs/network.py` | `src/dfs/sim/network.py` (`SimNetwork`) | `src/dfs/transport.py` (gRPC, U0) |

Run wiring: `src/dfs/sim/simulation.py` (`Simulation`) owns the seed and the one
PRNG, constructs the clock and network, and routes deliveries to registered
nodes by name.

The protocols live in `src/dfs/`, deliberately NOT in `src/dfs/sim/`: the Raft
core imports the interface and must never import the simulator (a separation the
project enforces with a grep check in its build harness).
`SimNetwork` keeps no queue of its own — it schedules deliveries onto the clock's
queue, so exactly one component decides event order.

### Code pointers — the safety invariants (U2)

| Piece | Lives in | Notes |
|---|---|---|
| Snapshot vocabulary | `src/dfs/state.py` | `Role`, `LogEntry`, `NodeState`, `ClusterState` — frozen dataclasses |
| The four checks | `src/dfs/invariants.py` | `InvariantChecker.check(cluster)` returns every `Violation` found |
| Failure report | `src/dfs/invariants.py` | `InvariantViolated` carries the violation plus the seed and virtual time |
| Single-stepping | `src/dfs/sim/clock.py` | `step()`, `next_due()`; `_fire_next()` is the only heap pop |
| Step-and-check loop | `src/dfs/sim/simulation.py` | `observe()`, `step()`, `run_until()` |

`state.py` and `invariants.py` import only the standard library and each other —
the checker has no knowledge of the simulator, so at U3 the real Raft node can
produce the same snapshots (rule 3, grep-verified).

Three of the four checks are stateful: Election Safety, Leader Append-Only and
Leader Completeness keep ledgers, because a violation can span two steps and be
invisible in either one alone. Log Matching is the only stateless check.
Entries are always addressed by `LogEntry.index`, never by position in the tuple.
The reasoning behind each of these choices, and the alternatives rejected, are
recorded in the project's design notes.

The invariants are proven able to fail by deliberately sabotaging the checker and
confirming the suite catches each sabotage; a check that has never fired is not
known to work.

### Code pointers — log replication (U4)

| Piece | Lives in | Notes |
|---|---|---|
| Message vocabulary | `src/dfs/messages.py` | `AppendEntries.entries` is `tuple[LogEntry, ...]`, not bare bytes — a re-sent entry keeps its original term |
| Request a write | `src/dfs/raft.py` | `RaftNode.submit(command)` appends locally, then replicates; refuses unless the node believes it is leader |
| Consistency check | `src/dfs/raft.py` | `_predecessor_matches()` — refuse unless the entry before the new ones matches by index AND term |
| Apply an append | `src/dfs/raft.py` | `_store_entries()` — skip an entry already held with the same term, truncate only on a genuine term conflict |
| Learn the commit point | `src/dfs/raft.py` | `_adopt_leader_commit()` — clamped to the local log, never moves backwards |
| Per-peer progress | `src/dfs/raft.py` | `_next_index` (optimistic, `last + 1`) and `_match_index` (known-replicated, starts at 0) |
| The commit rule | `src/dfs/raft.py` | `_advance_commit_index()` — an index commits only if its term is the leader's own, held by a quorum |
| Backtrack on refusal | `src/dfs/raft.py` | `_on_append_entries_response()` — `next_index = max(1, conflict_index)`, retried immediately |

Two properties here are worth reading the code for rather than trusting this table.

The commit rule encodes one clause twice. An entry is committed only when it was
created in the leader's **current** term and a majority has stored it; once that
happens every earlier entry is committed too, by Log Matching. The first half is a
guard, the second is why a single index number suffices and no per-entry scan is
needed. Committing a previous-term entry by counting replicas is the mistake the
Raft paper's Figure 8 exists to rule out, so the same rule is asserted independently
in the invariant checker as well — two encodings can only agree by both being
right.

The write path is idempotent by construction, not by luck. Storing skips an entry
already present with the same index and term, and the leader takes the maximum of
its recorded progress and the acknowledgement it just received, so a duplicated or
late-arriving message cannot move state the wrong way. This matters because the
transport is free to reorder and duplicate; the write path is written for that
network even where the tests do not yet drive it.

The reply carries the sender and the index it acknowledges. That is redundant for a
synchronous request/response client, where the caller knows both, and necessary for
this project's one-way delivery, which hands the receiver a bare object. The
general rule: in a one-way transport, anything the receiver of a reply needs must
be on the message.

## Consistency

- Metadata operations are linearizable: they are committed through Raft and read
  from committed state only, so a completed write is visible to every later read.
- A single chunk's writes are ordered by a short-lived primary lease, so replicas
  of one chunk cannot diverge.
- Whole-file writes are not atomic across chunks; this is stated as a known limit
  rather than papered over.

Under a network partition the system favors consistency over availability: the
majority side keeps serving, and the minority side stops accepting writes rather
than diverging.

## Key parameters

| Parameter | Default | Notes |
|-----------|---------|-------|
| Chunk size | 64 MB | 4 MB in tests so suites stay fast |
| Replication factor | 3 | adaptive range 2–5 |
| Heartbeat interval | 50 ms | well below the election timeout |
| Election timeout | 150–300 ms | randomized to avoid split votes |
| Primary lease | 60 s | expires so a failed primary never blocks a chunk |

All of these are configuration values, not constants baked into the code.
