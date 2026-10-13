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
