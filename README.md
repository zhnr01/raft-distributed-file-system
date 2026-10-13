# Distributed File System

A fault-tolerant distributed file system built from first principles: the Raft
consensus algorithm implemented from scratch, a metadata/data split in the style
of the Google File System, and an adaptive replication layer that adjusts how
many copies of a file are kept based on how often it is read.

Work in progress. Capabilities are listed here as they are built and verified;
anything not listed is not done yet.

## Design

- Metadata is replicated through consensus; chunk data is replicated directly.
  A quorum round trip per 64 MB chunk would be wasteful, and chunk contents only
  need per-chunk atomicity, not a global order.
- Raft is implemented from the paper rather than pulled from a library.
- Correctness is established with a deterministic simulator — seeded, reproducible
  runs under injected message loss, delay, reordering, and partitions — not by a
  demo that happened to work once.

See [docs/architecture.md](docs/architecture.md) for the full design.

## Layout

```
docs/   design notes
src/    implementation
tests/  unit, simulation, and chaos tests
```

## Status

Nothing is runnable yet. The consensus core and its test harness come first; the
storage layer is built on top once consensus is verified.

## Limitations

Single datacenter. No cluster membership changes, no erasure coding, no POSIX
semantics. Python is used for clarity and for the deterministic test harness;
throughput is measured and reported honestly, not compared against production
systems.
