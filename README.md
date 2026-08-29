<div align="center">

# ⚡ Cachey

### A **distributed** key-value cache store, written in Go

*Raft-based consensus. Sharding. Built for the cluster, not just the box.*

`Go 1.26.7` · `Experimental` · [License](LICENSE)

</div>

<br>

## 🌱 Overview

Cachey is a **distributed key-value cache store**. The long-term goal
is a cluster of nodes that replicate and shard data across machines —
not just a fast local cache on a single box.

The current implementation is the **first single-node foundation**: an
in-memory store exposed over TCP with newline-delimited JSON (NDJSON)
request and response framing. This is the base layer everything
distributed will be built on top of — step by step, this single node
grows into a **replicated, sharded** cluster.

<br>

## 🎯 Vision

At its core, Cachey is about **distribution** — spreading data and load
across many machines instead of relying on one. It's designed to evolve
from a local in-memory store into a full distributed cache with these
core properties:

| | Property | Description |
|---|---|---|
| 🧠 | **Raft-based consensus** | A consistent, replicated state machine |
| 🔀 | **Sharding** | Partition keys and scale horizontally across nodes |
| 🛡️ | **Failure detection & recovery** | Resilience to node and network failures |
| 🔌 | **Stable client protocol** | Hides cluster details from clients |

> **Note:** these are architectural *goals* — not claims about the current implementation.

<br>

## ✅ Features

- TCP client and server
- Newline-delimited JSON protocol
- In-memory key-value storage
- `GET`, `PUT`, and `DEL` commands
- Multiple commands over a single client connection
- Unit and TCP integration tests
- GitHub Actions checks for tests and static analysis

<br>

## 🚀 Quick Start

### Install

```sh
go install github.com/Sephy314/Cachey@latest
```

This installs the `cacheyd` binary to your `$GOPATH/bin` (or `$GOBIN`) —
no need to clone the repository.

### Run

**1. Start a Cachey server**

```sh
cacheyd :8080
```

**2. Connect with a client**

Point any TCP/NDJSON client at `127.0.0.1:8080` and start sending
`GET` / `PUT` / `DEL` commands (see [Protocol](#-protocol) below).

<br>

## 📡 Protocol

Cachey communicates over TCP using NDJSON — each request and response
is one JSON object followed by a newline.

| Field | Type | Description |
|---|---|---|
| `Type` | `string` | Command name: `GET`, `PUT`, or `DEL` |
| `Key` | `string` | Key to read, write, or delete |
| `Val` | `string` | Value for `PUT`; returned value for `GET` |

<table>
<tr><th>Requests</th><th>Responses</th></tr>
<tr valign="top">
<td>

```json
{"Type":"PUT","Key":"foo","Val":"bar"}
{"Type":"GET","Key":"foo","Val":""}
{"Type":"DEL","Key":"foo","Val":""}
```

</td>
<td>

```json
{"Type":"PUT","Key":"foo","Val":"bar"}
{"Type":"GET","Key":"foo","Val":"bar"}
{"Type":"DEL","Key":"foo","Val":""}
```

</td>
</tr>
</table>

The Go client in `pkg/client` handles JSON serialization and newline
framing automatically.

<br>

## 🏗️ Architecture

Cachey's architecture is built around one question: **how does a
single node become a distributed cluster?** The diagrams below show
where things stand today, and where the distributed design is headed.

### Foundation (single node, today)

```text
Client
  │
  │  TCP / NDJSON
  ▼
Server ──▶ Handler ──▶ Store ──▶ In-memory map
```

| Package | Responsibility |
|---|---|
| `internal/protocol` | Defines commands and JSON parsing |
| `internal/server` | Accepts TCP connections and dispatches requests |
| `internal/store` | Defines the storage interface and in-memory store |
| `pkg/client` | Provides a small TCP client |
| `cmd/cacheyd` | Starts the server |

### Target Distributed Architecture (the goal)

```text
                              Client
                                 │
                          Cluster Router
                                 │
                 ┌───────────────┴───────────────┐
                 │                                │
              Shard A                          Shard B
                 │                                │
     Raft group: A1, A2, A3           Raft group: B1, B2, B3
                 │                                │
        Replicated state                 Replicated state
```

The target design separates two responsibilities:

- **Sharding** — decides which shard owns a key and distributes load across the cluster
- **Raft** — keeps replicas within each shard consistent, handling leader election and log replication

These boundaries will be introduced incrementally, rather than assuming
a single-node in-memory map already provides distributed guarantees.

<br>

## � Durability & WAL Persistence

> **Status:** design (v10). The WAL durability layer is the next
> planned foundation for making a single node crash-safe before it
> becomes a replicated one.

Cachey's durability layer is a **WAL (Write-Ahead Log)** built for
crash recovery. The correctness bar is not "a file appears after PUT"
but:

```
PUT
 ↓
WAL durable
 ↓
success

Crash
 ↓
Recovery
 ↓
Snapshot + WAL
 ↓
Same state restored
```

The WAL is a durability layer for recovery — it is *not* a long-term
history store of the database.

### Architecture

```text
                 ┌──────────────┐
PUT ───────────► │     DML      │
                 └──────┬───────┘
                        │ Mutation
                        ▼
                 ┌──────────────┐
                 │    Logger    │  Logical WAL
                 └──────┬───────┘
                        │ Mutation / Control
                        ▼
                 ┌──────────────┐
                 │  WAL Channel │  Bounded
                 └──────┬───────┘
                        │
                        ▼
                 ┌──────────────┐
                 │  WAL Writer  │  Single writer
                 └──────┬───────┘
             ┌──────────┴──────────┐
             ▼                     ▼
      wal.ndjson             wal.tmp.ndjson
       Active WAL             Temporary WAL

                 ┌──────────────┐
                 │  WAL Manager │  Background
                 └──────┬───────┘
                        │ Control message
                        ▼
                   WAL Channel
```

- **Logger** — issues logical WAL record requests
- **WAL Writer** — the only goroutine that performs WAL file I/O; appends are always serialized
- **WAL Manager** — observes file state and drives sealing, snapshots, and rotation in the background

### Key invariants

1. **Single Writer** — one writer goroutine appends to every WAL file
2. **Ordering** — `log_index order = append order = durability order`
3. **Serialized Control** — mutations and control messages share one FIFO WAL channel
4. **WAL is Source of Truth** — the actual WAL file always wins over in-memory metadata
5. **Metadata is Volatile** — an in-memory cache, rebuilt during recovery
6. **Durability Contract** — a successful mutation is durable per the durability policy
7. **Index Continuity** — no gaps or duplicates in a healthy WAL
8. **Snapshot Boundary** — snapshots only cover up to a confirmed WAL boundary
9. **Atomic Snapshot Replace** — temp file + fsync + rename
10. **Atomic WAL Rotation** — temporary WAL becomes active via an atomic rename
11. **Directory Durability** — directory fsync after rename/unlink
12. **No Direct Manager Mutation** — the manager never mutates writer state directly
13. **Serialized Control Transition** — all writer state transitions go through the WAL channel
14. **Idempotent Control** — retrying the same control ID returns the same result, answered on the latest request's ack channel
15. **Non-Blocking Response** — `ResultCh` and `AckCh` are buffered (capacity 1); a vanished receiver never blocks the sender
16. **Bounded Memory** — WAL channel, hold queue, and temporary WAL never grow unbounded
17. **Crash Recoverability** — every intermediate crash state is decidable at startup recovery

### Durability policy

The initial implementation uses **synchronous fsync per mutation** —
simple and correct. A failed append or fsync is not reported as
successful, and the temporary WAL applies the same policy.

> **Future:** if benchmarks show a throughput ceiling, a **group
> commit** that batches writes before a single fsync can be introduced —
> while preserving index/append/durability ordering.

### Rotation lifecycle

```text
IDLE ──(StartSealing)──► SEALING ──(Snapshot)──► ROTATING ──(RotationComplete)──► IDLE
```

1. When the active WAL's metadata count approaches ~2,000 records, the manager sends `StartSealing`; the writer confirms a boundary index and switches new appends to `wal.tmp.ndjson`.
2. The manager writes a snapshot to `snapshot.tmp`, fsyncs, then atomically renames it to `snapshot` (followed by a directory fsync).
3. The manager sends `FinishRotation`; the writer stops appending to the temporary WAL and acks the last index.
4. The manager renames `wal.tmp.ndjson` → `wal.ndjson` (atomic in POSIX), then directory fsync.
5. The manager sends `RotationComplete`; the writer returns to `ACTIVE` and drains its hold queue before consuming new channel items.

Failures retry with exponential backoff; exceeding the retry limit exits
the process so startup recovery re-evaluates disk state. The temporary
WAL has its own explicit limit, applying backpressure when full.

### Recovery

- **Bootstrap** (no snapshot, no WALs) — empty store, `next_log_index = 1`
- **Partial write** — an incomplete trailing record is truncated; invalid JSON in the middle of the file is corruption (fail-fast)
- **Continuity check** — `snapshot.last_log_index = N` must be followed by WAL index `N+1`; gaps or duplicates fail-fast
- **Case A** (crash mid-sealing) — replay snapshot + both WALs, then rebuild them into one active WAL via `wal.ndjson.rebuild.tmp` + fsync + rename
- **Case B** (crash after snapshot, before rotation) — replay snapshot + WAL idempotently (last write wins)
- **Case C** (stray temporary WAL) — defensive check; a valid temporary WAL is verified and restored as the active WAL
- **`snapshot.tmp`** — never trusted as a completed snapshot; discarded when a complete `snapshot` exists

### Implementation order

`WALRecord`/`log_index` → WAL Writer → bounded WAL channel → mutation + buffered `ResultCh` → synchronous fsync → WAL Manager → `StartSealing` + buffered ack + idempotency → atomic snapshot → temporary WAL + hold queue → `FinishRotation` → atomic rename + directory fsync → `RotationComplete` → retry/backoff → crash recovery → partial-WAL recovery → continuity validation → failure/stress tests.

<br>

## �🛠️ Development

*For contributors working from a cloned copy of the repo:*

| Task | Command |
|---|---|
| Run the full test suite | `go test ./...` |
| Run static analysis | `go vet ./...` |
| Format Go files | `gofmt -w .` |

<br>

## 📄 License

See [LICENSE](LICENSE) for details.

<br>

<div align="center">

*Built one primitive at a time.*

</div>