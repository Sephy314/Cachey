<div align="center">

# ⚡ Cachey

### A **distributed** key-value cache store, written in Go

*Raft and PBFT consensus. Sharding. Built for the cluster, not just the box.*

`Go 1.26.7` · `Experimental` · [License](LICENSE)

</div>

<br>

## 🌱 Overview

Cachey is a **distributed key-value cache store** built in Go. It grows a
replicated, consistent cluster out of a plain in-memory store: writes are
proposed to a consensus leader, durably logged, and applied to replicas
only once they commit.

Two consensus engines are implemented and tested end to end:

- **Raft** (`internal/raft`) — crash-fault-tolerant replication: leader
  election, log replication with WAL-backed durability, dynamic membership,
  linearizable reads, leader redirect, and snapshot/log compaction.
- **PBFT** (`internal/pbft`) — Byzantine-fault-tolerant replication:
  normal-case pre-prepare/prepare/commit ordering, Ed25519-authenticated
  messages, view change with Byzantine-safe prepared certificates, and
  WAL-backed recovery of the ordered log.

The runnable `cacheyd` binary still starts a single node today; wiring it
into a multi-node cluster is the next step before keys are sharded across
machines.

<br>

## 🎯 Status

| | Area | Status |
|---|---|---|
| 🧠 | **Raft consensus** (crash-fault tolerant) | ✅ Implemented — election, replication, membership, linearizable reads, snapshots |
| 🧠 | **PBFT consensus** (Byzantine-fault tolerant) | ✅ Implemented — normal case, view change, Ed25519 auth, WAL recovery |
| 🔀 | **Sharding** | 🚧 Roadmap — next after `cacheyd` cluster wiring |
| 🛡️ | **Failure detection & recovery** | ✅ Elections/view changes + failover tested; WAL + snapshot recovery |
| 🔌 | **Stable client protocol** | ✅ NDJSON protocol + leader-redirect hints |

> **Note:** the status table reflects what is implemented in this
> repository today — both consensus *engines* and their integration tests
> are complete, while exposing cluster mode through the `cacheyd` binary
> and sharding keys across nodes remain on the roadmap.

<br>

## ✅ Features

**Core store**
- TCP client and server over newline-delimited JSON (NDJSON)
- `GET`, `PUT`, `DEL`, and `TTL` commands (expire a key after N ms)
- Durable **WAL** (write-ahead log) with crash recovery, rotation, and snapshots
- Background key-expiration sweep

**Raft consensus engine** (`internal/raft`)
- Leader election with randomized timeouts (no split votes)
- Log replication over TCP with commit/apply to the state machine
- Durable raft log through the existing WAL (rotation disabled in raft mode)
- Linearizable reads via the read-index protocol
- Leader redirect — clients are told where the current leader is
- Dynamic membership — add/remove voting members (single-server changes)
- Log compaction + `InstallSnapshot` catch-up for joining and restarting nodes
- End-to-end cluster tests: replication, failover, restart recovery, add/remove
  membership, stale-node election loss, network partitions, snapshot restore

**PBFT consensus engine** (`internal/pbft`)
- Normal-case consensus — pre-prepare / prepare / commit with total order
- Byzantine fault tolerance — tolerates `f` faulty replicas (`N >= 3f + 1`)
- All messages authenticated with Ed25519 signatures; forged/tampered traffic rejected
- View change / new view with Byzantine-safe prepared certificates
- WAL-backed persistence of the ordered log; restart recovery
- Primary-only writes — backups reject with `ErrNotPrimary` and advertise the primary
- Fault-injection e2e suite: reordering, partitions, equivocation, fake commits,
  primary silence/view change, duplicate delivery, no conflicting commit at a sequence

**Quality**
- Unit, integration, and end-to-end tests plus `-race` runs
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
`GET` / `PUT` / `TTL` / `DEL` commands (see [Protocol](#-protocol) below).

<br>

## 📡 Protocol

Cachey communicates over TCP using NDJSON — each request and response
is one JSON object followed by a newline.

| Field | Type | Description |
|---|---|---|
| `Type` | `string` | Command name: `GET`, `PUT`, `DEL`, `TTL`, or `ALV` |
| `Key` | `string` | Key to read, write, expire, or delete |
| `Val` | `string` | Value for `PUT`; returned value for `GET` |
| `TTL` | `int64` | Millisecond lifetime for `TTL` (optional) |

<table>
<tr><th>Requests</th><th>Responses</th></tr>
<tr valign="top">
<td>

```json
{"Type":"PUT","Key":"foo","Val":"bar"}
{"Type":"GET","Key":"foo","Val":""}
{"Type":"TTL","Key":"foo","TTL":60000}
{"Type":"DEL","Key":"foo","Val":""}
```

</td>
<td>

```json
{"Type":"PUT","Key":"foo","Val":"bar"}
{"Type":"GET","Key":"foo","Val":"bar"}
{"Type":"TTL","Key":"foo","TTL":0}
{"Type":"DEL","Key":"foo","Val":""}
```

</td>
</tr>
</table>

Failed commands are never dropped — the server replies with a
gRPC-style status object instead:

```json
{"code":5,"message":"invalid key"}
```

| `code` | gRPC name | Meaning |
|---|---|---|
| `3` | `InvalidArgument` | Malformed request |
| `5` | `NotFound` | Missing key |
| `12` | `Unimplemented` | Unknown command |

When a node is part of a replicated cluster, a write or read sent to a
non-leader fails with a `14` (`Unavailable`) status that carries the
current leader's address — the Go client can extract it with
`client.RedirectLeader(err)` and reconnect automatically. Raft nodes expose
this redirect over the wire; PBFT stores reject non-primary access with
`ErrNotPrimary` and advertise the primary through `Leader()`.

The Go client in `pkg/client` handles JSON serialization, newline
framing, and status errors automatically.

<br>

## 🏗️ Architecture

Cachey is built around one question: **how does a single node become a
distributed cluster?** The layers below show what runs today and where
the distributed design is headed.

### Single node (`cacheyd`, today)

```text
Client
  │
  │  TCP / NDJSON
  ▼
Server ──▶ Handler ──▶ Store ──▶ In-memory map
                            │
                            ▼
                           WAL  (crash-safe, snapshots + rotation)
```

### Raft-replicated store (library, tested)

The same handler serves reads and writes through a **replicated store**
(`server.ClusterStore`): every mutation is proposed to the raft leader,
replicated to a quorum of followers, durably logged, and only then
applied to each node's FSM.

```text
Client ──▶ Handler ──▶ ClusterStore
                         │
                     Raft Node ──leader election + log replication──▶ peers
                         │
                  WAL (durable log) ──▶ FSM (CacheyStore)
                         │
                  snapshots / compaction
```

| Package | Responsibility |
|---|---|
| `internal/protocol` | Defines commands and JSON parsing |
| `internal/raft` | Consensus core: election, replication, membership, read index, snapshots |
| `internal/pbft` | BFT consensus core: normal case, view change, Ed25519 auth, WAL recovery |
| `internal/server` | TCP dispatch; `ClusterStore`/`PbftClusterStore` adapt a node to `store.Store` |
| `internal/store` | Storage interface, in-memory store, and replicated FSM |
| `internal/wal` | Durable log backend for the store, raft log, and pbft log |
| `pkg/client` | TCP client with `RedirectLeader` for cluster redirects |
| `cmd/cacheyd` | Runs a single node (`cacheyd <addr> [data-dir]`) |

### PBFT-replicated store (library, tested)

PBFT is the Byzantine counterpart of the raft store: writes are ordered by
the view's **primary** through pre-prepare / prepare / commit, executed by
every replica in sequence, and exposed to clients by
`server.PbftClusterStore`. Backups reject writes with `pbft.ErrNotPrimary`
and advertise the primary, and every message is Ed25519-signed so a faulty
replica cannot equivocate undetected.

```text
Client ──▶ Handler ──▶ PbftClusterStore
                           │
                       PBFT Replica ──signed pre-prepare / prepare / commit──▶ peers
                           │
                    WAL (ordered log) ──▶ FSM (CacheyStore)
                           │
                 view change (on primary failure)
```

Fault-injection e2e tests cover Byzantine primaries and backups, view
changes under primary silence, network partitions, message loss and
reordering, and duplicate delivery.

### Target: sharded cluster (roadmap)

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

The roadmap splits two responsibilities:

- **Sharding** — decides which shard owns a key and spreads load across nodes
- **Raft** — keeps each shard's replicas consistent (already implemented and tested here)

Next up is wiring `cacheyd` to start and join a Raft group; then keys are
sharded across groups.

<br>

## 💾 Durability & WAL Persistence

> **Status:** implemented (v1). Active WAL + temporary WAL + snapshot,
> with a single writer goroutine and a background sealing/rotation
> manager. Run `cacheyd <addr> <data-dir>` to enable durability.

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
- **Continuity check** — replay enforces strictly increasing indices; a gap is corruption (fail-fast), while records already covered by the snapshot (left on disk by a crash during sealing) are replayed idempotently (last write wins)
- **Case A** (crash mid-sealing) — replay snapshot + both WALs, then rebuild them into one active WAL via `wal.ndjson.rebuild.tmp` + fsync + rename
- **Case B** (crash after snapshot, before rotation) — replay snapshot + WAL idempotently (last write wins)
- **Case C** (stray temporary WAL) — defensive check; a valid temporary WAL is verified and restored as the active WAL
- **`snapshot.tmp`** — never trusted as a completed snapshot; discarded when a complete `snapshot` exists

### Implementation order

`WALRecord`/`log_index` → WAL Writer → bounded WAL channel → mutation + buffered `ResultCh` → synchronous fsync → WAL Manager → `StartSealing` + buffered ack + idempotency → atomic snapshot → temporary WAL + hold queue → `FinishRotation` → atomic rename + directory fsync → `RotationComplete` → retry/backoff → crash recovery → partial-WAL recovery → continuity validation → failure/stress tests.

<br>

## 🛠️ Development

*For contributors working from a cloned copy of the repo:*

| Task | Command |
|---|---|
| Run the full test suite | `make test` |
| Run static analysis | `make vet` |
| Run tests and static analysis | `make check` |
| Run the race detector | `make race` |
| Format Go files | `make fmt` |
| Build the agent harness | `make build-harness` |

### AI Coding-Agent Harness

The `harness run` command drives a configured external coding agent through a
bounded task, verification, and feedback loop. Update the task in
[.harness/tasks/current.md](.harness/tasks/current.md), configure the agent and
allowed verification commands in [.harness/config.toml](.harness/config.toml),
then run:

```sh
go run ./cmd/harness run
```

The harness does not commit, push, reset, or discard repository changes.

<br>

## 📄 License

See [LICENSE](LICENSE) for details.

<br>

<div align="center">

*Built one primitive at a time.*

</div>