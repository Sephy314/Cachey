<div align="center">

# ⚡ Cachey

### A distributed key-value cache store, written in Go

*Building toward Raft-based consensus and sharding — one solid step at a time.*

`Go 1.26.7` · `Experimental` · [License](LICENSE)

</div>

<br>

## 🌱 Overview

The current implementation is the **first single-node foundation**: an
in-memory store exposed over TCP with newline-delimited JSON (NDJSON)
request and response framing.

This project will grow these primitives, step by step, into a
**replicated, sharded** key-value store.

<br>

## 🎯 Vision

Cachey is designed to evolve from a local in-memory store into a
distributed cache with these core properties:

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
go install github.com/Sephy314/Cachey/cmd/cacheyd@latest
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

### Foundation

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

### Target Distributed Architecture

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

## 🛠️ Development

*For contributors working from a cloned copy of the repo:*

| Task | Command |
|---|---|
| Run the full test suite | `go test ./...` |
| Run static analysis | `go vet ./...` |
| Format Go files | `gofmt -w .` |

<br>

## 🗺️ Roadmap

The roadmap follows the target architecture, keeping each step testable.

- [x] **1.** Harden the single-node storage and wire protocol
- [ ] **2.** Add persistent log and snapshot primitives
- [ ] **3.** Add node-to-node communication and cluster membership
- [ ] **4.** Implement Raft leader election, log replication, and recovery
- [ ] **5.** Turn each Raft group into a replicated shard
- [ ] **6.** Add key routing, shard rebalancing, and failure handling
- [ ] **7.** Add TTL/expiration, benchmarks, and operational tooling

<br>

## ⚠️ Project Status

Cachey is an **experimental learning project** working toward a
Raft-replicated, sharded distributed KV cache. The storage model and
wire protocol may change significantly as the architecture evolves.

**It is not intended for production use.**

<br>

## 📄 License

See [LICENSE](LICENSE) for details.

<br>

<div align="center">

*Built one primitive at a time.*

</div>