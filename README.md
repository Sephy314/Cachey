# Cachey

Cachey is a distributed key-value cache store written in Go. The long-term
goal is to build a practical distributed system with Raft-based consensus for
replication and sharding for horizontal scale.

The current implementation is the first single-node foundation: an in-memory
store exposed over TCP with newline-delimited JSON (NDJSON) request and
response framing. The project will grow these primitives into a replicated,
sharded KV store step by step.

## Vision

Cachey is designed to evolve from a local in-memory store into a distributed
cache with these core properties:

- Raft-based consensus for a consistent replicated state machine
- Sharding to partition keys and scale horizontally across nodes
- Failure detection and recovery for node and network failures
- A stable client protocol that hides cluster details from clients

These capabilities are architectural goals, not claims about the current
implementation.

## Current Features

- TCP client and server
- Newline-delimited JSON protocol
- In-memory key-value storage
- `GET`, `PUT`, and `DEL` commands
- Multiple commands over one client connection
- Unit and TCP integration tests
- GitHub Actions checks for tests and static analysis

## Requirements

- Go `1.26.7`

## Quick Start

Start a Cachey server:

```sh
go run ./cmd/cacheyd 127.0.0.1:8080
```

In another terminal, run the example client:

```sh
go run ./test/main.go 127.0.0.1:8080
```

The example client stores, retrieves, and deletes `testKey`.

## Protocol

Cachey communicates over TCP using NDJSON. Each request and response is one
JSON object followed by a newline.

Commands use the following fields:

| Field | Type | Description |
| --- | --- | --- |
| `Type` | string | Command name: `GET`, `PUT`, or `DEL` |
| `Key` | string | Key to read, write, or delete |
| `Val` | string | Value for `PUT`; returned value for `GET` |

Example requests:

```json
{"Type":"PUT","Key":"foo","Val":"bar"}
{"Type":"GET","Key":"foo","Val":""}
{"Type":"DEL","Key":"foo","Val":""}
```

Example responses:

```json
{"Type":"PUT","Key":"foo","Val":"bar"}
{"Type":"GET","Key":"foo","Val":"bar"}
{"Type":"DEL","Key":"foo","Val":""}
```

The Go client in `pkg/client` handles JSON serialization and newline
framing automatically.

## Architecture

### Current Foundation

```text
Client
  |
  | TCP / NDJSON
  v
Server -> Handler -> Store -> In-memory map
```

- `internal/protocol` defines commands and JSON parsing.
- `internal/server` accepts TCP connections and dispatches requests.
- `internal/store` defines the storage interface and in-memory store.
- `pkg/client` provides a small TCP client.
- `cmd/cacheyd` starts the server.

### Target Distributed Architecture

```text
                             Client
                                |
                         Cluster Router
                                |
                +---------------+---------------+
                |                               |
             Shard A                         Shard B
                |                               |
        Raft group: A1, A2, A3         Raft group: B1, B2, B3
                |                               |
        Replicated state                Replicated state
```

The target design separates two responsibilities:

- **Sharding** decides which shard owns a key and distributes load across the
	cluster.
- **Raft** keeps the replicas within each shard consistent and supports leader
	election and log replication.

The implementation will introduce these boundaries incrementally rather than
assuming that a single-node in-memory map already provides distributed
guarantees.

## Development

Run the full test suite:

```sh
go test ./...
```

Run static analysis:

```sh
go vet ./...
```

Format Go files before submitting changes:

```sh
gofmt -w .
```

## Roadmap

The roadmap follows the target architecture while keeping each step testable:

1. Harden the single-node storage and wire protocol
2. Add persistent log and snapshot primitives
3. Add node-to-node communication and cluster membership
4. Implement Raft leader election, log replication, and recovery
5. Turn each Raft group into a replicated shard
6. Add key routing, shard rebalancing, and failure handling
7. Add TTL/expiration, benchmarks, and operational tooling

## Project Status

Cachey is an experimental learning project working toward a Raft-replicated
and sharded distributed KV cache. The storage model and wire protocol may
change significantly as the architecture evolves, and it is not intended for
production use.

## License

See [LICENSE](LICENSE) for license details.
