# Cachey

Cachey is a small key-value store written in Go. It exposes an in-memory
store over a TCP connection and uses newline-delimited JSON (NDJSON) for
request and response framing.

The project is intentionally small and educational. Its current focus is on
understanding the building blocks of a distributed system: a network
protocol, a request handler, a storage interface, and a concurrent TCP
server.

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

```text
Client
	|
	| TCP / NDJSON
	v
Server -> Handler -> Store
										 |
										 v
							 In-memory map
```

- `internal/protocol` defines commands and JSON parsing.
- `internal/server` accepts TCP connections and dispatches requests.
- `internal/store` defines the storage interface and in-memory store.
- `pkg/client` provides a small TCP client.
- `cmd/cacheyd` starts the server.

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

The following ideas are intentionally not part of the current implementation:

- Persistent storage
- Node-to-node communication
- Replication and sharding
- Failure detection
- TTL and expiration
- Benchmarks
- Consensus and Raft-based replication

## Project Status

Cachey is an experimental learning project. The storage model and wire
protocol may change as the project evolves, and it is not intended for
production use.

## License

See [LICENSE](LICENSE) for license details.
