package protocol

type CommandType string

const (
	GET CommandType = "GET"
	PUT CommandType = "PUT"
	DEL CommandType = "DEL"
	ALV CommandType = "ALV"
	TTL CommandType = "TTL"
	// JOIN is a cluster-control command: a brand-new node asks an existing
	// member's client-facing server to add it to the replicated cluster (Key is
	// the joining node id, Val its JSON {raft, client} addresses). A standalone
	// server rejects it; it is not part of the normal cache protocol.
	JOIN CommandType = "JOIN"
)

type Command struct {
	Type CommandType
	Key  string
	Val  string
	// TTL is the expiration in milliseconds from now, used by the TTL command.
	TTL int64 `json:"TTL,omitempty"`
}
