package protocol

type CommandType string

const (
	GET CommandType = "GET"
	PUT CommandType = "PUT"
	DEL CommandType = "DEL"
	ALV CommandType = "ALV"
	TTL CommandType = "TTL"
)

type Command struct {
	Type CommandType
	Key  string
	Val  string
	// TTL is the expiration in milliseconds from now, used by the TTL command.
	TTL int64 `json:"TTL,omitempty"`
}
