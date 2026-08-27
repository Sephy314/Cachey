package protocol

type CommandType string

const (
	GET CommandType = "GET"
	PUT CommandType = "PUT"
	DEL CommandType = "DEL"
	ALV CommandType = "ALV"
)

type Command struct {
	Type CommandType
	Key  string
	Val  string
}


