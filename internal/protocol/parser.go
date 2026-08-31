package protocol

import (
	"encoding/json"
	"log"
)

func DeSerializeCommand(data []byte) (*Command, error) {
	// data: {"command":"FOO", "key":"bar", "val":"baz"}
	if len(data) == 0 {
		return nil, ErrorCodeInvalidCommand
	}

	log.Default().Printf("Deserializing command: %s", string(data))

	var cmd Command
	err := json.Unmarshal(data, &cmd)

	if err != nil {
		return nil, ErrorCodeInvalidCommand
	}
	return &cmd, nil

}

func (cmd *Command) Serialize() ([]byte, error) {
	// data: {"command":"FOO", "key":"bar", "val":"baz"}
	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, ErrorCodeInvalidCommand
	}
	return data, nil
}
