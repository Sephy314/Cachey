package server

import (
	"encoding/json"
	"errors"

	"github.com/Sephy314/Cachey/internal/protocol"
)

var (
	ErrorCodeInvalidCommand = protocol.Statusf(protocol.CodeUnimplemented, "invalid command")
	ErrorCodeInvalidKey     = errors.New("invalid key")
	ErrorCodeInvalidValue   = errors.New("invalid value")
)

// statusBytes serializes err as a gRPC-style status response. Errors that are
// *protocol.Status keep their code; anything else maps to CodeUnknown.
func statusBytes(err error) []byte {
	code := protocol.CodeUnknown
	var st *protocol.Status
	if errors.As(err, &st) {
		code = st.Code
	}
	b, marshalErr := json.Marshal(protocol.Status{Code: code, Message: err.Error()})
	if marshalErr != nil {
		return []byte(`{"code":13,"message":"internal error"}`)
	}
	return b
}
