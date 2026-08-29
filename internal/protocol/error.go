package protocol

import "errors"

var (
	ErrorCodeInvalidCommand = Statusf(CodeInvalidArgument, "invalid command")
	ErrorCodeInvalidKey     = errors.New("invalid key")
	ErrorCodeInvalidValue   = errors.New("invalid value")
)
