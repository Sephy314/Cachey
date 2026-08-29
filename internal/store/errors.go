package store

import (
	"errors"

	"github.com/Sephy314/Cachey/internal/protocol"
)

var (
	ErrorCodeInvalidCommand = errors.New("invalid command")
	ErrorCodeInvalidKey     = protocol.Statusf(protocol.CodeNotFound, "invalid key")
	ErrorCodeInvalidValue   = errors.New("invalid value")
)
