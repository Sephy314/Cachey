package store

import "errors"

var (
	ErrorCodeInvalidCommand = errors.New("invalid command")
	ErrorCodeInvalidKey     = errors.New("invalid key")
	ErrorCodeInvalidValue   = errors.New("invalid value")
)
