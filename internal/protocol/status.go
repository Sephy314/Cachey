package protocol

import (
	"fmt"
	"strconv"
)

// Code is a gRPC-compatible status code (google.rpc.Code).
type Code uint32

const (
	CodeOK                 Code = 0
	CodeCanceled           Code = 1
	CodeUnknown            Code = 2
	CodeInvalidArgument    Code = 3
	CodeDeadlineExceeded   Code = 4
	CodeNotFound           Code = 5
	CodeAlreadyExists      Code = 6
	CodePermissionDenied   Code = 7
	CodeResourceExhausted  Code = 8
	CodeFailedPrecondition Code = 9
	CodeAborted            Code = 10
	CodeOutOfRange         Code = 11
	CodeUnimplemented      Code = 12
	CodeInternal           Code = 13
	CodeUnavailable        Code = 14
	CodeDataLoss           Code = 15
	CodeUnauthenticated    Code = 16
)

// String returns the canonical gRPC name for c.
func (c Code) String() string {
	switch c {
	case CodeOK:
		return "OK"
	case CodeCanceled:
		return "Canceled"
	case CodeUnknown:
		return "Unknown"
	case CodeInvalidArgument:
		return "InvalidArgument"
	case CodeDeadlineExceeded:
		return "DeadlineExceeded"
	case CodeNotFound:
		return "NotFound"
	case CodeAlreadyExists:
		return "AlreadyExists"
	case CodePermissionDenied:
		return "PermissionDenied"
	case CodeResourceExhausted:
		return "ResourceExhausted"
	case CodeFailedPrecondition:
		return "FailedPrecondition"
	case CodeAborted:
		return "Aborted"
	case CodeOutOfRange:
		return "OutOfRange"
	case CodeUnimplemented:
		return "Unimplemented"
	case CodeInternal:
		return "Internal"
	case CodeUnavailable:
		return "Unavailable"
	case CodeDataLoss:
		return "DataLoss"
	case CodeUnauthenticated:
		return "Unauthenticated"
	}
	return "Code(" + strconv.FormatUint(uint64(c), 10) + ")"
}

// Status is a gRPC-style error carrying a code and a message. It is the
// on-the-wire error response for the Cachey protocol.
type Status struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
}

// Error implements error.
func (s *Status) Error() string { return s.Message }

// Statusf builds a *Status error with the given code and formatted message.
func Statusf(code Code, format string, args ...any) *Status {
	return &Status{Code: code, Message: fmt.Sprintf(format, args...)}
}
