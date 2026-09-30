package iface

import (
	"errors"
	"fmt"
)

// Code classifies an error so callers can react to it after it has crossed
// the network.
type Code uint8

const (
	CodeUnknown     Code = iota
	CodeNotFound         // the path, upload or chunk does not exist
	CodeConflict         // a compare-and-swap on a version failed
	CodeInvalid          // the request is malformed
	CodeUnavailable      // not enough nodes, peer down, or timed out; retry may help
	CodeRetry            // state not ready yet (replicas not reported); retry shortly
	CodeInternal         // a bug
	CodeCorrupt          // stored data does not match its hash; no intact copy was served
	CodeNotLeader        // this metadata peer is not the leader; Msg names the leader, or is empty if unknown
)

var codeNames = [...]string{"unknown", "not_found", "conflict", "invalid", "unavailable", "retry", "internal", "corrupt", "not_leader"}

func (c Code) String() string {
	if int(c) < len(codeNames) {
		return codeNames[c]
	}
	return fmt.Sprintf("code(%d)", c)
}

// ParseCode returns the Code named name, or CodeUnknown.
func ParseCode(name string) Code {
	for i, n := range codeNames {
		if n == name {
			return Code(i)
		}
	}
	return CodeUnknown
}

// Error is an error with a Code.
type Error struct {
	Code Code
	Msg  string
}

func (e *Error) Error() string { return e.Code.String() + ": " + e.Msg }

// Errorf returns an *Error with the given code.
func Errorf(c Code, format string, args ...any) error {
	return &Error{Code: c, Msg: fmt.Sprintf(format, args...)}
}

// CodeOf returns err's code, CodeNotFound for ErrNotFound, or CodeUnknown.
func CodeOf(err error) Code {
	var e *Error
	switch {
	case err == nil:
		return CodeUnknown
	case errors.As(err, &e):
		return e.Code
	case errors.Is(err, ErrNotFound):
		return CodeNotFound
	}
	return CodeUnknown
}

// AsError converts err to an *Error for the wire, using def when err carries
// no code.
func AsError(err error, def Code) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	c := CodeOf(err)
	if c == CodeUnknown {
		c = def
	}
	return &Error{Code: c, Msg: err.Error()}
}
