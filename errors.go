package connpool

import (
	"errors"
)

var (
	// ErrNilConfig is returned by New when the config is nil.
	ErrNilConfig = errors.New("nil config")
	// ErrInvalidConnPoolCfg wraps every config validation error.
	ErrInvalidConnPoolCfg = errors.New("invalid conn pool config")
	// ErrPoolAlreadyClosed is returned when the pool has been closed.
	ErrPoolAlreadyClosed = errors.New("conn pool already closed")
	// ErrDialFailed wraps the error of a failed attempt to open a connection.
	ErrDialFailed = errors.New("conn pool dial failed")
)
