package connpool

import (
	"errors"
)

var (
	ErrNilConfig          = errors.New("nil config")
	ErrInvalidConnPoolCfg = errors.New("invalid conn pool config")
	ErrPoolAlreadyClosed  = errors.New("conn pool already closed")
	ErrDialFailed         = errors.New("conn pool dial failed")
)
