package connpool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigAddrIPv6(t *testing.T) {
	cfg, err := NewConfig(WithHost("::1"), WithPort(5432))
	require.NoError(t, err)
	assert.Equal(t, "[::1]:5432", cfg.Addr())
}

func TestConfigValidation(t *testing.T) {
	tests := map[string][]Option{
		"empty network":     {WithPort(1), WithNetwork("")},
		"port too large":    {WithPort(70000)},
		"minIdle above max": {WithPort(1), WithMinIdle(5), WithMaxPoolSize(2)},
	}
	for name, opts := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := NewConfig(opts...)
			assert.ErrorIs(t, err, ErrInvalidConnPoolCfg)
		})
	}
}

func TestConfigUnixSocketNeedsNoPort(t *testing.T) {
	cfg, err := NewConfig(WithNetwork("unix"), WithHost("/tmp/pool.sock"))
	require.NoError(t, err)
	assert.Equal(t, "/tmp/pool.sock", cfg.Addr())
}

func TestConfigDialerNotShared(t *testing.T) {
	a, err := NewConfig(WithPort(1))
	require.NoError(t, err)
	b, err := NewConfig(WithPort(1))
	require.NoError(t, err)
	assert.NotSame(t, a.dialer, b.dialer)
}

func TestNoFinalDelay(t *testing.T) {
	cfg, err := NewConfig(WithPort(1), WithDialMaxAttempts(3), WithDialBackoff(time.Second, time.Minute), WithDialBackoffJitter(false))
	require.NoError(t, err)
	assert.Positive(t, cfg.dialBackoff.Delay(0))
	assert.Positive(t, cfg.dialBackoff.Delay(1))
	assert.Zero(t, cfg.dialBackoff.Delay(2))
}
