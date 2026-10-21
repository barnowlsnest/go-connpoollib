package connpool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type ConfigSuite struct {
	suite.Suite
}

func TestConfigSuite(t *testing.T) {
	suite.Run(t, new(ConfigSuite))
}

func (s *ConfigSuite) TestAddrIPv6() {
	cfg, err := NewConfig(WithHost("::1"), WithPort(5432))
	s.Require().NoError(err)
	s.Equal("[::1]:5432", cfg.Addr())
}

func (s *ConfigSuite) TestValidation() {
	tests := map[string][]Option{
		"empty network":     {WithPort(1), WithNetwork("")},
		"port too large":    {WithPort(70000)},
		"minIdle above max": {WithPort(1), WithMinIdle(5), WithMaxPoolSize(2)},
		"nil dialer":        {WithPort(1), WithDialer(nil)},
		"nil dial func":     {WithPort(1), WithDialFunc(nil)},
	}
	for name, opts := range tests {
		s.Run(name, func() {
			_, err := NewConfig(opts...)
			s.ErrorIs(err, ErrInvalidConnPoolCfg)
		})
	}
}

func (s *ConfigSuite) TestUnixSocketNeedsNoPort() {
	cfg, err := NewConfig(WithNetwork("unix"), WithHost("/tmp/pool.sock"))
	s.Require().NoError(err)
	s.Equal("/tmp/pool.sock", cfg.Addr())
}

func (s *ConfigSuite) TestDialDelay() {
	cfg, err := NewConfig(WithPort(1), WithDialMaxAttempts(4), WithDialBackoff(time.Second, 3*time.Second), WithDialBackoffJitter(false))
	s.Require().NoError(err)
	s.Equal(time.Second, cfg.dialDelay(0))
	s.Equal(2*time.Second, cfg.dialDelay(1))
	s.Equal(3*time.Second, cfg.dialDelay(2)) // capped
	s.Zero(cfg.dialDelay(3))                 // no delay after the last attempt
}
