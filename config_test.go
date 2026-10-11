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

func (s *ConfigSuite) TestDialerNotShared() {
	a, err := NewConfig(WithPort(1))
	s.Require().NoError(err)
	b, err := NewConfig(WithPort(1))
	s.Require().NoError(err)
	s.NotSame(a.dialer, b.dialer)
}

func (s *ConfigSuite) TestNoFinalDelay() {
	cfg, err := NewConfig(WithPort(1), WithDialMaxAttempts(3), WithDialBackoff(time.Second, time.Minute), WithDialBackoffJitter(false))
	s.Require().NoError(err)
	s.Positive(cfg.dialBackoff.Delay(0))
	s.Positive(cfg.dialBackoff.Delay(1))
	s.Zero(cfg.dialBackoff.Delay(2))
}
