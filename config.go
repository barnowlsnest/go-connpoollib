package connpool

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/barnowlsnest/go-asynctasklib/v2/pkg/retry"
)

const (
	defaultNetwork           = "tcp"
	defaultHost              = "127.0.0.1"
	defaultMinIdle           = 1
	defaultMaxPoolSize       = 10
	defaultMaxConcurrency    = 2
	defaultIdleTimeout       = 30 * time.Second
	defaultLifespanTimeout   = 5 * time.Minute
	defaultConnectionTimeout = 5 * time.Second
	defaultAcquireTimeout    = 5 * time.Second
	defaultKeepAliveTimeout  = 30 * time.Second
	defaultDialMaxAttempts   = 3
	defaultDialBackoffBase   = 100 * time.Millisecond
	defaultDialBackoffMax    = 2 * time.Second
	defaultDialBackoffJitter = true
)

// noFinalDelay stops the retry loop from sleeping after the last failed
// attempt, when no further attempt follows.
type noFinalDelay struct {
	retry.Strategy
	attempts int
}

func (s noFinalDelay) Delay(attempt int) time.Duration {
	if attempt >= s.attempts-1 {
		return 0
	}
	return s.Strategy.Delay(attempt)
}

func isUnixNetwork(network string) bool {
	switch network {
	case "unix", "unixgram", "unixpacket":
		return true
	}
	return false
}

type (
	Option func(*Config)

	Config struct {
		dialBackoff       retry.Strategy
		dialer            *net.Dialer
		network           string
		host              string
		dialMaxAttempts   int
		maxConcurrency    int
		maxPoolSize       int
		idleTimeout       time.Duration
		lifespanTimeout   time.Duration
		acquireTimeout    time.Duration
		dialBackoffBase   time.Duration
		dialBackoffMax    time.Duration
		minIdle           int
		port              int
		dialBackoffJitter bool
	}
)

func WithNetwork(network string) Option {
	return func(c *Config) {
		c.network = network
	}
}

func WithHost(host string) Option {
	return func(c *Config) {
		c.host = host
	}
}

func WithPort(port int) Option {
	return func(c *Config) {
		c.port = port
	}
}

func WithMinIdle(minIdle int) Option {
	return func(c *Config) {
		c.minIdle = minIdle
	}
}

func WithMaxPoolSize(maxPoolSize int) Option {
	return func(c *Config) {
		c.maxPoolSize = maxPoolSize
	}
}

func WithMaxConcurrency(maxConcurrency int) Option {
	return func(c *Config) {
		c.maxConcurrency = maxConcurrency
	}
}

func WithIdleTimeout(idleTimeout time.Duration) Option {
	return func(c *Config) {
		c.idleTimeout = idleTimeout
	}
}

func WithLifespanTimeout(lifespanTimeout time.Duration) Option {
	return func(c *Config) {
		c.lifespanTimeout = lifespanTimeout
	}
}

func WithDialer(dialer *net.Dialer) Option {
	return func(c *Config) {
		c.dialer = dialer
	}
}

func WithAcquireTimeout(acquireTimeout time.Duration) Option {
	return func(c *Config) {
		c.acquireTimeout = acquireTimeout
	}
}

// WithDialMaxAttempts sets the total number of dial attempts (the first try
// included) made before Acquire gives up on opening a new connection.
func WithDialMaxAttempts(attempts int) Option {
	return func(c *Config) {
		c.dialMaxAttempts = attempts
	}
}

// WithDialBackoff sets the exponential backoff between dial attempts: the
// n-th retry waits base * 2^n, capped at maxDelay.
func WithDialBackoff(base, maxDelay time.Duration) Option {
	return func(c *Config) {
		c.dialBackoffBase = base
		c.dialBackoffMax = maxDelay
	}
}

// WithDialBackoffJitter enables full jitter on dial backoff delays, spreading
// reconnects of many clients after a shared upstream failure.
func WithDialBackoffJitter(jitter bool) Option {
	return func(c *Config) {
		c.dialBackoffJitter = jitter
	}
}

func NewConfig(opts ...Option) (*Config, error) {
	cfg := &Config{
		host:            defaultHost,
		network:         defaultNetwork,
		minIdle:         defaultMinIdle,
		maxPoolSize:     defaultMaxPoolSize,
		maxConcurrency:  defaultMaxConcurrency,
		idleTimeout:     defaultIdleTimeout,
		lifespanTimeout: defaultLifespanTimeout,
		acquireTimeout:  defaultAcquireTimeout,
		dialer: &net.Dialer{
			Timeout:   defaultConnectionTimeout,
			KeepAlive: defaultKeepAliveTimeout,
		},
		dialMaxAttempts:   defaultDialMaxAttempts,
		dialBackoffBase:   defaultDialBackoffBase,
		dialBackoffMax:    defaultDialBackoffMax,
		dialBackoffJitter: defaultDialBackoffJitter,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	cfg.dialBackoff = noFinalDelay{
		Strategy: retry.NewExponentialBackoff(
			retry.WithBaseDelay(cfg.dialBackoffBase),
			retry.WithMaxDelay(cfg.dialBackoffMax),
			retry.WithJitter(cfg.dialBackoffJitter),
		),
		attempts: cfg.dialMaxAttempts,
	}

	return cfg, nil
}

func (cfg *Config) Network() string {
	return cfg.network
}

func (cfg *Config) Host() string {
	return cfg.host
}

func (cfg *Config) Port() int {
	return cfg.port
}

func (cfg *Config) MinIdle() int {
	return cfg.minIdle
}

func (cfg *Config) MaxPoolSize() int {
	return cfg.maxPoolSize
}

func (cfg *Config) MaxConcurrency() int {
	return cfg.maxConcurrency
}

func (cfg *Config) IdleTimeout() time.Duration {
	return cfg.idleTimeout
}

func (cfg *Config) LifespanTimeout() time.Duration {
	return cfg.lifespanTimeout
}

func (cfg *Config) AcquireTimeout() time.Duration {
	return cfg.acquireTimeout
}

func (cfg *Config) DialMaxAttempts() int {
	return cfg.dialMaxAttempts
}

func (cfg *Config) DialBackoffBase() time.Duration {
	return cfg.dialBackoffBase
}

func (cfg *Config) DialBackoffMax() time.Duration {
	return cfg.dialBackoffMax
}

func (cfg *Config) DialBackoffJitter() bool {
	return cfg.dialBackoffJitter
}

func (cfg *Config) Addr() string {
	if isUnixNetwork(cfg.network) {
		return cfg.host
	}
	return net.JoinHostPort(cfg.host, strconv.Itoa(cfg.port))
}

//nolint:gocyclo
func (cfg *Config) validate() error {
	switch {
	case cfg.network == "":
		return fmt.Errorf("network is required: %w", ErrInvalidConnPoolCfg)
	case cfg.host == "":
		return fmt.Errorf("host is required: %w", ErrInvalidConnPoolCfg)
	case !isUnixNetwork(cfg.network) && (cfg.port <= 0 || cfg.port > 65535):
		return fmt.Errorf("port must be between 1 and 65535: %w", ErrInvalidConnPoolCfg)
	case cfg.minIdle < 0:
		return fmt.Errorf("minIdle must be greater than or equal to 0: %w", ErrInvalidConnPoolCfg)
	case cfg.maxPoolSize <= 0:
		return fmt.Errorf("maxPoolSize must be greater than 0: %w", ErrInvalidConnPoolCfg)
	case cfg.minIdle > cfg.maxPoolSize:
		return fmt.Errorf("minIdle must be less than or equal to maxPoolSize: %w", ErrInvalidConnPoolCfg)
	case cfg.maxConcurrency <= 0:
		return fmt.Errorf("maxConcurrency must be greater than 0: %w", ErrInvalidConnPoolCfg)
	case cfg.idleTimeout <= 0:
		return fmt.Errorf("idleTimeout must be greater than 0: %w", ErrInvalidConnPoolCfg)
	case cfg.lifespanTimeout <= 0:
		return fmt.Errorf("lifespanTimeout must be greater than 0: %w", ErrInvalidConnPoolCfg)
	case cfg.dialer == nil:
		return fmt.Errorf("dialer is required: %w", ErrInvalidConnPoolCfg)
	case cfg.acquireTimeout <= 0:
		return fmt.Errorf("acquireTimeout must be greater than 0: %w", ErrInvalidConnPoolCfg)
	case cfg.dialMaxAttempts <= 0:
		return fmt.Errorf("dialMaxAttempts must be greater than 0: %w", ErrInvalidConnPoolCfg)
	case cfg.dialBackoffBase <= 0:
		return fmt.Errorf("dialBackoffBase must be greater than 0: %w", ErrInvalidConnPoolCfg)
	case cfg.dialBackoffMax < cfg.dialBackoffBase:
		return fmt.Errorf("dialBackoffMax must be greater than or equal to dialBackoffBase: %w", ErrInvalidConnPoolCfg)
	default:
		return nil
	}
}
