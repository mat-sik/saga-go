package kgoconsumer

import "time"

type config struct {
	backoffConfig    backoffConfig
	processingConfig processingConfig
}

func newConfig(opts ...Option) config {
	cfg := newDefaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func newDefaultConfig() config {
	return config{
		backoffConfig: backoffConfig{
			base:   200 * time.Millisecond,
			max:    10 * time.Second,
			factor: 2,
		},
		processingConfig: processingConfig{
			timeout: 5 * time.Minute,
		},
	}
}

type backoffConfig struct {
	base   time.Duration
	max    time.Duration
	factor float64
}

type Option func(*config)

func WithBackoffBase(base time.Duration) Option {
	return func(c *config) {
		c.backoffConfig.base = base
	}
}

func WithBackoffMax(max time.Duration) Option {
	return func(c *config) {
		c.backoffConfig.max = max
	}
}

func WithBackoffFactor(factor float64) Option {
	return func(c *config) {
		c.backoffConfig.factor = factor
	}
}

type processingConfig struct {
	timeout time.Duration
}

func WithProcessingTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.processingConfig.timeout = timeout
	}
}
