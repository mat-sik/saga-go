package kgoconsumer

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

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

type clientConfig struct {
	fetchMaxBytes          int32
	fetchMaxPartitionBytes int32
	onRebalanceBlocked     func()
}

func newClientConfig(opts ...ClientOption) clientConfig {
	cfg := newDefaultClientConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func newDefaultClientConfig() clientConfig {
	return clientConfig{
		fetchMaxBytes:          fetchMaxBytesUnset,
		fetchMaxPartitionBytes: fetchMaxPartitionBytesUnset,
		onRebalanceBlocked:     onRebalanceBlockedDefault,
	}
}

const (
	fetchMaxBytesUnset          = -1
	fetchMaxPartitionBytesUnset = -1
)

var onRebalanceBlockedDefault = func() {}

type ClientOption func(*clientConfig)

func WithFetchMaxBytes(n int32) ClientOption {
	return func(c *clientConfig) {
		c.fetchMaxBytes = n
	}
}

func WithFetchMaxPartitionBytes(n int32) ClientOption {
	return func(c *clientConfig) {
		c.fetchMaxPartitionBytes = n
	}
}

func WithOnRebalanceBlocked(fn func()) ClientOption {
	return func(c *clientConfig) {
		c.onRebalanceBlocked = fn
	}
}

func (c clientConfig) toKgoOpts() []kgo.Opt {
	var opts []kgo.Opt
	if c.fetchMaxBytes != fetchMaxBytesUnset {
		opts = append(opts, kgo.FetchMaxBytes(c.fetchMaxBytes))
	}
	if c.fetchMaxPartitionBytes != fetchMaxPartitionBytesUnset {
		opts = append(opts, kgo.FetchMaxPartitionBytes(c.fetchMaxPartitionBytes))
	}
	return opts
}
