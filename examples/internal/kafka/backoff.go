package kafka

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

type backoff struct {
	attempt int
	base    time.Duration
	max     time.Duration
	factor  float64
}

func newBackoff(base, max time.Duration, factor float64) *backoff {
	return &backoff{
		base:   base,
		max:    max,
		factor: factor,
	}
}

func (b *backoff) clear() {
	b.attempt = 0
}

func (b *backoff) wait(ctx context.Context) error {
	delay := b.sleepDuration()

	b.attempt++

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return fmt.Errorf("backoff waiting: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}

func (b *backoff) sleepDuration() time.Duration {
	factor := math.Pow(b.factor, float64(b.attempt))
	if math.IsInf(factor, 1) || factor > float64(b.max/b.base) {
		return rand.N(b.max)
	}
	delay := b.base * time.Duration(factor)
	return rand.N(delay)
}
