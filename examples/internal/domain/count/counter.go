package count

import (
	"context"
	"fmt"
)

type PortOut interface {
	Increment(ctx context.Context) error
	Decrement(ctx context.Context) error
}

type Counter struct {
	portOut PortOut
}

func NewCounter(portOut PortOut) Counter {
	return Counter{
		portOut: portOut,
	}
}

func (a Counter) Increment(ctx context.Context) error {
	if err := a.portOut.Increment(ctx); err != nil {
		return fmt.Errorf("incrementing count: %w", err)
	}
	return nil
}

func (a Counter) Decrement(ctx context.Context) error {
	if err := a.portOut.Decrement(ctx); err != nil {
		return fmt.Errorf("decrementing count: %w", err)
	}
	return nil
}
