package count

import (
	"context"
	"fmt"
)

type PortOut interface {
	Increment(ctx context.Context) error
	Decrement(ctx context.Context) error
}

type SagaAction[T, CT any] struct {
	portOut PortOut
}

func NewSagaAction[T, CT any](portOut PortOut) SagaAction[T, CT] {
	return SagaAction[T, CT]{
		portOut: portOut,
	}
}

func (a SagaAction[T, CT]) Execute(ctx context.Context, _ T) error {
	if err := a.portOut.Increment(ctx); err != nil {
		return fmt.Errorf("incrementing count: %w", err)
	}
	return nil
}

func (a SagaAction[T, CT]) Compensate(ctx context.Context, _ CT) error {
	if err := a.portOut.Decrement(ctx); err != nil {
		return fmt.Errorf("decrementing count: %w", err)
	}
	return nil
}
