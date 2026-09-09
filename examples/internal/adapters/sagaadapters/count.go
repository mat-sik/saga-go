package sagaadapters

import (
	"context"

	"github.com/mat-sik/saga-go/examples/internal/domain/count"
)

type CountAction[T, CT any] struct {
	counter count.Counter
}

func NewCountAction[T, CT any](counter count.Counter) CountAction[T, CT] {
	return CountAction[T, CT]{
		counter: counter,
	}
}

func (c CountAction[T, CT]) Execute(ctx context.Context, _ T) error {
	return c.counter.Increment(ctx)
}

func (c CountAction[T, CT]) Compensate(ctx context.Context, _ CT) error {
	return c.counter.Decrement(ctx)
}
