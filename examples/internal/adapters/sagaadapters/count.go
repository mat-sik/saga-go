package sagaadapters

import (
	"context"

	"github.com/mat-sik/saga-go/examples/internal/domain/count"
)

type CountSagaAction[T, CT any] struct {
	counter count.Counter
}

func NewCountSagaAction[T, CT any](counter count.Counter) CountSagaAction[T, CT] {
	return CountSagaAction[T, CT]{
		counter: counter,
	}
}

func (c CountSagaAction[T, CT]) Execute(ctx context.Context, _ T) error {
	return c.counter.Increment(ctx)
}

func (c CountSagaAction[T, CT]) Compensate(ctx context.Context, _ CT) error {
	return c.counter.Decrement(ctx)
}
