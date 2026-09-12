package oteldecorator

import (
	"context"

	"github.com/mat-sik/saga-go/examples/internal/adapters/sagaadapters"
	"github.com/mat-sik/saga-go/examples/internal/domain/count"
	"github.com/mat-sik/saga-go/examples/internal/otel/spanwrap"
	"go.opentelemetry.io/otel/trace"
)

type TracedCountSagaAction[T, CT any] struct {
	next   sagaadapters.CountSagaAction[T, CT]
	tracer trace.Tracer
}

func NewTracedCountSagaAction[T, CT any](
	next sagaadapters.CountSagaAction[T, CT],
	tracer trace.Tracer,
) TracedCountSagaAction[T, CT] {
	return TracedCountSagaAction[T, CT]{
		next:   next,
		tracer: tracer,
	}
}

func (a TracedCountSagaAction[T, CT]) Execute(ctx context.Context, cmd T) error {
	const (
		spanName    = "count.saga.execute"
		workErrDesc = "count execute failed"
	)

	work := func(ctx context.Context) error {
		return a.next.Execute(ctx, cmd)
	}

	return spanwrap.Wrap(ctx, a.tracer, spanName, workErrDesc, work)
}

func (a TracedCountSagaAction[T, CT]) Compensate(ctx context.Context, cmd CT) error {
	const (
		spanName    = "count.saga.compensate"
		workErrDesc = "count compensate failed"
	)

	work := func(ctx context.Context) error {
		return a.next.Compensate(ctx, cmd)
	}

	return spanwrap.Wrap(ctx, a.tracer, spanName, workErrDesc, work)
}

type TracedCounter struct {
	next   count.Counter
	tracer trace.Tracer
}

func NewTracedCounter(next count.Counter, tracer trace.Tracer) TracedCounter {
	return TracedCounter{
		next:   next,
		tracer: tracer,
	}
}

func (a TracedCounter) Increment(ctx context.Context) error {
	const (
		spanName    = "count.counter.increment"
		workErrDesc = "increment failed"
	)

	return spanwrap.Wrap(ctx, a.tracer, spanName, workErrDesc, a.next.Increment)
}

func (a TracedCounter) Decrement(ctx context.Context) error {
	const (
		spanName    = "count.counter.decrement"
		workErrDesc = "decrement failed"
	)

	return spanwrap.Wrap(ctx, a.tracer, spanName, workErrDesc, a.next.Decrement)
}
