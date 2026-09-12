package oteldecorator

import (
	"context"

	"github.com/mat-sik/saga-go/examples/internal/otel/spanwrap"
	"github.com/mat-sik/saga-go/idempotent"
	"go.opentelemetry.io/otel/trace"
)

type TracedIdempotentConsumer[T any] struct {
	next   idempotent.Consumer[T]
	tracer trace.Tracer
}

func NewTracedIdempotentConsumer[T any](next idempotent.Consumer[T], tracer trace.Tracer) TracedIdempotentConsumer[T] {
	return TracedIdempotentConsumer[T]{
		next:   next,
		tracer: tracer,
	}
}

func (c TracedIdempotentConsumer[T]) Consume(ctx context.Context, message T) (err error) {
	const (
		spanName    = "idempotent.consumer.consume"
		workErrDesc = "consume failed"
	)

	work := func(ctx context.Context) error {
		return c.next.Consume(ctx, message)
	}

	return spanwrap.Wrap(ctx, c.tracer, spanName, workErrDesc, work)
}
