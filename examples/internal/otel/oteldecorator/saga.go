package oteldecorator

import (
	"context"

	"github.com/mat-sik/saga-go/examples/internal/otel/spanwrap"
	"github.com/mat-sik/saga-go/saga"
	"go.opentelemetry.io/otel/trace"
)

type TracedSagaConsumer[T, CT any] struct {
	next   saga.Consumer[T, CT]
	tracer trace.Tracer
}

func NewTracedSagaConsumer[T, CT any](next saga.Consumer[T, CT], tracer trace.Tracer) TracedSagaConsumer[T, CT] {
	return TracedSagaConsumer[T, CT]{
		next:   next,
		tracer: tracer,
	}
}

func (c TracedSagaConsumer[T, CT]) Consume(ctx context.Context, cmd saga.Command[T, CT]) (err error) {
	const (
		spanName    = "saga.consumer.consume"
		workErrDesc = "consume failed"
	)

	work := func(ctx context.Context) error {
		return c.next.Consume(ctx, cmd)
	}

	return spanwrap.Wrap(ctx, c.tracer, spanName, workErrDesc, work)
}
