package otelobserver

import (
	"context"
	"fmt"

	"github.com/mat-sik/saga-go/saga"
	"go.opentelemetry.io/otel/trace"
)

type SagaConsumerObserver struct {
}

func NewSagaConsumerObserver() SagaConsumerObserver {
	return SagaConsumerObserver{}
}

func (i SagaConsumerObserver) Observe(ctx context.Context, event saga.Event) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	switch event {
	case saga.EventAlreadyHandled:
		span.AddEvent("saga.consumer.consume.transaction.already_handled")
	case saga.EventAlreadyCompensated:
		span.AddEvent("saga.consumer.consume.transaction.already_compensated")
	default:
		panic(fmt.Sprintf("unsupported event type %T", event))
	}
}
