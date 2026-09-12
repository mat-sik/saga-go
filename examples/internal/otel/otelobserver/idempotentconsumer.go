package otelobserver

import (
	"context"
	"fmt"

	"github.com/mat-sik/saga-go/idempotent"
	"go.opentelemetry.io/otel/trace"
)

type IdempotentConsumerObserver struct {
}

func NewIdempotentConsumerObserver() IdempotentConsumerObserver {
	return IdempotentConsumerObserver{}
}

func (i IdempotentConsumerObserver) Observe(ctx context.Context, event idempotent.Event) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	switch event {
	case idempotent.EventAlreadyHandled:
		span.AddEvent("idempotent.consumer.consume.message.already_handled")
	default:
		panic(fmt.Sprintf("unsupported event type %T", event))
	}
}
