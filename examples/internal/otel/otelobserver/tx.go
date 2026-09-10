package otelobserver

import (
	"context"
	"fmt"

	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type TxAggregateObserver struct{}

func NewAggregateObserver() TxAggregateObserver {
	return TxAggregateObserver{}
}

func (TxAggregateObserver) Observe(ctx context.Context, event tx.AggregateEvent) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	switch event := event.(type) {
	case tx.AggregateRaiseAlarmEvent:
		span.AddEvent(
			"tx.aggregate.register.raise.alarm",
			aggregateEventAttributes(
				event.PreviousValue,
				event.DeltaValue,
				event.CurrentValue,
				event.AlarmValue,
			),
		)
	case tx.AggregateClearAlarmEvent:
		span.AddEvent(
			"tx.aggregate.register.clear.alarm",
			aggregateEventAttributes(
				event.PreviousValue,
				event.DeltaValue,
				event.CurrentValue,
				event.AlarmValue,
			),
		)
	case tx.AggregateNoChangeEvent:
		span.AddEvent(
			"tx.aggregate.register.no_change",
			trace.WithAttributes(attribute.Int("value.alarm", event.AlarmValue)),
		)
	default:
		panic(fmt.Sprintf("unsupported event type %T", event))
	}
}

func aggregateEventAttributes(
	previousValue int,
	deltaValue int,
	currentValue int,
	alarmValue int,
) trace.EventOption {
	return trace.WithAttributes(
		attribute.Int("value.previous", previousValue),
		attribute.Int("value.delta", deltaValue),
		attribute.Int("value.current", currentValue),
		attribute.Int("value.alarm", alarmValue),
	)
}

type TxValidatorObserver struct{}

func NewValidatorObserver() TxValidatorObserver {
	return TxValidatorObserver{}
}

func (TxValidatorObserver) Observe(ctx context.Context, event tx.ValidatorEvent) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	switch event {
	case tx.ValidatorEventValidationUnsuccessful:
		span.AddEvent("tx.validator.validation.unsuccessful")
	default:
		panic(fmt.Sprintf("unsupported event type %T", event))
	}
}
