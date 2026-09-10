package oteldecorator

import (
	"context"

	"github.com/mat-sik/saga-go/examples/internal/adapters/sagaadapters"
	"github.com/mat-sik/saga-go/examples/internal/domain/alarm"
	"github.com/mat-sik/saga-go/examples/internal/otel/attrmap"
	"github.com/mat-sik/saga-go/examples/internal/otel/spanwrap"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type TracedAlarmSagaAction struct {
	next   sagaadapters.AlarmSagaAction
	tracer trace.Tracer
}

func NewTracedAlarmSagaAction(next sagaadapters.AlarmSagaAction, tracer trace.Tracer) TracedAlarmSagaAction {
	return TracedAlarmSagaAction{
		next:   next,
		tracer: tracer,
	}
}

func (a TracedAlarmSagaAction) Execute(ctx context.Context, cmd sagaadapters.RaiseAlarmSagaCommand) error {
	const (
		spanName    = "alarm.saga.execute"
		workErrDesc = "alarm execute failed"
	)

	work := func(ctx context.Context) error {
		return a.next.Execute(ctx, cmd)
	}
	attributes := func() []attribute.KeyValue {
		return attrmap.AlarmRaise(cmd.RaiseAlarmCommand)
	}

	return spanwrap.WrapWithAttrs(ctx, a.tracer, spanName, workErrDesc, work, attributes)
}

func (a TracedAlarmSagaAction) Compensate(ctx context.Context, cmd sagaadapters.ClearAlarmSagaCommand) error {
	const (
		spanName    = "alarm.saga.compensate"
		workErrDesc = "alarm compensate failed"
	)

	work := func(ctx context.Context) error {
		return a.next.Compensate(ctx, cmd)
	}
	attributes := func() []attribute.KeyValue {
		return attrmap.AlarmClear(cmd.ClearAlarmCommand)
	}

	return spanwrap.WrapWithAttrs(ctx, a.tracer, spanName, workErrDesc, work, attributes)
}

type TracedAlarmRaiser struct {
	next   alarm.Raiser
	tracer trace.Tracer
}

func NewTracedAlarmRaiser(next alarm.Raiser, tracer trace.Tracer) TracedAlarmRaiser {
	return TracedAlarmRaiser{
		next:   next,
		tracer: tracer,
	}
}

func (ar TracedAlarmRaiser) RaiseAlarm(ctx context.Context, cmd alarm.RaiseAlarmCommand) error {
	const (
		spanName    = "alarm.raiser.raise"
		workErrDesc = "raise alarm failed"
	)

	work := func(ctx context.Context) error {
		return ar.next.RaiseAlarm(ctx, cmd)
	}

	return spanwrap.Wrap(ctx, ar.tracer, spanName, workErrDesc, work)
}

func (ar TracedAlarmRaiser) ClearAlarm(ctx context.Context, cmd alarm.ClearAlarmCommand) error {
	const (
		spanName    = "alarm.raiser.clear"
		workErrDesc = "clear alarm failed"
	)

	work := func(ctx context.Context) error {
		return ar.next.ClearAlarm(ctx, cmd)
	}

	return spanwrap.Wrap(ctx, ar.tracer, spanName, workErrDesc, work)
}
