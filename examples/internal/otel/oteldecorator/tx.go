package oteldecorator

import (
	"context"

	"github.com/mat-sik/saga-go/examples/internal/adapters/sagaadapters"
	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"github.com/mat-sik/saga-go/examples/internal/otel/attrmap"
	"github.com/mat-sik/saga-go/examples/internal/otel/spanwrap"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type TracedTxValidator struct {
	next   tx.Validator
	tracer trace.Tracer
}

func NewTracedTxValidator(next tx.Validator, tracer trace.Tracer) TracedTxValidator {
	return TracedTxValidator{
		next:   next,
		tracer: tracer,
	}
}

func (v TracedTxValidator) ValidateAndCompensate(ctx context.Context, cmd tx.RegisterCommand) error {
	const (
		spanName    = "tx.validator.validate"
		workErrDesc = "validation failed"
	)

	work := func(ctx context.Context) error {
		return v.next.ValidateAndCompensate(ctx, cmd)
	}
	attributes := func() []attribute.KeyValue {
		return attrmap.TxRegister(cmd)
	}

	return spanwrap.WrapWithAttrs(ctx, v.tracer, spanName, workErrDesc, work, attributes)
}

type TracedTxSagaAction struct {
	next   sagaadapters.TxSagaAction
	tracer trace.Tracer
}

func NewTracedTxSagaAction(next sagaadapters.TxSagaAction, tracer trace.Tracer) TracedTxSagaAction {
	return TracedTxSagaAction{
		next:   next,
		tracer: tracer,
	}
}

func (a TracedTxSagaAction) Execute(ctx context.Context, cmd sagaadapters.RegisterSagaCommand) error {
	const (
		spanName    = "tx.saga.execute"
		workErrDesc = "tx execute failed"
	)

	work := func(ctx context.Context) error {
		return a.next.Execute(ctx, cmd)
	}
	attributes := func() []attribute.KeyValue {
		return attrmap.TxRegister(cmd.RegisterCommand)
	}

	return spanwrap.WrapWithAttrs(ctx, a.tracer, spanName, workErrDesc, work, attributes)
}

func (a TracedTxSagaAction) Compensate(ctx context.Context, cmd sagaadapters.UnregisterSagaCommand) error {
	const (
		spanName    = "tx.saga.compensate"
		workErrDesc = "tx compensate failed"
	)

	work := func(ctx context.Context) error {
		return a.next.Compensate(ctx, cmd)
	}
	attributes := func() []attribute.KeyValue {
		return attrmap.TxUnregister(cmd.UnregisterCommand)
	}

	return spanwrap.WrapWithAttrs(ctx, a.tracer, spanName, workErrDesc, work, attributes)
}

type TracedTxLog struct {
	next   tx.Log
	tracer trace.Tracer
}

func NewTracedTxLog(next tx.Log, tracer trace.Tracer) TracedTxLog {
	return TracedTxLog{
		next:   next,
		tracer: tracer,
	}
}

func (l TracedTxLog) Register(ctx context.Context, cmd tx.RegisterCommand) error {
	const (
		spanName    = "tx.log.register"
		workErrDesc = "log register failed"
	)

	work := func(ctx context.Context) error {
		return l.next.Register(ctx, cmd)
	}

	return spanwrap.Wrap(ctx, l.tracer, spanName, workErrDesc, work)
}

func (l TracedTxLog) Unregister(ctx context.Context, cmd tx.UnregisterCommand) error {
	const (
		spanName    = "tx.log.unregister"
		workErrDesc = "log unregister failed"
	)

	work := func(ctx context.Context) error {
		return l.next.Unregister(ctx, cmd)
	}

	return spanwrap.Wrap(ctx, l.tracer, spanName, workErrDesc, work)
}

type TracedTxAggregate struct {
	next   tx.Aggregate
	tracer trace.Tracer
}

func NewTracedTxAggregate(next tx.Aggregate, tracer trace.Tracer) TracedTxAggregate {
	return TracedTxAggregate{
		next:   next,
		tracer: tracer,
	}
}

func (l TracedTxAggregate) Register(ctx context.Context, cmd tx.RegisterCommand) error {
	const (
		spanName    = "tx.aggregate.register"
		workErrDesc = "aggregate register failed"
	)

	work := func(ctx context.Context) error {
		return l.next.Register(ctx, cmd)
	}

	return spanwrap.Wrap(ctx, l.tracer, spanName, workErrDesc, work)
}

func (l TracedTxAggregate) Unregister(ctx context.Context, cmd tx.UnregisterCommand) error {
	const (
		spanName    = "tx.aggregate.unregister"
		workErrDesc = "aggregate unregister failed"
	)

	work := func(ctx context.Context) error {
		return l.next.Unregister(ctx, cmd)
	}

	return spanwrap.Wrap(ctx, l.tracer, spanName, workErrDesc, work)
}
