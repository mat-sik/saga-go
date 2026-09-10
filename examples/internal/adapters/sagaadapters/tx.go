package sagaadapters

import (
	"context"

	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
)

type RegisterSagaCommand struct {
	tx.RegisterCommand
}

func (r RegisterSagaCommand) ToTransaction() (RegisterSagaCommand, bool) {
	return r, true
}

func (r RegisterSagaCommand) ToCompensatingTransaction() (UnregisterSagaCommand, bool) {
	return UnregisterSagaCommand{}, false
}

type UnregisterSagaCommand struct {
	tx.UnregisterCommand
}

func (u UnregisterSagaCommand) ToTransaction() (RegisterSagaCommand, bool) {
	return RegisterSagaCommand{}, false
}

func (u UnregisterSagaCommand) ToCompensatingTransaction() (UnregisterSagaCommand, bool) {
	return u, true
}

type TxAction struct {
	logSagaAction       tx.Log
	aggregateSagaAction tx.Aggregate
}

func NewTxAction(logSagaAction tx.Log, aggregateSagaAction tx.Aggregate) TxAction {
	return TxAction{
		logSagaAction:       logSagaAction,
		aggregateSagaAction: aggregateSagaAction,
	}
}

func (a TxAction) Execute(ctx context.Context, cmd RegisterSagaCommand) error {
	if err := a.aggregateSagaAction.Register(ctx, cmd.RegisterCommand); err != nil {
		return err
	}
	if err := a.logSagaAction.Register(ctx, cmd.RegisterCommand); err != nil {
		return err
	}
	return nil
}

func (a TxAction) Compensate(ctx context.Context, cmd UnregisterSagaCommand) error {
	if err := a.aggregateSagaAction.Unregister(ctx, cmd.UnregisterCommand); err != nil {
		return err
	}
	if err := a.logSagaAction.Unregister(ctx, cmd.UnregisterCommand); err != nil {
		return err
	}
	return nil
}
