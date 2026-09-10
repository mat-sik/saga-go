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

type TxSagaAction struct {
	log       tx.Log
	aggregate tx.Aggregate
}

func NewTxSagaAction(log tx.Log, aggregate tx.Aggregate) TxSagaAction {
	return TxSagaAction{
		log:       log,
		aggregate: aggregate,
	}
}

func (a TxSagaAction) Execute(ctx context.Context, cmd RegisterSagaCommand) error {
	if err := a.aggregate.Register(ctx, cmd.RegisterCommand); err != nil {
		return err
	}
	if err := a.log.Register(ctx, cmd.RegisterCommand); err != nil {
		return err
	}
	return nil
}

func (a TxSagaAction) Compensate(ctx context.Context, cmd UnregisterSagaCommand) error {
	if err := a.aggregate.Unregister(ctx, cmd.UnregisterCommand); err != nil {
		return err
	}
	if err := a.log.Unregister(ctx, cmd.UnregisterCommand); err != nil {
		return err
	}
	return nil
}
