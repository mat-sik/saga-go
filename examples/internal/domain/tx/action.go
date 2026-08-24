package tx

import (
	"context"
)

type SagaAction struct {
	logSagaAction       LogSagaAction
	aggregateSagaAction AggregateSagaAction
}

func NewSagaAction(
	logSagaAction LogSagaAction,
	aggregateSagaAction AggregateSagaAction,
) SagaAction {
	return SagaAction{
		logSagaAction:       logSagaAction,
		aggregateSagaAction: aggregateSagaAction,
	}
}

func (a SagaAction) Execute(ctx context.Context, registerCommand RegisterCommand) error {
	if err := a.aggregateSagaAction.Execute(ctx, registerCommand); err != nil {
		return err
	}
	if err := a.logSagaAction.Execute(ctx, registerCommand); err != nil {
		return err
	}
	return nil
}

func (a SagaAction) Compensate(ctx context.Context, unregisterCommand UnregisterCommand) error {
	if err := a.aggregateSagaAction.Compensate(ctx, unregisterCommand); err != nil {
		return err
	}
	if err := a.logSagaAction.Compensate(ctx, unregisterCommand); err != nil {
		return err
	}
	return nil
}
