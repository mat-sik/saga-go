package tx

import (
	"context"
)

type Action struct {
	logSagaAction       LogSagaAction
	aggregateSagaAction AggregateSagaAction
}

func NewAction(
	logSagaAction LogSagaAction,
	aggregateSagaAction AggregateSagaAction,
) Action {
	return Action{
		logSagaAction:       logSagaAction,
		aggregateSagaAction: aggregateSagaAction,
	}
}

func (a Action) Execute(ctx context.Context, registerCommand RegisterCommand) error {
	if err := a.aggregateSagaAction.Execute(ctx, registerCommand); err != nil {
		return err
	}
	if err := a.logSagaAction.Execute(ctx, registerCommand); err != nil {
		return err
	}
	return nil
}

func (a Action) Compensate(ctx context.Context, unregisterCommand UnregisterCommand) error {
	if err := a.aggregateSagaAction.Compensate(ctx, unregisterCommand); err != nil {
		return err
	}
	if err := a.logSagaAction.Compensate(ctx, unregisterCommand); err != nil {
		return err
	}
	return nil
}
