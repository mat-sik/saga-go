package tx

import (
	"context"
)

type Action struct {
	unitOfWork          UnitOfWork
	logSagaAction       LogSagaAction
	aggregateSagaAction AggregateSagaAction
}

func NewAction(
	unitOfWork UnitOfWork,
	logSagaAction LogSagaAction,
	aggregateSagaAction AggregateSagaAction,
) Action {
	return Action{
		unitOfWork:          unitOfWork,
		logSagaAction:       logSagaAction,
		aggregateSagaAction: aggregateSagaAction,
	}
}

func (a Action) Execute(ctx context.Context, registerCommand RegisterCommand) error {
	return a.unitOfWork(ctx, func(ctx context.Context) error {
		if err := a.aggregateSagaAction.Execute(ctx, registerCommand); err != nil {
			return err
		}
		if err := a.logSagaAction.Execute(ctx, registerCommand); err != nil {
			return err
		}
		return nil
	})
}

func (a Action) Compensate(ctx context.Context, unregisterCommand UnregisterCommand) error {
	return a.unitOfWork(ctx, func(ctx context.Context) error {
		if err := a.aggregateSagaAction.Compensate(ctx, unregisterCommand); err != nil {
			return err
		}
		if err := a.logSagaAction.Compensate(ctx, unregisterCommand); err != nil {
			return err
		}
		return nil
	})
}
