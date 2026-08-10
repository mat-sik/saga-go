package tx

import (
	"context"

	"github.com/mat-sik/saga-go/saga"
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

func (a Action) Execute(ctx context.Context, tx saga.Transaction) error {
	return a.unitOfWork(ctx, func(ctx context.Context) error {
		if err := a.aggregateSagaAction.Execute(ctx, tx); err != nil {
			return err
		}
		if err := a.logSagaAction.Execute(ctx, tx); err != nil {
			return err
		}
		return nil
	})
}

func (a Action) Compensate(ctx context.Context, tx saga.CompensatingTransaction) error {
	return a.unitOfWork(ctx, func(ctx context.Context) error {
		if err := a.aggregateSagaAction.Compensate(ctx, tx); err != nil {
			return err
		}
		if err := a.logSagaAction.Compensate(ctx, tx); err != nil {
			return err
		}
		return nil
	})
}
