package count

import (
	"context"
	"fmt"

	"github.com/mat-sik/saga-go/examples/domain/tx"
	"github.com/mat-sik/saga-go/saga"
)

type Incrementer interface {
	increment(ctx context.Context) error
}

type Decrementer interface {
	decrement(ctx context.Context) error
}

type Action struct {
	incrementer Incrementer
	decrementer Decrementer
}

func NewAction(
	incrementer Incrementer,
	decrementer Decrementer,
) Action {
	return Action{
		incrementer: incrementer,
		decrementer: decrementer,
	}
}

func (a Action) Execute(ctx context.Context, _ saga.Transaction) error {
	return a.incrementer.increment(ctx)
}

func (a Action) Compensate(ctx context.Context, _ saga.CompensatingTransaction) error {
	return a.decrementer.decrement(ctx)
}

type AlreadyProcessedChecker interface {
	processed(ctx context.Context, transactionID string) (bool, error)
}

type ProcessedRegisterer interface {
	register(ctx context.Context, transactionID string) error
}

type IdempotentAction struct {
	unitOfWork              tx.UnitOfWork
	alreadyProcessedChecker AlreadyProcessedChecker
	processedRegisterer     ProcessedRegisterer
	incrementer             Incrementer
	decrementer             Decrementer
}

func NewIdempotentAction(
	unitOfWork tx.UnitOfWork,
	alreadyProcessedChecker AlreadyProcessedChecker,
	processedRegisterer ProcessedRegisterer,
	incrementer Incrementer,
	decrementer Decrementer,
) IdempotentAction {
	return IdempotentAction{
		unitOfWork:              unitOfWork,
		alreadyProcessedChecker: alreadyProcessedChecker,
		processedRegisterer:     processedRegisterer,
		incrementer:             incrementer,
		decrementer:             decrementer,
	}
}

func (a IdempotentAction) Execute(ctx context.Context, transaction saga.Transaction) error {
	registerCommand, ok := transaction.(tx.RegisterCommand)
	if !ok {
		return fmt.Errorf("unexpected transaction type '%T", transaction)
	}

	return a.doWork(ctx, registerCommand.RegisterID.ID.TransactionID, a.incrementer.increment)
}

func (a IdempotentAction) Compensate(ctx context.Context, compensatingTransaction saga.CompensatingTransaction) error {
	unregisterCommand, ok := compensatingTransaction.(tx.UnregisterCommand)
	if !ok {
		return fmt.Errorf("unexpected transaction type '%T", compensatingTransaction)
	}

	return a.doWork(ctx, unregisterCommand.RegisterCommand.RegisterID.ID.TransactionID, a.decrementer.decrement)
}

func (a IdempotentAction) doWork(ctx context.Context, transactionID string, operationFN func(ctx context.Context) error) error {
	return a.unitOfWork(ctx, func(ctx context.Context) error {
		if processed, err := a.alreadyProcessedChecker.processed(ctx, transactionID); err != nil {
			return fmt.Errorf("checking if processed tx: %w", err)
		} else if processed {
			return nil
		}

		if err := operationFN(ctx); err != nil {
			return err
		}

		return a.processedRegisterer.register(ctx, transactionID)
	})
}
