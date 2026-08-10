package tx

import (
	"context"
	"fmt"

	"github.com/mat-sik/saga-go/saga"
)

type UnitOfWork func(ctx context.Context, fn func(ctx context.Context) error) error

type AggregateSagaAction struct {
	aggregateSagaExecutor    aggregateSagaExecutor
	aggregateSagaCompensator aggregateSagaCompensator
}

func NewAggregateSagaAction(
	unitOfWork UnitOfWork,
	transactionFinalizedChecker TransactionFinalizedChecker,
	aggregateUpserter AggregateUpserter,
	aggregateExistenceChecker AggregateExistenceChecker,
	aggregateValueSubtractor AggregateValueSubtractor,
) AggregateSagaAction {
	sagaExecutor := aggregateSagaExecutor{
		unitOfWork:                  unitOfWork,
		transactionFinalizedChecker: transactionFinalizedChecker,
		aggregateUpserter:           aggregateUpserter,
	}

	sagaCompensator := aggregateSagaCompensator{
		unitOfWork:                unitOfWork,
		aggregateExistenceChecker: aggregateExistenceChecker,
		aggregateValueSubtractor:  aggregateValueSubtractor,
	}

	return AggregateSagaAction{
		aggregateSagaExecutor:    sagaExecutor,
		aggregateSagaCompensator: sagaCompensator,
	}
}

func (a AggregateSagaAction) Execute(ctx context.Context, tx saga.Transaction) error {
	registerCommand, ok := tx.(RegisterCommand)
	if !ok {
		return fmt.Errorf("unexpected transaction type '%T", tx)
	}
	return a.aggregateSagaExecutor.execute(ctx, registerCommand)
}

func (a AggregateSagaAction) Compensate(ctx context.Context, tx saga.CompensatingTransaction) error {
	unregisterCommand, ok := tx.(UnregisterCommand)
	if !ok {
		return fmt.Errorf("unexpected transaction type '%T", tx)
	}
	return a.aggregateSagaCompensator.compensate(ctx, unregisterCommand)
}

type AggregateUpserter interface {
	Upsert(ctx context.Context, id RegisterID, value int) error
}

type TransactionFinalizedChecker interface {
	Check(ctx context.Context, id string) (bool, error)
}

type aggregateSagaExecutor struct {
	unitOfWork                  UnitOfWork
	transactionFinalizedChecker TransactionFinalizedChecker
	aggregateUpserter           AggregateUpserter
}

func (a aggregateSagaExecutor) execute(ctx context.Context, registerCommand RegisterCommand) error {
	return a.unitOfWork(ctx, func(ctx context.Context) error {
		return a.UpsertOrAbortIfFinalized(ctx, registerCommand.RegisterID, registerCommand.Value)
	})
}

func (a aggregateSagaExecutor) UpsertOrAbortIfFinalized(ctx context.Context, registerID RegisterID, value int) error {
	if finalized, err := a.transactionFinalizedChecker.Check(ctx, registerID.ID.TransactionID); err != nil {
		return fmt.Errorf("checking if tx finalized: %w", err)
	} else if finalized {
		return nil
	}

	if err := a.aggregateUpserter.Upsert(ctx, registerID, value); err != nil {
		return fmt.Errorf("upserting tx: %w", err)
	}

	return nil
}

type AggregateExistenceChecker interface {
	exists(ctx context.Context, id RegisterID) (bool, error)
}

type AggregateValueSubtractor interface {
	subtract(ctx context.Context, id RegisterID, value int) error
}

type aggregateSagaCompensator struct {
	unitOfWork                UnitOfWork
	aggregateExistenceChecker AggregateExistenceChecker
	aggregateValueSubtractor  AggregateValueSubtractor
}

func (a aggregateSagaCompensator) compensate(ctx context.Context, unregisterCommand UnregisterCommand) error {
	return a.unitOfWork(ctx, func(ctx context.Context) error {
		if exists, err := a.aggregateExistenceChecker.exists(ctx, unregisterCommand.RegisterCommand.RegisterID); err != nil {
			return fmt.Errorf("finding existing aggregate: %w", err)
		} else if !exists {
			return nil
		}

		return a.aggregateValueSubtractor.subtract(ctx, unregisterCommand.RegisterCommand.RegisterID, unregisterCommand.RegisterCommand.Value)
	})
}
