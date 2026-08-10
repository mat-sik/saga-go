package tx

import (
	"context"
	"fmt"

	"github.com/mat-sik/saga-go/saga"
)

type LogExistenceChecker interface {
	Check(ctx context.Context, id string) (bool, error)
}

type LogInserter interface {
	Insert(ctx context.Context, registerCommand RegisterCommand) error
}

type CompensatingLogInserter interface {
	Insert(ctx context.Context, registerCommand RegisterCommand) error
}

type LogSagaAction struct {
	UnitOfWork              UnitOfWork
	logExistenceChecker     LogExistenceChecker
	logInserter             LogInserter
	compensatingLogInserter CompensatingLogInserter
}

func NewLogSagaAction(
	unitOfWork UnitOfWork,
	logExistenceChecker LogExistenceChecker,
	logInserter LogInserter,
	compensationLogInserter CompensatingLogInserter,
) LogSagaAction {
	return LogSagaAction{
		UnitOfWork:              unitOfWork,
		logExistenceChecker:     logExistenceChecker,
		logInserter:             logInserter,
		compensatingLogInserter: compensationLogInserter,
	}
}

func (l LogSagaAction) Execute(ctx context.Context, tx saga.Transaction) error {
	registerCommand, ok := tx.(RegisterCommand)
	if !ok {
		return fmt.Errorf("unexpected transaction type '%T", tx)
	}

	return l.insert(ctx, registerCommand, l.logInserter.Insert)
}

func (l LogSagaAction) Compensate(ctx context.Context, tx saga.CompensatingTransaction) error {
	unregisterCommand, ok := tx.(UnregisterCommand)
	if !ok {
		return fmt.Errorf("unexpected transaction type '%T", tx)
	}

	return l.insert(ctx, unregisterCommand.RegisterCommand, l.compensatingLogInserter.Insert)
}

func (l LogSagaAction) insert(
	ctx context.Context,
	registerCommand RegisterCommand,
	insertFN func(ctx context.Context, registerCommand RegisterCommand) error,
) error {
	return l.UnitOfWork(ctx, func(ctx context.Context) error {
		if exists, err := l.logExistenceChecker.Check(ctx, registerCommand.RegisterID.ID.TransactionID); err != nil {
			return fmt.Errorf("checking if tx exists: %w", err)
		} else if exists {
			return nil
		}

		return insertFN(ctx, registerCommand)
	})
}
