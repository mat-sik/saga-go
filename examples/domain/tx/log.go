package tx

import (
	"context"
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
	logExistenceChecker     LogExistenceChecker
	logInserter             LogInserter
	compensatingLogInserter CompensatingLogInserter
}

func NewLogSagaAction(
	logExistenceChecker LogExistenceChecker,
	logInserter LogInserter,
	compensationLogInserter CompensatingLogInserter,
) LogSagaAction {
	return LogSagaAction{
		logExistenceChecker:     logExistenceChecker,
		logInserter:             logInserter,
		compensatingLogInserter: compensationLogInserter,
	}
}

func (l LogSagaAction) Execute(ctx context.Context, registerCommand RegisterCommand) error {
	return l.logInserter.Insert(ctx, registerCommand)
}

func (l LogSagaAction) Compensate(ctx context.Context, unregisterCommand UnregisterCommand) error {
	return l.compensatingLogInserter.Insert(ctx, unregisterCommand.RegisterCommand)
}
