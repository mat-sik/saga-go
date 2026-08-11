package saga

import (
	"context"
	"fmt"
)

type Identifiable[ID any] interface {
	Id() ID
}

type Command[ID, TX, CTX any] interface {
	Identifiable[ID]
	ToTransaction() (TX, bool)
	ToCompensatingTransaction() (CTX, bool)
}

type ActionDispatcher[ID, TX, CTX any] interface {
	SagaAction(command Command[ID, TX, CTX]) (Action[TX, CTX], bool)
}

type CommandAlreadyHandledChecker[ID any] interface {
	check(id ID) bool
}

type TransactionCompensatedChecker[TX any] interface {
	check(tx TX) bool
}

type Consumer[ID, TX, CTX any] struct {
	sagaActionDispatcher          ActionDispatcher[ID, TX, CTX]
	commandAlreadyHandledChecker  CommandAlreadyHandledChecker[ID]
	transactionCompensatedChecker TransactionCompensatedChecker[TX]
}

func NewConsumer[ID, TX, CTX any](
	sagaActionDispatcher ActionDispatcher[ID, TX, CTX],
	commandAlreadyHandledChecker CommandAlreadyHandledChecker[ID],
	transactionCompensatedChecker TransactionCompensatedChecker[TX],
) Consumer[ID, TX, CTX] {
	return Consumer[ID, TX, CTX]{
		sagaActionDispatcher:          sagaActionDispatcher,
		commandAlreadyHandledChecker:  commandAlreadyHandledChecker,
		transactionCompensatedChecker: transactionCompensatedChecker,
	}
}

func (c Consumer[ID, TX, CTX]) Consume(ctx context.Context, command Command[ID, TX, CTX]) error {
	sagaAction, sagaActionFound := c.sagaActionDispatcher.SagaAction(command)
	if !sagaActionFound {
		return fmt.Errorf("no processor registered for command '%v'", command)
	}

	if c.commandAlreadyHandledChecker.check(command.Id()) {
		return nil
	}

	if tx, ok := command.ToTransaction(); ok {
		if c.transactionCompensatedChecker.check(tx) {
			return nil
		}
		return sagaAction.Execute(ctx, tx)
	}

	if compensatingTx, ok := command.ToCompensatingTransaction(); ok {
		return sagaAction.Compensate(ctx, compensatingTx)
	}

	return fmt.Errorf("command '%v' is not transaction nor compensating transaction", command)
}
