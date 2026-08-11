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
	check(id ID) (bool, error)
}

type TransactionCompensatedChecker[TX any] interface {
	check(tx TX) (bool, error)
}

type CommandHandledMarker[ID any] interface {
	mark(id ID) error
}

type Consumer[ID, TX, CTX any] struct {
	sagaActionDispatcher          ActionDispatcher[ID, TX, CTX]
	commandAlreadyHandledChecker  CommandAlreadyHandledChecker[ID]
	transactionCompensatedChecker TransactionCompensatedChecker[TX]
	commandHandledMarker          CommandHandledMarker[ID]
}

func NewConsumer[ID, TX, CTX any](
	sagaActionDispatcher ActionDispatcher[ID, TX, CTX],
	commandAlreadyHandledChecker CommandAlreadyHandledChecker[ID],
	transactionCompensatedChecker TransactionCompensatedChecker[TX],
	commandHandledMarker CommandHandledMarker[ID],
) Consumer[ID, TX, CTX] {
	return Consumer[ID, TX, CTX]{
		sagaActionDispatcher:          sagaActionDispatcher,
		commandAlreadyHandledChecker:  commandAlreadyHandledChecker,
		transactionCompensatedChecker: transactionCompensatedChecker,
		commandHandledMarker:          commandHandledMarker,
	}
}

func (c Consumer[ID, TX, CTX]) Consume(ctx context.Context, command Command[ID, TX, CTX]) (err error) {
	defer func() {
		if err == nil {
			err = c.commandHandledMarker.mark(command.Id())
		}
	}()

	sagaAction, sagaActionFound := c.sagaActionDispatcher.SagaAction(command)
	if !sagaActionFound {
		return fmt.Errorf("no processor registered for command '%v'", command)
	}

	var alreadyHandled bool
	if alreadyHandled, err = c.commandAlreadyHandledChecker.check(command.Id()); err != nil {
		return fmt.Errorf("checking if command '%v' handled: %w", command, err)
	} else if alreadyHandled {
		return nil
	}

	if tx, ok := command.ToTransaction(); ok {
		var alreadyCompensated bool
		if alreadyCompensated, err = c.transactionCompensatedChecker.check(tx); err != nil {
			return fmt.Errorf("checking if tx '%v' compensated: %w", tx, err)
		} else if alreadyCompensated {
			return nil
		}
		return sagaAction.Execute(ctx, tx)
	}

	if compensatingTx, ok := command.ToCompensatingTransaction(); ok {
		return sagaAction.Compensate(ctx, compensatingTx)
	}

	return fmt.Errorf("command '%v' is not transaction nor compensating transaction", command)
}
