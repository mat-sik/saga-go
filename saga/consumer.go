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
	sagaActions                   []Action[TX, CTX]
	commandAlreadyHandledChecker  CommandAlreadyHandledChecker[ID]
	transactionCompensatedChecker TransactionCompensatedChecker[TX]
	commandHandledMarker          CommandHandledMarker[ID]
}

func NewConsumer[ID, TX, CTX any](
	sagaActions []Action[TX, CTX],
	commandAlreadyHandledChecker CommandAlreadyHandledChecker[ID],
	transactionCompensatedChecker TransactionCompensatedChecker[TX],
	commandHandledMarker CommandHandledMarker[ID],
) Consumer[ID, TX, CTX] {
	return Consumer[ID, TX, CTX]{
		sagaActions:                   sagaActions,
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

	var alreadyHandled bool
	if alreadyHandled, err = c.commandAlreadyHandledChecker.check(command.Id()); err != nil {
		return fmt.Errorf("checking if command '%v' handled: %w", command, err)
	} else if alreadyHandled {
		return nil
	}

	var sagaActionConsumer func(context.Context, Action[TX, CTX]) error
	sagaActionConsumer, err = c.newSagaActionConsumer(command)
	if sagaActionConsumer == nil || err != nil {
		return err
	}

	for _, sagaAction := range c.sagaActions {
		if err = sagaActionConsumer(ctx, sagaAction); err != nil {
			return err
		}
	}

	return nil
}

func (c Consumer[ID, TX, CTX]) newSagaActionConsumer(command Command[ID, TX, CTX]) (func(context.Context, Action[TX, CTX]) error, error) {
	if tx, ok := command.ToTransaction(); ok {
		if alreadyCompensated, err := c.transactionCompensatedChecker.check(tx); err != nil {
			return nil, fmt.Errorf("checking if tx '%v' compensated: %w", tx, err)
		} else if alreadyCompensated {
			return nil, nil
		}

		return func(ctx context.Context, sagaAction Action[TX, CTX]) error {
			return sagaAction.Execute(ctx, tx)
		}, nil
	}

	if compensatingTx, ok := command.ToCompensatingTransaction(); ok {
		return func(ctx context.Context, sagaAction Action[TX, CTX]) error {
			return sagaAction.Compensate(ctx, compensatingTx)
		}, nil
	}

	return nil, fmt.Errorf("command '%v' is not transaction nor compensating transaction", command)
}
