package saga

import (
	"context"
	"fmt"
)

type Identifiable[ID any] interface {
	Id() ID
}

type Command[ID, T, CT any] interface {
	Identifiable[ID]
	ToTransaction() (T, bool)
	ToCompensatingTransaction() (CT, bool)
}

type Consumer[ID, T, CT any] struct {
	sagaActions []Action[T, CT]
	portOut     PortOut[ID, T, CT]
}

func NewConsumer[ID, T, CT any](sagaActions []Action[T, CT], portOut PortOut[ID, T, CT]) Consumer[ID, T, CT] {
	return Consumer[ID, T, CT]{
		sagaActions: sagaActions,
		portOut:     portOut,
	}
}

func (c Consumer[ID, T, CT]) Consume(ctx context.Context, command Command[ID, T, CT]) (err error) {
	defer func() {
		if err == nil {
			err = c.portOut.MarkCommandAsHandled(command.Id())
		}
	}()

	var alreadyHandled bool
	if alreadyHandled, err = c.portOut.CommandAlreadyHandled(command.Id()); err != nil {
		return fmt.Errorf("checking if command '%v' handled: %w", command, err)
	} else if alreadyHandled {
		return nil
	}

	var sagaActionConsumer func(context.Context, Action[T, CT]) error
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

func (c Consumer[ID, T, CT]) newSagaActionConsumer(command Command[ID, T, CT]) (func(context.Context, Action[T, CT]) error, error) {
	if tx, ok := command.ToTransaction(); ok {
		if alreadyCompensated, err := c.portOut.TransactionCompensated(tx); err != nil {
			return nil, fmt.Errorf("checking if tx '%v' compensated: %w", tx, err)
		} else if alreadyCompensated {
			return nil, nil
		}

		return func(ctx context.Context, sagaAction Action[T, CT]) error {
			return sagaAction.Execute(ctx, tx)
		}, nil
	}

	if compensatingTx, ok := command.ToCompensatingTransaction(); ok {
		return func(ctx context.Context, sagaAction Action[T, CT]) error {
			return sagaAction.Compensate(ctx, compensatingTx)
		}, nil
	}

	return nil, fmt.Errorf("command '%v' is not transaction nor compensating transaction", command)
}
