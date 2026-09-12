package saga

import (
	"context"
	"fmt"
)

type Command[T, CT any] interface {
	ToTransaction() (T, bool)
	ToCompensatingTransaction() (CT, bool)
}

type PortOut[T, CT any] interface {
	CommandAlreadyHandled(ctx context.Context, command Command[T, CT]) (bool, error)
	MarkCommandAsHandled(ctx context.Context, command Command[T, CT]) error
	TransactionCompensated(ctx context.Context, tx T) (bool, error)
}

type Consumer[T, CT any] interface {
	Consume(ctx context.Context, command Command[T, CT]) (err error)
}

type consumer[T, CT any] struct {
	sagaActions []Action[T, CT]
	portOut     PortOut[T, CT]
	observer    Observer
}

func NewConsumer[T, CT any](sagaActions []Action[T, CT], portOut PortOut[T, CT], observer Observer) Consumer[T, CT] {
	return consumer[T, CT]{
		sagaActions: sagaActions,
		portOut:     portOut,
		observer:    observer,
	}
}

func (c consumer[T, CT]) Consume(ctx context.Context, command Command[T, CT]) (err error) {
	var alreadyHandled bool

	defer func() {
		if err == nil && !alreadyHandled {
			if err = c.portOut.MarkCommandAsHandled(ctx, command); err != nil {
				err = fmt.Errorf("marking command as handled %v: %w", command, err)
			}
		}
	}()

	if alreadyHandled, err = c.portOut.CommandAlreadyHandled(ctx, command); err != nil {
		return fmt.Errorf("checking if command %v handled: %w", command, err)
	} else if alreadyHandled {
		c.observer.Observe(ctx, EventAlreadyHandled)
		return nil
	}

	var sagaActionConsumer func(context.Context, Action[T, CT]) error
	sagaActionConsumer, err = c.newSagaActionConsumer(ctx, command)
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

func (c consumer[T, CT]) newSagaActionConsumer(ctx context.Context, command Command[T, CT]) (func(context.Context, Action[T, CT]) error, error) {
	if tx, ok := command.ToTransaction(); ok {
		if alreadyCompensated, err := c.portOut.TransactionCompensated(ctx, tx); err != nil {
			return nil, fmt.Errorf("checking if tx %v compensated: %w", tx, err)
		} else if alreadyCompensated {
			c.observer.Observe(ctx, EventAlreadyCompensated)
			return nil, nil
		}

		return func(ctx context.Context, sagaAction Action[T, CT]) error {
			if err := sagaAction.Execute(ctx, tx); err != nil {
				return fmt.Errorf("executing tx %v: %w", tx, err)
			}
			return nil
		}, nil
	}

	if compensatingTx, ok := command.ToCompensatingTransaction(); ok {
		return func(ctx context.Context, sagaAction Action[T, CT]) error {
			if err := sagaAction.Compensate(ctx, compensatingTx); err != nil {
				return fmt.Errorf("compensating with %v: %w", command, err)
			}
			return nil
		}, nil
	}

	return nil, fmt.Errorf("command %v is not transaction nor compensating transaction", command)
}

type Observer interface {
	Observe(context.Context, Event)
}

type Event int

const (
	EventAlreadyHandled     Event = iota
	EventAlreadyCompensated Event = iota
)
