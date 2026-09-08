package idempotent

import (
	"context"
	"fmt"
)

type PortOut[T any] interface {
	MessageAlreadyHandled(ctx context.Context, message T) (bool, error)
	MarkMessageAsHandled(ctx context.Context, message T) error
}

type Consumer[T any] struct {
	recordConsumers []func(context.Context, T) error
	portOut         PortOut[T]
}

func NewConsumer[T any](recordConsumers []func(context.Context, T) error, portOut PortOut[T]) Consumer[T] {
	return Consumer[T]{
		recordConsumers: recordConsumers,
		portOut:         portOut,
	}
}

func (c Consumer[T]) Consume(ctx context.Context, message T) (err error) {
	var alreadyHandled bool

	defer func() {
		if err == nil && !alreadyHandled {
			if err = c.portOut.MarkMessageAsHandled(ctx, message); err != nil {
				err = fmt.Errorf("marking message as handled %v: %w", message, err)
			}
		}
	}()

	if alreadyHandled, err = c.portOut.MessageAlreadyHandled(ctx, message); err != nil {
		return fmt.Errorf("checking if message %v handled: %w", message, err)
	} else if alreadyHandled {
		return nil
	}

	for _, recordConsumer := range c.recordConsumers {
		if err = recordConsumer(ctx, message); err != nil {
			return err
		}
	}

	return nil
}
