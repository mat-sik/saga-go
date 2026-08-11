package count

import (
	"context"

	"github.com/mat-sik/saga-go/examples/domain/tx"
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

func (a Action) Execute(ctx context.Context, _ tx.RegisterCommand) error {
	return a.incrementer.increment(ctx)
}

func (a Action) Compensate(ctx context.Context, _ tx.UnregisterCommand) error {
	return a.decrementer.decrement(ctx)
}
