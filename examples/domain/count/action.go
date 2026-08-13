package count

import (
	"context"
	"fmt"

	"github.com/mat-sik/saga-go/examples/domain/tx"
)

type Action struct {
	OutPort OutPort
}

func NewAction(outPort OutPort) Action {
	return Action{
		OutPort: outPort,
	}
}

func (a Action) Execute(ctx context.Context, _ tx.RegisterCommand) error {
	if err := a.OutPort.Increment(ctx); err != nil {
		return fmt.Errorf("incrementing count: %w", err)
	}
	return nil
}

func (a Action) Compensate(ctx context.Context, _ tx.UnregisterCommand) error {
	if err := a.OutPort.Decrement(ctx); err != nil {
		return fmt.Errorf("decrementing count: %w", err)
	}
	return nil
}
