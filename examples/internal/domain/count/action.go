package count

import (
	"context"
	"fmt"

	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
)

type Action struct {
	portOut PortOut
}

func NewAction(portOut PortOut) Action {
	return Action{
		portOut: portOut,
	}
}

func (a Action) Execute(ctx context.Context, _ tx.RegisterCommand) error {
	if err := a.portOut.Increment(ctx); err != nil {
		return fmt.Errorf("incrementing count: %w", err)
	}
	return nil
}

func (a Action) Compensate(ctx context.Context, _ tx.UnregisterCommand) error {
	if err := a.portOut.Decrement(ctx); err != nil {
		return fmt.Errorf("decrementing count: %w", err)
	}
	return nil
}
