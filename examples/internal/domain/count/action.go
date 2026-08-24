package count

import (
	"context"
	"fmt"

	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
)

type SagaAction struct {
	portOut PortOut
}

func NewSagaAction(portOut PortOut) SagaAction {
	return SagaAction{
		portOut: portOut,
	}
}

func (a SagaAction) Execute(ctx context.Context, _ tx.RegisterCommand) error {
	if err := a.portOut.Increment(ctx); err != nil {
		return fmt.Errorf("incrementing count: %w", err)
	}
	return nil
}

func (a SagaAction) Compensate(ctx context.Context, _ tx.UnregisterCommand) error {
	if err := a.portOut.Decrement(ctx); err != nil {
		return fmt.Errorf("decrementing count: %w", err)
	}
	return nil
}
