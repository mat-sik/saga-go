package tx

import (
	"context"
	"fmt"
)

// TODO: add logic to send information about breeched limit alarm -> this should publish to topic
type AggregateSagaAction struct {
	portOut AggregatePortOut
}

func NewAggregateSagaAction(portOut AggregatePortOut) AggregateSagaAction {
	return AggregateSagaAction{
		portOut: portOut,
	}
}

func (a AggregateSagaAction) Execute(ctx context.Context, registerCommand RegisterCommand) error {
	if err := a.portOut.Upsert(ctx, registerCommand.RegisterID, registerCommand.Value); err != nil {
		return fmt.Errorf("upserting tx: %w", err)
	}
	return nil
}

func (a AggregateSagaAction) Compensate(ctx context.Context, unregisterCommand UnregisterCommand) error {
	if err := a.portOut.Subtract(ctx, unregisterCommand.RegisterCommand.RegisterID, unregisterCommand.RegisterCommand.Value); err != nil {
		return fmt.Errorf("subtracting from tx: %w", err)
	}
	return nil
}
