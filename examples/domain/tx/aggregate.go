package tx

import (
	"context"
	"fmt"
)

type AggregateUpserter interface {
	Upsert(ctx context.Context, id RegisterID, value int) error
}

type AggregateValueSubtractor interface {
	subtract(ctx context.Context, id RegisterID, value int) error
}

type AggregateSagaAction struct {
	aggregateUpserter        AggregateUpserter
	aggregateValueSubtractor AggregateValueSubtractor
}

func NewAggregateSagaAction(
	aggregateUpserter AggregateUpserter,
	aggregateValueSubtractor AggregateValueSubtractor,
) AggregateSagaAction {
	return AggregateSagaAction{
		aggregateUpserter:        aggregateUpserter,
		aggregateValueSubtractor: aggregateValueSubtractor,
	}
}

func (a AggregateSagaAction) Execute(ctx context.Context, registerCommand RegisterCommand) error {
	if err := a.aggregateUpserter.Upsert(ctx, registerCommand.RegisterID, registerCommand.Value); err != nil {
		return fmt.Errorf("upserting tx: %w", err)
	}
	return nil
}

func (a AggregateSagaAction) Compensate(ctx context.Context, unregisterCommand UnregisterCommand) error {
	return a.aggregateValueSubtractor.subtract(ctx, unregisterCommand.RegisterCommand.RegisterID, unregisterCommand.RegisterCommand.Value)
}
