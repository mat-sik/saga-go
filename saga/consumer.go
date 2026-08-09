package saga

import (
	"context"
	"fmt"
)

type Command interface {
	ToTransaction() (Transaction, bool)
	ToCompensatingTransaction() (CompensatingTransaction, bool)
}

type ActionDispatcher interface {
	SagaAction(command Command) (Action, bool)
}

type Consumer struct {
	sagaActionDispatcher ActionDispatcher
}

func NewConsumer(sagaActionDispatcher ActionDispatcher) Consumer {
	return Consumer{
		sagaActionDispatcher: sagaActionDispatcher,
	}
}

func (c Consumer) Consume(ctx context.Context, command Command) error {
	sagaAction, sagaActionFound := c.sagaActionDispatcher.SagaAction(command)
	if !sagaActionFound {
		return fmt.Errorf("no processor registered for command '%v'", command)
	}

	if tx, ok := command.ToTransaction(); ok {
		return sagaAction.Execute(ctx, tx)
	}

	if compensatingTx, ok := command.ToCompensatingTransaction(); ok {
		return sagaAction.Compensate(ctx, compensatingTx)
	}

	return fmt.Errorf("command '%v' is not transaction nor compensating transaction", command)
}
