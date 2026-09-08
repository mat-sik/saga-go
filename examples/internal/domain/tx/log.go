package tx

import (
	"context"
	"fmt"
	"time"
)

type LogPortOut interface {
	InsertLog(ctx context.Context, registerCommand RegisterCommand) error
	InsertCompensatingLog(ctx context.Context, transactionID string, compensatedTransactionID string, createdAt time.Time) error
}

type LogSagaAction struct {
	portOut LogPortOut
}

func NewLogSagaAction(portOut LogPortOut) LogSagaAction {
	return LogSagaAction{
		portOut: portOut,
	}
}

func (l LogSagaAction) Register(ctx context.Context, registerCommand RegisterCommand) error {
	if err := l.portOut.InsertLog(ctx, registerCommand); err != nil {
		return fmt.Errorf("inserting log: %w", err)
	}
	return nil
}

func (l LogSagaAction) Unregister(ctx context.Context, unregisterCommand UnregisterCommand) error {
	if err := l.portOut.InsertCompensatingLog(
		ctx,
		unregisterCommand.ID,
		unregisterCommand.RegisterCommand.RegisterID.ID.TransactionID,
		unregisterCommand.Time,
	); err != nil {
		return fmt.Errorf("inserting compensating log: %w", err)
	}
	return nil
}
