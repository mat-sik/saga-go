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

type Log interface {
	Register(ctx context.Context, registerCommand RegisterCommand) error
	Unregister(ctx context.Context, unregisterCommand UnregisterCommand) error
}

type log struct {
	portOut LogPortOut
}

func NewLog(portOut LogPortOut) Log {
	return log{
		portOut: portOut,
	}
}

func (l log) Register(ctx context.Context, registerCommand RegisterCommand) error {
	if err := l.portOut.InsertLog(ctx, registerCommand); err != nil {
		return fmt.Errorf("inserting log: %w", err)
	}
	return nil
}

func (l log) Unregister(ctx context.Context, unregisterCommand UnregisterCommand) error {
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
