package tx

import (
	"context"
	"time"
)

type LogPortOut interface {
	InsertLog(ctx context.Context, registerCommand RegisterCommand) error
	InsertCompensatingLog(ctx context.Context, transactionID string, compensatedTransactionID string, createdAt time.Time) error
}
