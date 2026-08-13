package tx

import (
	"context"
	"fmt"
	"time"

	"github.com/mat-sik/saga-go/examples/domain/tx"
	"github.com/mat-sik/saga-go/examples/txctx"
)

var _ tx.LogPortOut = (*LogRepository)(nil)

type LogRepository struct{}

func NewLogRepository() *LogRepository {
	return &LogRepository{}
}

func (r *LogRepository) InsertLog(ctx context.Context, registerCommand tx.RegisterCommand) error {
	dbTx, err := txctx.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("extracting tx from ctx in log repository: %w", err)
	}

	id := registerCommand.RegisterID
	eventTime := id.Time.UTC()
	day := eventTime.Truncate(24 * time.Hour)

	const query = `
		INSERT INTO transaction_logs (transaction_id, player_id, currency, day, value, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	if _, err = dbTx.Exec(
		ctx,
		query,
		id.ID.TransactionID,
		id.ID.PlayerID,
		id.ID.Currency,
		day,
		registerCommand.Value,
		eventTime,
	); err != nil {
		return fmt.Errorf("inserting transaction log: %w", err)
	}
	return nil
}

func (r *LogRepository) InsertCompensatingLog(
	ctx context.Context,
	transactionID string,
	compensatedTransactionID string,
	createdAt time.Time,
) error {
	dbTx, err := txctx.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("extracting tx from ctx in log repository: %w", err)
	}

	const query = `
		INSERT INTO compensating_transaction_logs (transaction_id, compensated_transaction_id, created_at)
		VALUES ($1, $2, $3)
	`
	if _, err = dbTx.Exec(ctx, query, transactionID, compensatedTransactionID, createdAt.UTC()); err != nil {
		return fmt.Errorf("inserting compensating transaction log: %w", err)
	}
	return nil
}
