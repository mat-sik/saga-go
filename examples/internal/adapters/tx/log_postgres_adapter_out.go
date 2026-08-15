package tx

import (
	"context"
	"fmt"
	"time"

	domain "github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"github.com/mat-sik/saga-go/examples/internal/txctx"
)

type LogRepository struct{}

func NewLogRepository() *LogRepository {
	return &LogRepository{}
}

func (r *LogRepository) InsertLog(ctx context.Context, registerCommand domain.RegisterCommand) error {
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

	transactionID := id.ID.TransactionID
	playerID := id.ID.PlayerID
	currency := id.ID.Currency
	value := registerCommand.Value

	if _, err = dbTx.Exec(ctx, query,
		transactionID,
		playerID,
		currency,
		day,
		value,
		eventTime,
	); err != nil {
		return fmt.Errorf(
			"inserting transaction log '(%s, %s, %s, %s, %d, %s)': %w",
			transactionID, playerID, currency, day, value, eventTime, err,
		)
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

	eventTime := createdAt.UTC()

	if _, err = dbTx.Exec(ctx, query, transactionID, compensatedTransactionID, eventTime); err != nil {
		return fmt.Errorf(
			"inserting compensating transaction log '(%s, %s, %s)': %w",
			transactionID, compensatedTransactionID, eventTime, err,
		)
	}
	return nil
}
