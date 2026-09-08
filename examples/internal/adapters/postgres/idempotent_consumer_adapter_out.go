package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type idempotentConsumerRepository struct {
}

func (i idempotentConsumerRepository) commandAlreadyHandled(
	ctx context.Context,
	tx pgx.Tx,
	transactionID string,
) (bool, error) {
	const query = `
    	SELECT EXISTS(SELECT 1 FROM processed_transactions WHERE transaction_id = $1)
	`

	var alreadyHandled bool
	if err := tx.QueryRow(ctx, query, transactionID).Scan(&alreadyHandled); err != nil {
		return false, fmt.Errorf("querying for already handled transaction %s: %w", transactionID, err)
	}
	return alreadyHandled, nil
}

func (i idempotentConsumerRepository) markCommandAsHandled(
	ctx context.Context,
	tx pgx.Tx,
	transactionID string,
) error {
	const query = `
		INSERT INTO processed_transactions (transaction_id)
		VALUES ($1)
	`

	if _, err := tx.Exec(ctx, query, transactionID); err != nil {
		return fmt.Errorf("marking transaction %s as handled: %w", transactionID, err)
	}

	return nil
}
