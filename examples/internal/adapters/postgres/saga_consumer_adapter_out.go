package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type sagaConsumerRepository struct {
}

func (s sagaConsumerRepository) commandAlreadyHandled(
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

func (s sagaConsumerRepository) markCommandAsHandled(
	ctx context.Context,
	tx pgx.Tx,
	transactionID string,
	compensatedTransactionID *string,
) error {
	const query = `
		INSERT INTO processed_transactions (transaction_id, compensated_transaction_id)
		VALUES ($1, $2)
	`

	if _, err := tx.Exec(ctx, query, transactionID, compensatedTransactionID); err != nil {
		if compensatedTransactionID != nil {
			return fmt.Errorf(
				"marking transaction %s compensating %s as handled: %w",
				transactionID, *compensatedTransactionID, err,
			)
		}
		return fmt.Errorf("marking transaction %s as handled: %w", transactionID, err)
	}

	return nil
}

func (s sagaConsumerRepository) transactionCompensated(
	ctx context.Context,
	tx pgx.Tx,
	transactionID string,
) (bool, error) {
	const query = `
    	SELECT EXISTS(SELECT 1 FROM processed_transactions WHERE compensated_transaction_id = $1)
	`

	var alreadyCompensated bool
	if err := tx.QueryRow(ctx, query, transactionID).Scan(&alreadyCompensated); err != nil {
		return false, fmt.Errorf("querying for already compensated transaction %s: %w", transactionID, err)
	}
	return alreadyCompensated, nil
}
