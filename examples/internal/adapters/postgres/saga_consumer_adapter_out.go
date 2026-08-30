package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/saga-go/examples/internal/txctx"
)

func commandAlreadyHandled(ctx context.Context, tx pgx.Tx, transactionID string) (bool, error) {
	const query = `
    	SELECT EXISTS(SELECT 1 FROM processed_transactions WHERE transaction_id = $1)
	`

	var alreadyHandled bool
	if err := tx.QueryRow(ctx, query, transactionID).Scan(&alreadyHandled); err != nil {
		return false, fmt.Errorf("querying for already handled transaction %s: %w", transactionID, err)
	}
	return alreadyHandled, nil
}

func markCommandAsHandled(ctx context.Context, tx pgx.Tx, transactionID string, compensatedTransactionID *string) error {
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

func transactionCompensated(ctx context.Context, tx pgx.Tx, transactionID string) (bool, error) {
	const query = `
    	SELECT EXISTS(SELECT 1 FROM processed_transactions WHERE compensated_transaction_id = $1)
	`

	var alreadyCompensated bool
	if err := tx.QueryRow(ctx, query, transactionID).Scan(&alreadyCompensated); err != nil {
		return false, fmt.Errorf("querying for already compensated transaction %s: %w", transactionID, err)
	}
	return alreadyCompensated, nil
}

func WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(ctx context.Context) error) error {
	pgxTx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning tx: %w", err)
	}

	defer func() {
		if err != nil {
			if rollbackErr := pgxTx.Rollback(ctx); rollbackErr != nil {
				err = errors.Join(err, fmt.Errorf("rolling back: %w", rollbackErr))
			}
			return
		}
		if commitErr := pgxTx.Commit(ctx); commitErr != nil {
			err = errors.Join(err, fmt.Errorf("committing: %w", commitErr))
		}
	}()

	ctx = txctx.WithTx(ctx, pgxTx)

	return fn(ctx)
}
