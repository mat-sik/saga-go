package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/saga-go/examples/internal/txctx"
)

func WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(ctx context.Context) error) (err error) {
	defer func() {
		err = wrapIfTransient(err)
	}()

	pgxTx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning tx: %w", err)
	}

	defer func() {
		if err != nil {
			if rollbackErr := pgxTx.Rollback(ctx); rollbackErr != nil {
				err = errors.Join(err, fmt.Errorf("rolling back: %w", rollbackErr))
			}
		} else if commitErr := pgxTx.Commit(ctx); commitErr != nil {
			err = errors.Join(err, fmt.Errorf("committing: %w", commitErr))
		}
	}()

	ctx = txctx.WithTx(ctx, pgxTx)

	return fn(ctx)
}
