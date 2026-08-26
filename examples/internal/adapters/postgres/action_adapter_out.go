package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/saga-go/examples/internal/txctx"
	"github.com/mat-sik/saga-go/saga"
)

type ScopedTxAction[T, CT any] struct {
	pool   *pgxpool.Pool
	action saga.Action[T, CT]
}

func NewScopedTxAction[T, CT any](pool *pgxpool.Pool, action saga.Action[T, CT]) ScopedTxAction[T, CT] {
	return ScopedTxAction[T, CT]{
		pool:   pool,
		action: action,
	}
}

func (a ScopedTxAction[T, CT]) Execute(ctx context.Context, tx T) error {
	execute := func(ctx context.Context) error {
		return a.action.Execute(ctx, tx)
	}
	return a.withTx(ctx, execute)
}

func (a ScopedTxAction[T, CT]) Compensate(ctx context.Context, tx CT) error {
	compensate := func(ctx context.Context) error {
		return a.action.Compensate(ctx, tx)
	}
	return a.withTx(ctx, compensate)
}

func (a ScopedTxAction[T, CT]) withTx(ctx context.Context, fn func(ctx context.Context) error) error {
	pgxTx, err := a.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning tx: %w", err)
	}

	defer func() {
		if err != nil {
			if rollbackErr := pgxTx.Rollback(ctx); rollbackErr != nil {
				err = errors.Join(err, fmt.Errorf("rolling back: %w", rollbackErr))
			}
		}
	}()

	ctx = txctx.WithTx(ctx, pgxTx)

	return fn(ctx)
}
