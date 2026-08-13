package txctx

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

type txKey struct{}

var key = txKey{}

var ErrNoTx = errors.New("no transaction in context")

func WithTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, key, tx)
}

func FromContext(ctx context.Context) (pgx.Tx, error) {
	if tx, ok := ctx.Value(key).(pgx.Tx); ok {
		return tx, nil
	}
	return nil, ErrNoTx
}
