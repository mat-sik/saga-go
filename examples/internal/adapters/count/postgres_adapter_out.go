package count

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/mat-sik/saga-go/examples/internal/txctx"
)

type Repository struct{}

func NewCountRepository() *Repository {
	return &Repository{}
}

func (r *Repository) Increment(ctx context.Context) error {
	return r.shift(ctx, 1)
}

func (r *Repository) Decrement(ctx context.Context) error {
	return r.shift(ctx, -1)
}

func (r *Repository) shift(ctx context.Context, delta int) error {
	tx, err := txctx.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("extracting tx in count repository: %w", err)
	}

	const updateQuery = `UPDATE counts SET count = count + $1`
	tag, err := tx.Exec(ctx, updateQuery, delta)
	if err != nil {
		return fmt.Errorf("updating count: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if err = r.insert(ctx, tx, delta); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) insert(ctx context.Context, tx pgx.Tx, delta int) error {
	const insertQuery = `INSERT INTO counts (count) VALUES ($1)`
	if _, err := tx.Exec(ctx, insertQuery, delta); err != nil {
		return fmt.Errorf("inserting count: %w", err)
	}
	return nil
}
