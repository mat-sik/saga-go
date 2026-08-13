package tx

import (
	"context"
	"fmt"
	"time"

	"github.com/mat-sik/saga-go/examples/domain/tx"
	"github.com/mat-sik/saga-go/examples/txctx"
)

var _ tx.AggregatePortOut = (*AggregateRepository)(nil)

type AggregateRepository struct{}

func NewAggregateRepository() *AggregateRepository {
	return &AggregateRepository{}
}

func (r *AggregateRepository) Upsert(ctx context.Context, id tx.RegisterID, value int) error {
	return r.shift(ctx, id, value)
}

func (r *AggregateRepository) Subtract(ctx context.Context, id tx.RegisterID, value int) error {
	return r.shift(ctx, id, -value)
}

func (r *AggregateRepository) shift(ctx context.Context, id tx.RegisterID, delta int) error {
	dbTx, err := txctx.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("extracting tx from ctx in aggregate repository: %w", err)
	}

	const query = `
		INSERT INTO transactions_aggregates (player_id, currency, day, value, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $5)
		ON CONFLICT (player_id, currency, day)
		DO UPDATE SET value = transactions_aggregates.value + $4, updated_at = $5
	`
	eventTime := id.Time.UTC()
	day := eventTime.Truncate(24 * time.Hour)

	if _, err = dbTx.Exec(
		ctx,
		query,
		id.ID.PlayerID,
		id.ID.Currency,
		day,
		delta,
		eventTime,
	); err != nil {
		return fmt.Errorf("upserting aggregate: %w", err)
	}
	return nil
}
