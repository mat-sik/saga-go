package postgres

import (
	"context"
	"fmt"
	"time"

	domain "github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"github.com/mat-sik/saga-go/examples/internal/txctx"
)

type AggregateRepository struct{}

func NewAggregateRepository() AggregateRepository {
	return AggregateRepository{}
}

func (r AggregateRepository) Upsert(ctx context.Context, id domain.RegisterID, value int) (int, error) {
	delta := value
	return r.shift(ctx, id, delta)
}

func (r AggregateRepository) Subtract(ctx context.Context, id domain.RegisterID, value int) (int, error) {
	delta := -value
	return r.shift(ctx, id, delta)
}

func (r AggregateRepository) shift(ctx context.Context, id domain.RegisterID, delta int) (int, error) {
	dbTx, err := txctx.FromContext(ctx)
	if err != nil {
		return 0, fmt.Errorf("extracting tx from ctx in aggregate repository: %w", err)
	}

	const query = `
		INSERT INTO transactions_aggregates (player_id, currency, day, value, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $5)
		ON CONFLICT (player_id, currency, day)
		DO UPDATE SET value = transactions_aggregates.value + $4, updated_at = $5
		RETURNING value
	`

	var updatedValue int

	playerID := id.ID.PlayerID
	currency := id.ID.Currency
	eventTime := id.Time.UTC()
	day := eventTime.Truncate(24 * time.Hour)

	if err = dbTx.QueryRow(ctx, query,
		playerID,
		currency,
		day,
		delta,
		eventTime,
	).Scan(&updatedValue); err != nil {
		return 0, fmt.Errorf(
			"upserting aggregate (%s, %s, %s) delta %d eventTime %s: %w",
			playerID, currency, day, delta, eventTime, err,
		)
	}
	return updatedValue, nil
}
