package tx

import "context"

type AggregatePortOut interface {
	Upsert(ctx context.Context, id RegisterID, value int) (int, error)
	Subtract(ctx context.Context, id RegisterID, value int) (int, error)
}
