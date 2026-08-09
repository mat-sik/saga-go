package saga

import "context"

type Transaction any

type CompensatingTransaction any

type Action interface {
	Execute(ctx context.Context, tx Transaction) error
	Compensate(ctx context.Context, tx CompensatingTransaction) error
}
