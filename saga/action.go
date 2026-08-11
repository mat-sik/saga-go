package saga

import "context"

type Action[TX, CTX any] interface {
	Execute(ctx context.Context, tx TX) error
	Compensate(ctx context.Context, tx CTX) error
}
