package saga

import "context"

type Action[T, CT any] interface {
	Execute(ctx context.Context, tx T) error
	Compensate(ctx context.Context, tx CT) error
}
