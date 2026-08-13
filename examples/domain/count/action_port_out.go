package count

import "context"

type OutPort interface {
	Increment(ctx context.Context) error
	Decrement(ctx context.Context) error
}
