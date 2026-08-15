package count

import "context"

type PortOut interface {
	Increment(ctx context.Context) error
	Decrement(ctx context.Context) error
}
