package saga

import "context"

type PortOut[ID, T, CT any] interface {
	CommandAlreadyHandled(ctx context.Context, id ID) (bool, error)
	MarkCommandAsHandled(ctx context.Context, id ID) error
	TransactionCompensated(ctx context.Context, tx T) (bool, error)
}
