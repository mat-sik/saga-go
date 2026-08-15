package saga

import "context"

type PortOut[T, CT any] interface {
	CommandAlreadyHandled(ctx context.Context, command Command[T, CT]) (bool, error)
	MarkCommandAsHandled(ctx context.Context, command Command[T, CT]) error
	TransactionCompensated(ctx context.Context, tx T) (bool, error)
}
