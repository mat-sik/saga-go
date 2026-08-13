package tx

import "context"

type LogPortOut interface {
	InsertLog(ctx context.Context, registerCommand RegisterCommand) error
	InsertCompensatingLog(ctx context.Context, registerCommand RegisterCommand) error
}
