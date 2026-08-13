package tx

import (
	"context"
	"fmt"
)

type LogSagaAction struct {
	portOut LogPortOut
}

func NewLogSagaAction(portOut LogPortOut) LogSagaAction {
	return LogSagaAction{
		portOut: portOut,
	}
}

func (l LogSagaAction) Execute(ctx context.Context, registerCommand RegisterCommand) error {
	if err := l.portOut.InsertLog(ctx, registerCommand); err != nil {
		return fmt.Errorf("inserting log: %w", err)
	}
	return nil
}

func (l LogSagaAction) Compensate(ctx context.Context, unregisterCommand UnregisterCommand) error {
	if err := l.portOut.InsertCompensatingLog(ctx, unregisterCommand.RegisterCommand); err != nil {
		return fmt.Errorf("inserting compensating log: %w", err)
	}
	return nil
}
