package alarm

import (
	"context"
	"fmt"
)

type RaiserPortOut interface {
	RaiseAlarm(ctx context.Context, raiseAlarmCommand RaiseAlarmCommand) error
	ClearAlarm(ctx context.Context, clearAlarmCommand ClearAlarmCommand) error
}

type Raiser struct {
	portOut RaiserPortOut
}

func NewAlarmRaiser(portOut RaiserPortOut) Raiser {
	return Raiser{portOut: portOut}
}

func (ar Raiser) Execute(ctx context.Context, cmd RaiseAlarmCommand) error {
	return ar.raiseAlarm(ctx, cmd)
}

func (ar Raiser) raiseAlarm(ctx context.Context, cmd RaiseAlarmCommand) error {
	if err := ar.portOut.RaiseAlarm(ctx, cmd); err != nil {
		return fmt.Errorf("raising alarm of player %s %d/%d: %w", cmd.PlayerID, cmd.Value, cmd.AlarmValue, err)
	}
	return nil
}

type RaiseAlarmCommand struct {
	ID         string
	PlayerID   string
	AlarmValue int
	Value      int
}

func (r RaiseAlarmCommand) ToTransaction() (RaiseAlarmCommand, bool) {
	return r, true
}

func (r RaiseAlarmCommand) ToCompensatingTransaction() (ClearAlarmCommand, bool) {
	return ClearAlarmCommand{}, false
}

func NewRaiseAlarmCommand(id string, playerID string, alarmValue, value int) RaiseAlarmCommand {
	return RaiseAlarmCommand{
		ID:         id,
		PlayerID:   playerID,
		AlarmValue: alarmValue,
		Value:      value,
	}
}

func (ar Raiser) Compensate(ctx context.Context, cmd ClearAlarmCommand) error {
	return ar.clearAlarm(ctx, cmd)
}

func (ar Raiser) clearAlarm(ctx context.Context, cmd ClearAlarmCommand) error {
	if err := ar.portOut.ClearAlarm(ctx, cmd); err != nil {
		return fmt.Errorf("clearing alarm of player %s: %w", cmd.PlayerID, err)
	}
	return nil
}

type ClearAlarmCommand struct {
	ID       string
	PlayerID string
}

func (c ClearAlarmCommand) ToTransaction() (RaiseAlarmCommand, bool) {
	return RaiseAlarmCommand{}, false
}

func (c ClearAlarmCommand) ToCompensatingTransaction() (ClearAlarmCommand, bool) {
	return c, true
}

func NewClearAlarmCommand(id string, playerID string) ClearAlarmCommand {
	return ClearAlarmCommand{
		ID:       id,
		PlayerID: playerID,
	}
}

type ValueProviderPortOut interface {
	AlarmValue(ctx context.Context, playerID string) (int, error)
}

type ValueProvider struct {
	portOut ValueProviderPortOut
}

func NewAlarmValueProvider(portOut ValueProviderPortOut) ValueProvider {
	return ValueProvider{portOut: portOut}
}

func (avp ValueProvider) Provide(ctx context.Context, playerID string) (int, error) {
	alarmValue, err := avp.portOut.AlarmValue(ctx, playerID)
	if err != nil {
		return 0, fmt.Errorf("providing alarm value for player %s: %w", playerID, err)
	}
	return alarmValue, nil
}
