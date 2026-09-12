package alarm

import (
	"context"
	"fmt"
)

type RaiserPortOut interface {
	RaiseAlarm(ctx context.Context, raiseAlarmCommand RaiseAlarmCommand) error
	ClearAlarm(ctx context.Context, clearAlarmCommand ClearAlarmCommand) error
}

type Raiser interface {
	RaiseAlarm(ctx context.Context, cmd RaiseAlarmCommand) error
	ClearAlarm(ctx context.Context, cmd ClearAlarmCommand) error
}

type raiser struct {
	portOut RaiserPortOut
}

func NewAlarmRaiser(portOut RaiserPortOut) Raiser {
	return raiser{portOut: portOut}
}

func (r raiser) RaiseAlarm(ctx context.Context, cmd RaiseAlarmCommand) error {
	if err := r.portOut.RaiseAlarm(ctx, cmd); err != nil {
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

func NewRaiseAlarmCommand(id string, playerID string, alarmValue, value int) RaiseAlarmCommand {
	return RaiseAlarmCommand{
		ID:         id,
		PlayerID:   playerID,
		AlarmValue: alarmValue,
		Value:      value,
	}
}

func (r raiser) ClearAlarm(ctx context.Context, cmd ClearAlarmCommand) error {
	if err := r.portOut.ClearAlarm(ctx, cmd); err != nil {
		return fmt.Errorf("clearing alarm of player %s: %w", cmd.PlayerID, err)
	}
	return nil
}

type ClearAlarmCommand struct {
	ID       string
	PlayerID string
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
