package tx

import (
	"context"
	"fmt"
)

type AlarmRaiserPortOut interface {
	RaiseAlarm(ctx context.Context, playerID string, alarmValue, value int) error
	ClearAlarm(ctx context.Context, playerID string) error
}

type AlarmRaiser struct {
	portOut AlarmRaiserPortOut
}

func NewAlarmRaiser(portOut AlarmRaiserPortOut) AlarmRaiser {
	return AlarmRaiser{portOut: portOut}
}

func (ar AlarmRaiser) RaiseAlarm(ctx context.Context, playerID string, alarmValue, value int) error {
	if err := ar.portOut.RaiseAlarm(ctx, playerID, alarmValue, value); err != nil {
		return fmt.Errorf("raising alarm of player %s %d/%d: %w", playerID, value, alarmValue, err)
	}
	return nil
}

func (ar AlarmRaiser) ClearAlarm(ctx context.Context, playerID string) error {
	if err := ar.portOut.ClearAlarm(ctx, playerID); err != nil {
		return fmt.Errorf("clearing alarm of player %s: %w", playerID, err)
	}
	return nil
}

type AlarmValueProviderPortOut interface {
	AlarmValue(ctx context.Context, playerID string) (int, error)
}

type AlarmValueProvider struct {
	portOut AlarmValueProviderPortOut
}

func NewAlarmValueProvider(portOut AlarmValueProviderPortOut) AlarmValueProvider {
	return AlarmValueProvider{portOut: portOut}
}

func (avp AlarmValueProvider) provide(ctx context.Context, playerID string) (int, error) {
	alarmValue, err := avp.portOut.AlarmValue(ctx, playerID)
	if err != nil {
		return 0, fmt.Errorf("providing alarm value for player %s: %w", playerID, err)
	}
	return alarmValue, nil
}
