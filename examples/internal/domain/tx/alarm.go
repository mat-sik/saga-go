package tx

import (
	"context"
	"fmt"
)

type alarmRaiser struct {
	portOut AlarmRaiserPortOut
}

func (ar alarmRaiser) RaiseAlarm(ctx context.Context, playerID string, alarmValue, value int) error {
	if err := ar.portOut.RaiseAlarm(ctx, playerID, alarmValue, value); err != nil {
		return fmt.Errorf("raising alarm of player %s %d/%d: %w", playerID, value, alarmValue, err)
	}
	return nil
}

func (ar alarmRaiser) ClearAlarm(ctx context.Context, playerID string) error {
	if err := ar.portOut.ClearAlarm(ctx, playerID); err != nil {
		return fmt.Errorf("clearing alarm of player %s: %w", playerID, err)
	}
	return nil
}

type alarmValueProvider struct {
	portOut AlarmValueProviderPortOut
}

func (avp alarmValueProvider) provide(ctx context.Context, playerID string) (int, error) {
	alarmValue, err := avp.portOut.AlarmValue(ctx, playerID)
	if err != nil {
		return 0, fmt.Errorf("providing alarm value for player %s: %w", playerID, err)
	}
	return alarmValue, nil
}
