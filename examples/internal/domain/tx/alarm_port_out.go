package tx

import "context"

type AlarmRaiserPortOut interface {
	RaiseAlarm(ctx context.Context, playerID string, alarmValue, value int) error
	ClearAlarm(ctx context.Context, playerID string) error
}

type AlarmValueProviderPortOut interface {
	AlarmValue(ctx context.Context, playerID string) (int, error)
}
