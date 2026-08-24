package static

import (
	"context"
)

type AlarmValueProvider struct {
	threshold int
}

func NewAlarmValueProvider(alarmValue int) AlarmValueProvider {
	return AlarmValueProvider{
		threshold: alarmValue,
	}
}

func (a AlarmValueProvider) AlarmValue(_ context.Context, _ string) (int, error) {
	return a.threshold, nil
}
