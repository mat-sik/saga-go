package attrmap

import (
	"github.com/mat-sik/saga-go/examples/internal/domain/alarm"
	"go.opentelemetry.io/otel/attribute"
)

func AlarmRaise(cmd alarm.RaiseAlarmCommand) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("command.alarm.raise.id", cmd.ID),
		attribute.String("command.alarm.raise.player.id", cmd.PlayerID),
		attribute.Int("command.alarm.raise.alarm.value", cmd.AlarmValue),
		attribute.Int("command.alarm.raise.value", cmd.Value),
	}
}

func AlarmClear(cmd alarm.ClearAlarmCommand) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("command.alarm.clear.id", cmd.ID),
		attribute.String("command.alarm.clear.player.id", cmd.PlayerID),
	}
}
