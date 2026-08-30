package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/saga-go/examples/internal/adapters/postgres"
	"github.com/mat-sik/saga-go/examples/internal/domain/alarm"
	"github.com/mat-sik/saga-go/examples/internal/kgoconsumer"
	"github.com/mat-sik/saga-go/saga"
	"github.com/twmb/franz-go/pkg/kgo"
)

func NewAlarmSagaConsumer(
	client kgoconsumer.Client,
	pool *pgxpool.Pool,
	dlqTopic string,
	sagaConsumer saga.Consumer[alarm.RaiseAlarmCommand, alarm.ClearAlarmCommand],
	options ...kgoconsumer.Option,
) (kgoconsumer.Consumer, error) {
	recordConsumer := func(ctx context.Context, record *kgo.Record) error {
		cmd, err := mapToAlarmCommand(record)
		if err != nil {
			return err
		}
		consume := func(ctx context.Context) error {
			return sagaConsumer.Consume(ctx, cmd)
		}
		return postgres.WithTx(ctx, pool, consume)
	}

	return kgoconsumer.NewConsumer(
		client,
		dlqTopic,
		recordConsumer,
		options...,
	)
}

func mapToAlarmCommand(record *kgo.Record) (saga.Command[alarm.RaiseAlarmCommand, alarm.ClearAlarmCommand], error) {
	alarmType, err := headerValue(record, AlarmTypeHeader)
	if err != nil {
		return nil, err
	}

	playerID := string(record.Key)

	switch alarmType {
	case AlarmTypeRaise:
		var alarmRecord RaiseAlarmRecord
		if err := json.Unmarshal(record.Value, &alarmRecord); err != nil {
			return nil, fmt.Errorf("unmarshaling raiseAlarmRecord %v: %w", record, err)
		}
		return alarmRecord.toRaiseAlarmCommand(playerID), nil
	case AlarmTypeClear:
		var alarmRecord ClearAlarmRecord
		if err := json.Unmarshal(record.Value, &alarmRecord); err != nil {
			return nil, fmt.Errorf("unmarshaling clearAlarmRecord %v: %w", record, err)
		}
		return alarmRecord.toClearAlarmCommand(playerID), nil
	default:
		return nil, fmt.Errorf("unsupported alarmType %s", alarmType)
	}
}
