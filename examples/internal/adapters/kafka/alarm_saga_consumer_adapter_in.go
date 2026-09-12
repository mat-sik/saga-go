package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mat-sik/saga-go/examples/internal/adapters/sagaadapters"
	"github.com/mat-sik/saga-go/kgoconsumer"
	"github.com/mat-sik/saga-go/saga"
	"github.com/twmb/franz-go/pkg/kgo"
)

type TxRunner func(ctx context.Context, fn func(context.Context) error) error

func NewAlarmSagaConsumer(
	client kgoconsumer.Client,
	runTx TxRunner,
	dlqTopic string,
	sagaConsumer saga.Consumer[sagaadapters.RaiseAlarmSagaCommand, sagaadapters.ClearAlarmSagaCommand],
	options ...kgoconsumer.Option,
) (kgoconsumer.Consumer, error) {
	recordConsumer := func(ctx context.Context, record *kgo.Record) error {
		cmd, err := mapToAlarmCommand(record)
		if err != nil {
			return err
		}
		consume := func(ctx context.Context) error {
			if err := sagaConsumer.Consume(ctx, cmd); err != nil {
				return fmt.Errorf("consuming alarm command %v: %w", cmd, err)
			}
			return nil
		}
		return runTx(ctx, consume)
	}

	return kgoconsumer.NewConsumer(
		client,
		dlqTopic,
		recordConsumer,
		options...,
	)
}

func mapToAlarmCommand(record *kgo.Record) (saga.Command[sagaadapters.RaiseAlarmSagaCommand, sagaadapters.ClearAlarmSagaCommand], error) {
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
		return newRaiseAlarmSagaCommand(alarmRecord, playerID), nil
	case AlarmTypeClear:
		var alarmRecord ClearAlarmRecord
		if err := json.Unmarshal(record.Value, &alarmRecord); err != nil {
			return nil, fmt.Errorf("unmarshaling clearAlarmRecord %v: %w", record, err)
		}
		return newClearAlarmSagaCommand(alarmRecord, playerID), nil
	default:
		return nil, fmt.Errorf("unsupported alarmType %s", alarmType)
	}
}

func newClearAlarmSagaCommand(record ClearAlarmRecord, playerID string) sagaadapters.ClearAlarmSagaCommand {
	return sagaadapters.ClearAlarmSagaCommand{
		ClearAlarmCommand: record.ToClearAlarmCommand(playerID),
	}
}

func newRaiseAlarmSagaCommand(record RaiseAlarmRecord, playerID string) sagaadapters.RaiseAlarmSagaCommand {
	return sagaadapters.RaiseAlarmSagaCommand{
		RaiseAlarmCommand: record.ToRaiseAlarmCommand(playerID),
	}
}
