package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mat-sik/saga-go/examples/internal/domain/alarm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type AlarmProducer struct {
	client *kgo.Client
	topic  string
}

func NewAlarmProducer(client *kgo.Client, topic string) AlarmProducer {
	return AlarmProducer{
		client: client,
		topic:  topic,
	}
}

func (a AlarmProducer) RaiseAlarm(ctx context.Context, cmd alarm.RaiseAlarmCommand) error {
	payload := newRaiseAlarmRecord(cmd)
	return a.produce(ctx, cmd.PlayerID, payload, AlarmTypeRaise)
}

func (a AlarmProducer) ClearAlarm(ctx context.Context, cmd alarm.ClearAlarmCommand) error {
	payload := newClearAlarmRecord(cmd)
	return a.produce(ctx, cmd.PlayerID, payload, AlarmTypeClear)
}

func (a AlarmProducer) produce(ctx context.Context, key string, payload any, alarmType string) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling record %v: %w", payload, err)
	}
	record := &kgo.Record{
		Key:   []byte(key),
		Value: body,
		Headers: []kgo.RecordHeader{
			{
				Key:   AlarmTypeHeader,
				Value: []byte(alarmType),
			},
		},
		Topic: a.topic,
	}
	result := a.client.ProduceSync(ctx, record)
	return result.FirstErr()
}

type RaiseAlarmRecord struct {
	ID         string `json:"id"`
	AlarmValue int    `json:"alarmValue"`
	Value      int    `json:"value"`
}

func newRaiseAlarmRecord(cmd alarm.RaiseAlarmCommand) RaiseAlarmRecord {
	return RaiseAlarmRecord{
		ID:         cmd.ID,
		AlarmValue: cmd.AlarmValue,
		Value:      cmd.Value,
	}
}

func (r RaiseAlarmRecord) ToRaiseAlarmCommand(playerID string) alarm.RaiseAlarmCommand {
	return alarm.NewRaiseAlarmCommand(r.ID, playerID, r.AlarmValue, r.Value)
}

type ClearAlarmRecord struct {
	ID string `json:"id"`
}

func newClearAlarmRecord(cmd alarm.ClearAlarmCommand) ClearAlarmRecord {
	return ClearAlarmRecord{
		ID: cmd.ID,
	}
}

func (r ClearAlarmRecord) ToClearAlarmCommand(playerID string) alarm.ClearAlarmCommand {
	return alarm.NewClearAlarmCommand(r.ID, playerID)
}

const (
	AlarmTypeHeader = "alarm-type"
	AlarmTypeRaise  = "raise"
	AlarmTypeClear  = "clear"
)
