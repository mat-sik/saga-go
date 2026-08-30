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
	payload := RaiseAlarmRecord{
		ID:         cmd.ID,
		PlayerID:   cmd.PlayerID,
		AlarmValue: cmd.AlarmValue,
		Value:      cmd.Value,
	}
	return a.produce(ctx, cmd.PlayerID, payload, AlarmTypeRaise)
}

func (a AlarmProducer) ClearAlarm(ctx context.Context, cmd alarm.ClearAlarmCommand) error {
	payload := ClearAlarmRecord{
		ID:       cmd.ID,
		PlayerID: cmd.PlayerID,
	}
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
	PlayerID   string `json:"playerID"`
	AlarmValue int    `json:"alarmValue"`
	Value      int    `json:"value"`
}

type ClearAlarmRecord struct {
	ID       string `json:"id"`
	PlayerID string `json:"playerID"`
}

const (
	AlarmTypeHeader = "alarm-type"
	AlarmTypeRaise  = "raise"
	AlarmTypeClear  = "clear"
)
