package kafka

import (
	"context"
	"encoding/json"
	"fmt"

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

func (a AlarmProducer) RaiseAlarm(ctx context.Context, playerID string, alarmValue, value int) error {
	return a.produce(ctx, playerID, RaiseAlarmRecord{
		PlayerID:   playerID,
		AlarmValue: alarmValue,
		Value:      value,
	})
}

func (a AlarmProducer) ClearAlarm(ctx context.Context, playerID string) error {
	return a.produce(ctx, playerID, ClearAlarmRecord{PlayerID: playerID})
}

func (a AlarmProducer) produce(ctx context.Context, playerID string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling record %v: %w", payload, err)
	}
	record := &kgo.Record{
		Topic: a.topic,
		Key:   []byte(playerID),
		Value: body,
	}
	result := a.client.ProduceSync(ctx, record)
	return result.FirstErr()
}

type RaiseAlarmRecord struct {
	PlayerID   string `json:"PlayerID"`
	AlarmValue int    `json:"AlarmValue"`
	Value      int    `json:"Value"`
}

type ClearAlarmRecord struct {
	PlayerID string `json:"PlayerID"`
}
