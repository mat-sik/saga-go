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
	payload := RaiseAlarmRecord{
		PlayerID:   playerID,
		AlarmValue: alarmValue,
		Value:      value,
	}
	return a.produce(ctx, playerID, payload, AlarmTypeRaise)
}

func (a AlarmProducer) ClearAlarm(ctx context.Context, playerID string) error {
	payload := ClearAlarmRecord{
		PlayerID: playerID,
	}
	return a.produce(ctx, playerID, payload, AlarmTypeClear)
}

func (a AlarmProducer) produce(ctx context.Context, playerID string, payload any, alarmType string) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling record %v: %w", payload, err)
	}
	record := &kgo.Record{
		Key:   []byte(playerID),
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
	PlayerID   string `json:"playerID"`
	AlarmValue int    `json:"alarmValue"`
	Value      int    `json:"value"`
}

func MapToRaiseAlarmRecord(record *kgo.Record) (RaiseAlarmRecord, bool, error) {
	alarmType, err := headerValue(record, AlarmTypeHeader)
	if err != nil {
		return RaiseAlarmRecord{}, false, err
	}
	if alarmType != AlarmTypeRaise {
		return RaiseAlarmRecord{}, false, nil
	}
	var out RaiseAlarmRecord
	if err := json.Unmarshal(record.Value, &out); err != nil {
		return RaiseAlarmRecord{}, false, fmt.Errorf("unmarshaling raiseAlarmRecord %v: %w", record, err)
	}
	return out, true, nil
}

type ClearAlarmRecord struct {
	PlayerID string `json:"playerID"`
}

func MapToClearAlarmRecord(record *kgo.Record) (ClearAlarmRecord, bool, error) {
	alarmType, err := headerValue(record, AlarmTypeHeader)
	if err != nil {
		return ClearAlarmRecord{}, false, err
	}
	if alarmType != AlarmTypeClear {
		return ClearAlarmRecord{}, false, nil
	}
	var out ClearAlarmRecord
	if err := json.Unmarshal(record.Value, &out); err != nil {
		return ClearAlarmRecord{}, false, fmt.Errorf("unmarshaling clearAlarmRecord %v: %w", record, err)
	}
	return out, true, nil
}

const (
	AlarmTypeHeader = "alarm-type"
	AlarmTypeRaise  = "raise"
	AlarmTypeClear  = "clear"
)
