package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"github.com/mat-sik/saga-go/examples/internal/kgoconsumer"
	"github.com/mat-sik/saga-go/saga"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TODO: add transient and pernament errors in the adapters and domain logic
func NewSagaConsumer(
	client kgoconsumer.Client,
	dlqTopic string,
	sagaConsumer saga.Consumer[tx.RegisterCommand, tx.UnregisterCommand],
	options ...kgoconsumer.Option,
) (kgoconsumer.Consumer, error) {
	consumer := kgoSagaConsumer{
		consumer: sagaConsumer,
	}

	return kgoconsumer.NewConsumer(
		client,
		dlqTopic,
		consumer.consumeRecord,
		options...,
	)
}

type kgoSagaConsumer struct {
	consumer saga.Consumer[tx.RegisterCommand, tx.UnregisterCommand]
}

func (k kgoSagaConsumer) consumeRecord(ctx context.Context, record *kgo.Record) (err error) {
	command, err := mapToCommand(record)
	if err != nil {
		return err
	}

	if err = k.consumer.Consume(ctx, command); err != nil {
		return fmt.Errorf("consuming command %v: %w", command, err)
	}
	return nil
}

func mapToCommand(record *kgo.Record) (saga.Command[tx.RegisterCommand, tx.UnregisterCommand], error) {
	cmdType, err := headerValue(record, CmdTypeHeader)
	if err != nil {
		return nil, err
	}

	transactionID := string(record.Key)

	switch cmdType {
	case CmdTypeRegister:
		var registerRecord RegisterRecord
		if err = json.Unmarshal(record.Value, &registerRecord); err != nil {
			return nil, fmt.Errorf("unmarshaling register record: %w", err)
		}
		return registerRecord.toRegisterCommand(transactionID), nil
	case CmdTypeUnregister:
		var unregisterRecord UnregisterRecord
		if err = json.Unmarshal(record.Value, &unregisterRecord); err != nil {
			return nil, fmt.Errorf("unmarshaling unregister record: %w", err)
		}
		return unregisterRecord.toUnregisterCommand(transactionID), nil
	default:
		return nil, fmt.Errorf("unsupported cmdType %s", cmdType)
	}
}

type RegisterRecord struct {
	PlayerID string    `json:"playerId"`
	Currency string    `json:"currency"`
	Value    int       `json:"value"`
	Time     time.Time `json:"time"`
}

func (r RegisterRecord) toRegisterCommand(transactionID string) tx.RegisterCommand {
	return tx.RegisterCommand{
		RegisterID: tx.RegisterID{
			ID: tx.ID{
				TransactionID: transactionID,
				PlayerID:      r.PlayerID,
				Currency:      r.Currency,
			},
			Time: r.Time,
		},
		Value: r.Value,
	}
}

type UnregisterRecord struct {
	RegisterRecord              RegisterRecord `json:"registerRecord"`
	RegisterRecordTransactionID string         `json:"registerRecordTransactionID"`
	Time                        time.Time      `json:"time"`
}

func (r UnregisterRecord) toUnregisterCommand(transactionID string) tx.UnregisterCommand {
	return tx.UnregisterCommand{
		ID:              transactionID,
		RegisterCommand: r.RegisterRecord.toRegisterCommand(r.RegisterRecordTransactionID),
		Time:            r.Time,
	}
}

func headerValue(record *kgo.Record, key string) (string, error) {
	for _, header := range record.Headers {
		if header.Key == key {
			return string(header.Value), nil
		}
	}
	return "", fmt.Errorf("record missing %s header", key)
}

const (
	CmdTypeHeader     = "cmd-type"
	CmdTypeRegister   = "register"
	CmdTypeUnregister = "unregister"
)
