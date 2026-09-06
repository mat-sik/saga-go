package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mat-sik/saga-go/examples/internal/adapters/sagaadapters"
	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"github.com/mat-sik/saga-go/examples/internal/kgoconsumer"
	"github.com/mat-sik/saga-go/saga"
	"github.com/twmb/franz-go/pkg/kgo"
)

func NewTxSagaConsumer(
	client kgoconsumer.Client,
	txRunner TxRunner,
	dlqTopic string,
	sagaConsumer saga.Consumer[sagaadapters.RegisterSagaCommand, sagaadapters.UnregisterSagaCommand],
	options ...kgoconsumer.Option,
) (kgoconsumer.Consumer, error) {
	consumer := kgoSagaConsumer{
		runTx:    txRunner,
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
	runTx    TxRunner
	consumer saga.Consumer[sagaadapters.RegisterSagaCommand, sagaadapters.UnregisterSagaCommand]
}

func (k kgoSagaConsumer) consumeRecord(ctx context.Context, record *kgo.Record) (err error) {
	cmd, err := mapToTxCommand(record)
	if err != nil {
		return err
	}

	consume := func(txCtx context.Context) error {
		if err = k.consumer.Consume(txCtx, cmd); err != nil {
			return fmt.Errorf("consuming tx command %v: %w", cmd, err)
		}
		return nil
	}

	return k.runTx(ctx, consume)
}

func mapToTxCommand(record *kgo.Record) (saga.Command[sagaadapters.RegisterSagaCommand, sagaadapters.UnregisterSagaCommand], error) {
	cmdType, err := headerValue(record, CmdTypeHeader)
	if err != nil {
		return nil, err
	}

	switch cmdType {
	case CmdTypeRegister:
		var registerRecord RegisterRecord
		if err = json.Unmarshal(record.Value, &registerRecord); err != nil {
			return nil, fmt.Errorf("unmarshaling register record: %w", err)
		}
		transactionID := string(record.Key)
		return registerRecord.toRegisterSagaCommand(transactionID), nil
	case CmdTypeUnregister:
		var unregisterRecord UnregisterRecord
		if err = json.Unmarshal(record.Value, &unregisterRecord); err != nil {
			return nil, fmt.Errorf("unmarshaling unregister record: %w", err)
		}
		registerRecordTransactionID := string(record.Key)
		return unregisterRecord.toUnregisterSagaCommand(registerRecordTransactionID), nil
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

func NewRegisterRecord(playerID, currency string, value int, time time.Time) RegisterRecord {
	return RegisterRecord{
		PlayerID: playerID,
		Currency: currency,
		Value:    value,
		Time:     time,
	}
}

func (r RegisterRecord) toRegisterCommand(transactionID string) tx.RegisterCommand {
	return tx.NewRegisterCommand(transactionID, r.PlayerID, r.Currency, r.Time, r.Value)
}

func (r RegisterRecord) toRegisterSagaCommand(transactionID string) sagaadapters.RegisterSagaCommand {
	return sagaadapters.RegisterSagaCommand{
		RegisterCommand: r.toRegisterCommand(transactionID),
	}
}

type UnregisterRecord struct {
	ID             string         `json:"id"`
	Time           time.Time      `json:"time"`
	RegisterRecord RegisterRecord `json:"registerRecord"`
}

func NewUnregisterRecord(id string, time time.Time, cmd tx.RegisterCommand) UnregisterRecord {
	registerID := cmd.RegisterID
	registerRecordID := registerID.ID

	return UnregisterRecord{
		ID:   id,
		Time: time,
		RegisterRecord: NewRegisterRecord(
			registerRecordID.PlayerID,
			registerRecordID.Currency,
			cmd.Value,
			registerID.Time,
		),
	}
}

func (r UnregisterRecord) toUnregisterSagaCommand(registerRecordTransactionID string) sagaadapters.UnregisterSagaCommand {
	registerSagaCommand := r.RegisterRecord.toRegisterSagaCommand(registerRecordTransactionID)
	return sagaadapters.UnregisterSagaCommand{
		UnregisterCommand: tx.NewUnregisterCommand(r.ID, r.Time, registerSagaCommand.RegisterCommand),
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
