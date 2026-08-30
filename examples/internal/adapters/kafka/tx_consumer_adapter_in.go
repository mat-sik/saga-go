package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/saga-go/examples/internal/adapters/postgres"
	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"github.com/mat-sik/saga-go/examples/internal/kgoconsumer"
	"github.com/mat-sik/saga-go/saga"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TODO: add transient and pernament errors in the adapters and domain logic
func NewTxSagaConsumer(
	client kgoconsumer.Client,
	pool *pgxpool.Pool,
	dlqTopic string,
	sagaConsumer saga.Consumer[tx.RegisterCommand, tx.UnregisterCommand],
	options ...kgoconsumer.Option,
) (kgoconsumer.Consumer, error) {
	consumer := kgoSagaConsumer{
		pool:     pool,
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
	pool     *pgxpool.Pool
	consumer saga.Consumer[tx.RegisterCommand, tx.UnregisterCommand]
}

func (k kgoSagaConsumer) consumeRecord(ctx context.Context, record *kgo.Record) (err error) {
	command, err := mapToTxCommand(record)
	if err != nil {
		return err
	}

	consume := func(txCtx context.Context) error {
		if err = k.consumer.Consume(txCtx, command); err != nil {
			return fmt.Errorf("consuming command %v: %w", command, err)
		}
		return nil
	}

	return postgres.WithTx(ctx, k.pool, consume)
}

func mapToTxCommand(record *kgo.Record) (saga.Command[tx.RegisterCommand, tx.UnregisterCommand], error) {
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

type UnregisterRecord struct {
	RegisterRecordTransactionID string         `json:"registerRecordTransactionID"`
	Time                        time.Time      `json:"time"`
	RegisterRecord              RegisterRecord `json:"registerRecord"`
}

func NewUnregisterRecord(cmd tx.RegisterCommand, time time.Time) UnregisterRecord {
	registerID := cmd.RegisterID
	id := registerID.ID

	return UnregisterRecord{
		RegisterRecordTransactionID: id.TransactionID,
		Time:                        time,
		RegisterRecord:              NewRegisterRecord(id.PlayerID, id.Currency, cmd.Value, registerID.Time),
	}
}

func (r UnregisterRecord) toUnregisterCommand(transactionID string) tx.UnregisterCommand {
	registerCommand := r.RegisterRecord.toRegisterCommand(r.RegisterRecordTransactionID)
	return tx.NewUnregisterCommand(transactionID, r.Time, registerCommand)
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
