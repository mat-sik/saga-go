package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"github.com/mat-sik/saga-go/examples/internal/kgoconsumer"
	"github.com/mat-sik/saga-go/examples/internal/txctx"
	"github.com/mat-sik/saga-go/saga"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TODO: add transient and pernament errors in the adapters and domain logic
func NewSagaConsumer(
	client kgoconsumer.Client,
	dlqTopic string,
	pool *pgxpool.Pool,
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
	command, err := mapToCommand(record)
	if err != nil {
		return err
	}

	pgxTx, err := k.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning tx: %w", err)
	}
	defer func() {
		if err != nil {
			if rollbackErr := pgxTx.Rollback(ctx); rollbackErr != nil {
				err = errors.Join(err, fmt.Errorf("rolling back: %w", rollbackErr))
			}
		}
	}()

	ctx = txctx.WithTx(ctx, pgxTx)
	if err = k.consumer.Consume(ctx, command); err != nil {
		return fmt.Errorf("consuming command %v: %w", command, err)
	}
	return nil
}

func mapToCommand(record *kgo.Record) (saga.Command[tx.RegisterCommand, tx.UnregisterCommand], error) {
	cmdType, err := headerValue(record, cmdTypeHeader)
	if err != nil {
		return nil, err
	}

	transactionID := string(record.Key)

	switch cmdType {
	case cmdTypeRegister:
		var registerRecord RegisterRecord
		if err = json.Unmarshal(record.Value, &registerRecord); err != nil {
			return nil, fmt.Errorf("unmarshaling register record: %w", err)
		}
		return registerRecord.toRegisterCommand(transactionID), nil
	case cmdTypeUnregister:
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
	cmdTypeHeader     = "cmd-type"
	cmdTypeRegister   = "register"
	cmdTypeUnregister = "unregister"
)
