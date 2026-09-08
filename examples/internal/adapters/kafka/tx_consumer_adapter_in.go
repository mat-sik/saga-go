package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"github.com/mat-sik/saga-go/examples/internal/idempotent"
	"github.com/mat-sik/saga-go/examples/internal/kgoconsumer"
	"github.com/twmb/franz-go/pkg/kgo"
)

func NewTxConsumer(
	client kgoconsumer.Client,
	runTx TxRunner,
	dlqTopic string,
	idempotentConsumer idempotent.Consumer[tx.RegisterCommand],
	options ...kgoconsumer.Option,
) (kgoconsumer.Consumer, error) {
	consumeRecord := func(ctx context.Context, record *kgo.Record) error {
		registerCommand, ok, err := mapToRegisterCommand(record)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}

		consume := func(ctx context.Context) error {
			if err := idempotentConsumer.Consume(ctx, registerCommand); err != nil {
				return fmt.Errorf("consuming register command %v: %w", registerCommand, err)
			}
			return nil
		}
		return runTx(ctx, consume)
	}

	return kgoconsumer.NewConsumer(
		client,
		dlqTopic,
		consumeRecord,
		options...,
	)
}

func mapToRegisterCommand(record *kgo.Record) (tx.RegisterCommand, bool, error) {
	cmdType, err := headerValue(record, CmdTypeHeader)
	if err != nil {
		return tx.RegisterCommand{}, false, err
	}

	switch cmdType {
	case CmdTypeRegister:
		var registerRecord RegisterRecord
		if err = json.Unmarshal(record.Value, &registerRecord); err != nil {
			return tx.RegisterCommand{}, false, fmt.Errorf("unmarshaling register record: %w", err)
		}
		transactionID := string(record.Key)
		return registerRecord.toRegisterCommand(transactionID), true, nil
	case CmdTypeUnregister:
		return tx.RegisterCommand{}, false, err
	default:
		return tx.RegisterCommand{}, false, fmt.Errorf("unsupported cmdType %s", cmdType)
	}
}
