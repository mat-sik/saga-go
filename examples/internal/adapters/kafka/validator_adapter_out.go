package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"github.com/twmb/franz-go/pkg/kgo"
)

type RandomValidator struct {
	client            *kgo.Client
	topic             string
	compensatePercent int
}

func NewRandomValidator(client *kgo.Client, topic string, compensatePercent int) RandomValidator {
	return RandomValidator{
		client:            client,
		topic:             topic,
		compensatePercent: compensatePercent,
	}
}

func (r RandomValidator) Validate(_ context.Context, _ tx.RegisterCommand) (bool, error) {
	roll := rand.N(100) + 1
	shouldCompensate := roll <= r.compensatePercent
	return !shouldCompensate, nil
}

func (r RandomValidator) Compensate(ctx context.Context, registerCommand tx.RegisterCommand) error {
	transactionID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generating UUIDv7: %w", err)
	}

	record, err := r.newRecord(transactionID.String(), mapToUnregisterRecord(registerCommand))
	if err != nil {
		return err
	}

	return r.client.ProduceSync(ctx, record).FirstErr()
}

func mapToUnregisterRecord(registerCommand tx.RegisterCommand) UnregisterRecord {
	registerID := registerCommand.RegisterID
	id := registerID.ID

	return UnregisterRecord{
		RegisterRecord: RegisterRecord{
			PlayerID: id.PlayerID,
			Currency: id.Currency,
			Value:    registerCommand.Value,
			Time:     registerID.Time,
		},
		RegisterRecordTransactionID: id.TransactionID,
		Time:                        time.Now(),
	}
}

func (r RandomValidator) newRecord(transactionID string, unregisterRecord UnregisterRecord) (*kgo.Record, error) {
	body, err := json.Marshal(unregisterRecord)
	if err != nil {
		return nil, fmt.Errorf("marshaling %v: %w", unregisterRecord, err)
	}

	key := transactionID
	return &kgo.Record{
		Key:   []byte(key),
		Value: body,
		Topic: r.topic,
		Headers: []kgo.RecordHeader{
			{
				Key:   CmdTypeHeader,
				Value: []byte(CmdTypeUnregister),
			},
		},
	}, nil
}
