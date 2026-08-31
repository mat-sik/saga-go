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
	id, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generating compensate transaction UUIDv7: %w", err)
	}

	unregisterRecord := NewUnregisterRecord(id.String(), time.Now(), registerCommand)
	registerRecordTransactionID := registerCommand.RegisterID.ID.TransactionID
	record, err := r.newRecord(registerRecordTransactionID, unregisterRecord)
	if err != nil {
		return err
	}

	return r.client.ProduceSync(ctx, record).FirstErr()
}

func (r RandomValidator) newRecord(
	registerRecordTransactionID string,
	unregisterRecord UnregisterRecord,
) (*kgo.Record, error) {
	body, err := json.Marshal(unregisterRecord)
	if err != nil {
		return nil, fmt.Errorf("marshaling %v: %w", unregisterRecord, err)
	}

	key := registerRecordTransactionID
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
