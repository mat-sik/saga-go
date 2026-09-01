package test

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/mat-sik/saga-go/examples/internal/kgoconsumer"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TODO: test case when before cancel context is created, rebalance triggers and processing should be cancelled

type transientFailureRecordConsumer struct {
	failuresRemaining int
	failedIDs         []int
	processedIDs      []int
}

func (c transientFailureRecordConsumer) consumeRecord(_ context.Context, record *kgo.Record) error {
	cmd := newCommand(record)

	if c.failuresRemaining > 0 {
		c.failedIDs = append(c.failedIDs, cmd.id)
		return errors.Join(errors.New("synthetic failure"), kgoconsumer.ErrTransient)
	}

	c.processedIDs = append(c.processedIDs, cmd.id)
	return nil
}

func produceCommand(ctx context.Context, producer producer, topic string, id int) error {
	cmd := command{
		id: id,
	}
	return producer.produceSync(ctx, cmd.toRecord(topic))
}

type command struct {
	id int
}

func newCommand(record *kgo.Record) command {
	encodedID := record.Key
	id := byteToInt(encodedID)
	return command{
		id: id,
	}
}

func (c command) toRecord(topic string) *kgo.Record {
	encodedID := intToByte(c.id)
	return &kgo.Record{
		Key:   encodedID[:],
		Value: encodedID[:],
		Topic: topic,
	}
}

func byteToInt(encoded []byte) int {
	return int(int64(binary.BigEndian.Uint64(encoded)))
}

func intToByte(n int) [8]byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(n))
	return encoded
}
