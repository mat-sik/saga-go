package test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/mat-sik/saga-go/examples/internal/kgoconsumer"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TODO: test case when before cancel context is created, rebalance triggers and processing should be cancelled
func TestBasicConsume(t *testing.T) {
	topic := newTopicName(t, "basic-consume")
	createTopic(t, topic, 1)

	topicDLQ := newTopicName(t, "basic-consume-dlq")
	createTopic(t, topicDLQ, 1)

	ids := []int{1, 2, 3, 4, 5}
	prod := newProducer(t)
	for _, id := range ids {
		produceCommand(t, prod, topic, id)
	}

	var wg sync.WaitGroup

	stubConsumer := newTransientFailureRecordConsumer(&wg, 0, len(ids))
	client := newKgoConsumerClient(t, "basic-consume", []string{topic})

	cons, err := kgoconsumer.NewConsumer(client, topicDLQ, stubConsumer.consumeRecord)
	if err != nil {
		t.Fatalf("creating new kgoconsumer consumer: %v", err)
	}

	consumerErrCh := make(chan error)
	consumerCtx, cancelConsuming := context.WithCancel(t.Context())

	go func() {
		defer func() {
			cons.Close()
		}()

		if err := cons.StartPolling(consumerCtx); err != nil {
			consumerErrCh <- fmt.Errorf("polling: %w", err)
			return
		}
		consumerErrCh <- nil
	}()

	wg.Wait()
	cancelConsuming()
	if err := <-consumerErrCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("consumer polling: %v", err)
	}

	if !slices.Equal(stubConsumer.processedIDs, ids) {
		t.Fatalf("got: %v want: %v", stubConsumer.processedIDs, ids)
	}
}

func newKgoConsumerClient(tb testing.TB, consumerGroup string, topics []string) kgoconsumer.Client {
	consumerGroup = newConsumerGroupName(tb, consumerGroup)

	client, err := kgoconsumer.NewClient(testKafkaBrokers, consumerGroup, topics)
	if err != nil {
		tb.Fatalf("creating new kgoconsumer client: %v", err)
	}
	return client
}

type transientFailureRecordConsumer struct {
	wg           *sync.WaitGroup
	failTimes    int
	failedIDs    []int
	processedIDs []int
}

func newTransientFailureRecordConsumer(wg *sync.WaitGroup, failTimes, toProcess int) transientFailureRecordConsumer {
	wg.Add(failTimes + toProcess)
	return transientFailureRecordConsumer{
		wg:        wg,
		failTimes: failTimes,
	}
}

func (c *transientFailureRecordConsumer) consumeRecord(_ context.Context, record *kgo.Record) error {
	defer func() {
		c.wg.Done()
	}()

	cmd := newCommand(record)

	if c.failTimes > len(c.failedIDs) {
		c.failedIDs = append(c.failedIDs, cmd.id)
		return errors.Join(errors.New("synthetic failure"), kgoconsumer.ErrTransient)
	}

	c.processedIDs = append(c.processedIDs, cmd.id)
	return nil
}

func produceCommand(tb testing.TB, producer producer, topic string, id int) {
	cmd := command{
		id: id,
	}
	producer.produce(tb, cmd.toRecord(topic))
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
