package test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/mat-sik/saga-go/examples/internal/kgoconsumer"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TODO: test case when before cancel context is created, rebalance triggers and processing should be cancelled
func TestBasicConsume(t *testing.T) {
	topic := newTopicName(t, "commands")
	createTopic(t, topic, 3)

	topicDLQ := newTopicName(t, "commands-dlq")
	createTopic(t, topicDLQ, 1)

	ids := map[int32][]int{
		0: {1, 2},
		1: {3},
		2: {4},
	}
	prod := newProducer(t)
	produceCommands(t, prod, topic, ids)

	var wg sync.WaitGroup

	stubConsumer := newTransientFailureRecordConsumer(&wg, 0, lenValues(ids))
	client := newKgoConsumerClient(t, "commands", []string{topic})

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

	if !reflect.DeepEqual(stubConsumer.processedIDsByPartition, ids) {
		t.Fatalf(
			"processed IDs mismatch: got %v, want %v",
			stubConsumer.processedIDsByPartition,
			ids,
		)
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
	wg                      *sync.WaitGroup
	failTimes               int
	failedTimes             int
	failedIDsByPartition    map[int32][]int
	processedIDsByPartition map[int32][]int
}

func newTransientFailureRecordConsumer(wg *sync.WaitGroup, failTimes, toProcess int) transientFailureRecordConsumer {
	wg.Add(failTimes + toProcess)
	return transientFailureRecordConsumer{
		wg:                      wg,
		failTimes:               failTimes,
		failedIDsByPartition:    make(map[int32][]int),
		processedIDsByPartition: make(map[int32][]int),
	}
}

func (c *transientFailureRecordConsumer) consumeRecord(_ context.Context, record *kgo.Record) error {
	defer func() {
		c.wg.Done()
	}()

	cmd := newCommand(record)

	if c.failTimes > c.failedTimes {
		ids := c.failedIDsByPartition[record.Partition]
		c.failedIDsByPartition[record.Partition] = append(ids, cmd.id)

		c.failedTimes++

		return errors.Join(errors.New("synthetic failure"), kgoconsumer.ErrTransient)
	}

	ids := c.processedIDsByPartition[record.Partition]
	c.processedIDsByPartition[record.Partition] = append(ids, cmd.id)
	return nil
}

func produceCommands(tb testing.TB, producer producer, topic string, idByPartition map[int32][]int) {
	records := make([]*kgo.Record, 0)
	for partition, ids := range idByPartition {
		for _, id := range ids {
			cmd := command{
				id: id,
			}
			records = append(records, cmd.toRecord(topic, partition))
		}
	}
	producer.produce(tb, records...)
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

func (c command) toRecord(topic string, partition int32) *kgo.Record {
	encodedID := intToByte(c.id)
	return &kgo.Record{
		Key:       encodedID[:],
		Value:     encodedID[:],
		Topic:     topic,
		Partition: partition,
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

func lenValues[K comparable, V any](data map[K][]V) int {
	size := 0
	for _, v := range data {
		size += len(v)
	}
	return size
}
