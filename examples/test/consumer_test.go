package test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/mat-sik/saga-go/examples/internal/kgoconsumer"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TODO: test case when before cancel context is created, rebalance triggers and processing should be cancelled
func TestConsumption(t *testing.T) {
	tests := []struct {
		name                         string
		ids                          map[int32][]int
		shouldFail                   map[int]int
		expectedFailedIDsByPartition map[int32][]int
		topicDLQPartitions           int
	}{
		{
			name: "basic",
			ids: map[int32][]int{
				0: {1, 2},
				1: {3},
				2: {4},
			},
			expectedFailedIDsByPartition: make(map[int32][]int),
			topicDLQPartitions:           1,
		},
		{
			name: "transient failures",
			ids: map[int32][]int{
				0: {1, 2},
				1: {3},
				2: {4},
			},
			shouldFail: map[int]int{
				2: 3,
				4: 2,
			},
			expectedFailedIDsByPartition: map[int32][]int{
				0: {2, 2, 2},
				2: {4, 4},
			},
			topicDLQPartitions: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic := newTopicName(t, "commands")
			createTopic(t, topic, len(tt.ids))

			topicDLQ := newTopicName(t, "commands-dlq")
			createTopic(t, topicDLQ, tt.topicDLQPartitions)

			prod := newProducer(t)
			produceCommands(t, prod, topic, tt.ids)

			var wg sync.WaitGroup

			stubConsumer := newTransientFailureRecordConsumer(&wg, tt.shouldFail, tt.ids)
			client := newKgoConsumerClient(t, "commands", []string{topic})

			cons, err := kgoconsumer.NewConsumer(client, topicDLQ, stubConsumer.consumeRecord, kgoconsumer.WithBackoffMax(100*time.Microsecond))
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

			if !reflect.DeepEqual(stubConsumer.processedIDsByPartition, tt.ids) {
				t.Fatalf(
					"processed IDs mismatch: got %v, want %v",
					stubConsumer.processedIDsByPartition,
					tt.ids,
				)
			}

			if !reflect.DeepEqual(stubConsumer.failedIDsByPartition, tt.expectedFailedIDsByPartition) {
				t.Fatalf(
					"failed IDs mismatch: got %v, want %v",
					stubConsumer.failedIDsByPartition,
					tt.expectedFailedIDsByPartition,
				)
			}
		})
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
	shouldFail              map[int]int
	failedIDsByPartition    map[int32][]int
	processedIDsByPartition map[int32][]int
}

func newTransientFailureRecordConsumer(
	wg *sync.WaitGroup,
	shouldFail map[int]int,
	toProcess map[int32][]int,
) transientFailureRecordConsumer {
	failTimes := calculateFailTimes(shouldFail)
	toProcessAmount := lenValues(toProcess)

	wg.Add(failTimes + toProcessAmount)

	return transientFailureRecordConsumer{
		wg:                      wg,
		shouldFail:              shouldFail,
		failedIDsByPartition:    make(map[int32][]int),
		processedIDsByPartition: make(map[int32][]int),
	}
}

func calculateFailTimes(shouldFail map[int]int) int {
	failTimes := 0
	for _, count := range shouldFail {
		failTimes += count
	}
	return failTimes
}

func (c *transientFailureRecordConsumer) consumeRecord(_ context.Context, record *kgo.Record) error {
	defer func() {
		c.wg.Done()
	}()

	cmd := newCommand(record)

	if failTimes, ok := c.shouldFail[cmd.id]; ok && failTimes > 0 {
		ids := c.failedIDsByPartition[record.Partition]
		c.failedIDsByPartition[record.Partition] = append(ids, cmd.id)

		c.shouldFail[cmd.id] = failTimes - 1

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
