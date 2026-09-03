package test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/mat-sik/saga-go/examples/internal/kgoconsumer"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TODO: test case when before cancel context is created, rebalance triggers and processing should be cancelled
func TestConsumption(t *testing.T) {
	tests := []struct {
		name                                    string
		ids                                     map[int32][]int
		shouldFailTransientlyTimes              map[int]int
		shouldFailPermanently                   map[int]struct{}
		expectedProcessedIDsByPartition         map[int32][]int
		expectedFailedIDsByPartition            map[int32][]int
		expectedFailedPermanentlyIDsByPartition map[int32][]int
	}{
		{
			name: "no failures",
			ids: map[int32][]int{
				0: {1, 2},
				1: {3},
				2: {4},
			},
			expectedProcessedIDsByPartition: map[int32][]int{
				0: {1, 2},
				1: {3},
				2: {4},
			},
			expectedFailedIDsByPartition:            make(map[int32][]int),
			expectedFailedPermanentlyIDsByPartition: make(map[int32][]int),
		},
		{
			name: "transient failures",
			ids: map[int32][]int{
				0: {1, 2},
				1: {3},
				2: {4},
			},
			shouldFailTransientlyTimes: map[int]int{
				2: 3,
				4: 2,
			},
			expectedProcessedIDsByPartition: map[int32][]int{
				0: {1, 2},
				1: {3},
				2: {4},
			},
			expectedFailedIDsByPartition: map[int32][]int{
				0: {2, 2, 2},
				2: {4, 4},
			},
			expectedFailedPermanentlyIDsByPartition: make(map[int32][]int),
		},
		{
			name: "permanent failures",
			ids: map[int32][]int{
				0: {1, 2},
				1: {3},
				2: {4},
			},
			shouldFailPermanently: map[int]struct{}{
				1: {},
				2: {},
				3: {},
				4: {},
			},
			expectedProcessedIDsByPartition: make(map[int32][]int),
			expectedFailedIDsByPartition:    make(map[int32][]int),
			expectedFailedPermanentlyIDsByPartition: map[int32][]int{
				0: {1, 2},
				1: {3},
				2: {4},
			},
		},
		{
			name: "mixed failures",
			ids: map[int32][]int{
				0: {1, 2},
				1: {3},
				2: {4},
			},
			shouldFailTransientlyTimes: map[int]int{
				2: 3,
				4: 2,
			},
			shouldFailPermanently: map[int]struct{}{
				2: {},
				4: {},
			},
			expectedProcessedIDsByPartition: map[int32][]int{
				0: {1},
				1: {3},
			},
			expectedFailedIDsByPartition: map[int32][]int{
				0: {2, 2, 2},
				2: {4, 4},
			},
			expectedFailedPermanentlyIDsByPartition: map[int32][]int{
				0: {2},
				2: {4},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic := newTopicName(t, "commands")
			createTopic(t, topic, len(tt.ids))

			topicDLQ := newTopicName(t, "commands-dlq")
			createTopic(t, topicDLQ, 1)

			prod := newProducer(t)
			produceCommands(t, prod, topic, tt.ids)

			var wgStub sync.WaitGroup

			stub := newStubConsumer(&wgStub, tt.shouldFailTransientlyTimes, tt.shouldFailPermanently, tt.ids)
			testedConsumer := newTestedConsumer(t, topic, topicDLQ, stub)

			consumerDoneCh := make(chan error)
			consumerCtx, cancelConsumer := context.WithCancel(t.Context())

			go func() {
				if err := testedConsumer.StartPolling(consumerCtx); err != nil {
					consumerDoneCh <- fmt.Errorf("polling: %w", err)
					return
				}
				consumerDoneCh <- nil
			}()

			wgStub.Wait()

			if len(tt.expectedFailedPermanentlyIDsByPartition) > 0 {
				assertDLQCommandsEqual(t, topicDLQ, tt.expectedFailedPermanentlyIDsByPartition)
			}

			cancelConsumer()

			if err := <-consumerDoneCh; err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("consumer polling: %v", err)
			}

			if !reflect.DeepEqual(stub.processedIDsByPartition, tt.expectedProcessedIDsByPartition) {
				t.Fatalf(
					"processed IDs mismatch: got %v, want %v",
					stub.processedIDsByPartition,
					tt.ids,
				)
			}

			if !reflect.DeepEqual(stub.failedIDsByPartition, tt.expectedFailedIDsByPartition) {
				t.Fatalf(
					"failed transiently IDs mismatch: got %v, want %v",
					stub.failedIDsByPartition,
					tt.expectedFailedIDsByPartition,
				)
			}

			if !reflect.DeepEqual(stub.failedPermanentlyIDsByPartition, tt.expectedFailedPermanentlyIDsByPartition) {
				t.Fatalf(
					"failed permanently IDs mismatch: got %v, want %v",
					stub.failedPermanentlyIDsByPartition,
					tt.expectedFailedPermanentlyIDsByPartition,
				)
			}
		})
	}
}

func assertDLQCommandsEqual(tb testing.TB, topicDLQ string, expectedFailedPermanentlyIDsByPartition map[int32][]int) {
	tb.Helper()

	dlqCommands := consumeDLQTopic(tb, topicDLQ)
	expectedDLQCommands := newDLQCommands(expectedFailedPermanentlyIDsByPartition)

	sortDLQCommands(dlqCommands)
	sortDLQCommands(expectedDLQCommands)

	if !slices.Equal(dlqCommands, expectedDLQCommands) {
		tb.Fatalf(
			"dlq commands mismatch: got %v, want %v",
			dlqCommands,
			expectedDLQCommands,
		)
	}
}

func newTestedConsumer(t *testing.T, topic string, topicDLQ string, stub stubConsumer) kgoconsumer.Consumer {
	client := newKgoConsumerClient(t, "commands", []string{topic})

	cons, err := kgoconsumer.NewConsumer(client, topicDLQ, stub.consumeRecord, kgoconsumer.WithBackoffMax(100*time.Microsecond))
	if err != nil {
		t.Fatalf("creating new kgoconsumer consumer: %v", err)
	}
	t.Cleanup(cons.Close)

	return cons
}

func newKgoConsumerClient(tb testing.TB, consumerGroup string, topics []string) kgoconsumer.Client {
	consumerGroup = newConsumerGroupName(tb, consumerGroup)

	client, err := kgoconsumer.NewClient(testKafkaBrokers, consumerGroup, topics)
	if err != nil {
		tb.Fatalf("creating new kgoconsumer client: %v", err)
	}
	return client
}

type stubConsumer struct {
	wg                              *sync.WaitGroup
	shouldFailTransientlyTimes      map[int]int
	shouldFailPermanently           map[int]struct{}
	failedIDsByPartition            map[int32][]int
	failedPermanentlyIDsByPartition map[int32][]int
	processedIDsByPartition         map[int32][]int
}

func newStubConsumer(
	wg *sync.WaitGroup,
	shouldFailTransientlyTimes map[int]int,
	shouldFailPermanently map[int]struct{},
	toProcess map[int32][]int,
) stubConsumer {
	failTimes := calculateFailTimes(shouldFailTransientlyTimes)
	toProcessAmount := lenValues(toProcess)

	wg.Add(failTimes + toProcessAmount)

	return stubConsumer{
		wg:                              wg,
		shouldFailTransientlyTimes:      shouldFailTransientlyTimes,
		shouldFailPermanently:           shouldFailPermanently,
		failedIDsByPartition:            make(map[int32][]int),
		failedPermanentlyIDsByPartition: make(map[int32][]int),
		processedIDsByPartition:         make(map[int32][]int),
	}
}

func calculateFailTimes(shouldFail map[int]int) int {
	failTimes := 0
	for _, count := range shouldFail {
		failTimes += count
	}
	return failTimes
}

func (c *stubConsumer) consumeRecord(_ context.Context, record *kgo.Record) error {
	defer func() {
		c.wg.Done()
	}()

	cmd := newCommand(record)

	if failTimes, ok := c.shouldFailTransientlyTimes[cmd.id]; ok && failTimes > 0 {
		ids := c.failedIDsByPartition[record.Partition]
		c.failedIDsByPartition[record.Partition] = append(ids, cmd.id)

		c.shouldFailTransientlyTimes[cmd.id] = failTimes - 1

		return errors.Join(errTransient, kgoconsumer.ErrTransient)
	}

	if _, ok := c.shouldFailPermanently[cmd.id]; ok {
		ids := c.failedPermanentlyIDsByPartition[record.Partition]
		c.failedPermanentlyIDsByPartition[record.Partition] = append(ids, cmd.id)
		return errPermanent
	}

	ids := c.processedIDsByPartition[record.Partition]
	c.processedIDsByPartition[record.Partition] = append(ids, cmd.id)
	return nil
}

var (
	errTransient = errors.New("transient failure")
	errPermanent = errors.New("permanent failure")
)

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

func consumeDLQTopic(tb testing.TB, dlqTopic string) []dlqCommand {
	cons := newConsumer(tb, []string{dlqTopic}, newConsumerGroupName(tb, "dlq-test-consumer"))

	records := cons.consumeRecords(tb)

	commands := make([]dlqCommand, len(records))
	for i, r := range records {
		commands[i] = dlqCommand{
			cmd:   newCommand(r),
			cause: dlqReason(tb, r),
		}
	}

	return commands
}

func dlqReason(tb testing.TB, record *kgo.Record) string {
	for _, h := range record.Headers {
		if h.Key == "dlq-reason" {
			return string(h.Value)
		}
	}
	tb.Fatalf("dlq-reason not encoded in the header of record %v", record)
	return ""
}

type dlqCommand struct {
	cmd   command
	cause string
}

func newDLQCommands(expectedFailedPermanentlyIDsByPartition map[int32][]int) []dlqCommand {
	var commands []dlqCommand
	for _, ids := range expectedFailedPermanentlyIDsByPartition {
		for _, id := range ids {
			commands = append(commands, newDLQCommand(id))
		}
	}
	return commands
}

func newDLQCommand(id int) dlqCommand {
	return dlqCommand{
		cmd:   command{id: id},
		cause: errPermanent.Error(),
	}
}

func sortDLQCommands(commands []dlqCommand) {
	slices.SortFunc(commands, func(a, b dlqCommand) int {
		return a.cmd.id - b.cmd.id
	})
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
