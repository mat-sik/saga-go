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

	"github.com/mat-sik/saga-go/kgoconsumer"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRebalance_CancelledBatchIsRefetchedWithoutRevoke(t *testing.T) {
	t.Parallel()

	ids := map[int32][]int{0: {1, 2}}

	topic := newTopicName(t, "commands")
	createTopic(t, topic, 1)
	topicDLQ := newTopicName(t, "commands-dlq")
	createTopic(t, topicDLQ, 1)

	prod := newProducer(t)
	produceCommands(t, prod, topic, ids)

	consumerGroup := newConsumerGroupName(t, "commands")
	client := newKgoConsumerClient(t, consumerGroup, []string{topic})

	var wg sync.WaitGroup
	stub := newCancelledBatchStubConsumer(&wg, ids)

	testedConsumer := newTestedConsumer(t, client, topicDLQ, stub.consumeRecord,
		kgoconsumer.WithProcessingTimeout(100*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error)
	go func() {
		done <- testedConsumer.StartPolling(ctx)
	}()

	wg.Wait()
	cancel()

	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("consumer polling: %v", err)
	}

	if !reflect.DeepEqual(stub.processedIDsByPartition, ids) {
		t.Fatalf(
			"processed IDs mismatch: got %v, want %v",
			stub.processedIDsByPartition,
			ids,
		)
	}
}

type cancelledBatchStubConsumer struct {
	wg                      *sync.WaitGroup
	processedIDsByPartition map[int32][]int
	firstCall               bool
}

func newCancelledBatchStubConsumer(wg *sync.WaitGroup, toProcess map[int32][]int) *cancelledBatchStubConsumer {
	wg.Add(lenValues(toProcess))
	return &cancelledBatchStubConsumer{
		wg:                      wg,
		processedIDsByPartition: make(map[int32][]int),
		firstCall:               true,
	}
}

func (c *cancelledBatchStubConsumer) consumeRecord(ctx context.Context, record *kgo.Record) error {
	if c.firstCall {
		c.firstCall = false
		<-ctx.Done()
		return fmt.Errorf("record consuming: %w", ctx.Err())
	}

	cmd := newCommand(record)
	ids := c.processedIDsByPartition[record.Partition]
	c.processedIDsByPartition[record.Partition] = append(ids, cmd.id)
	c.wg.Done()

	return nil
}

func TestRebalance(t *testing.T) {
	t.Parallel()

	ids := map[int32][]int{
		0: {1, 2},
		1: {3, 4, 5},
		2: {6, 7, 8, 9},
		3: {10},
	}

	topic := newTopicName(t, "commands")
	createTopic(t, topic, len(ids))

	topicDLQ := newTopicName(t, "commands-dlq")
	createTopic(t, topicDLQ, 1)

	prod := newProducer(t)
	produceCommands(t, prod, topic, ids)

	consumerGroup := newConsumerGroupName(t, "commands")

	rebalanceStarted := make(chan struct{}, 1)
	onRebalanceBlocked := kgoconsumer.WithOnRebalanceBlocked(func() {
		select {
		case rebalanceStarted <- struct{}{}:
		default:
		}
	})
	client := newKgoConsumerClient(t, consumerGroup, []string{topic}, onRebalanceBlocked)

	var wgStub sync.WaitGroup
	consumptionStarted := make(chan struct{})
	stub := newRebalanceAwaitingStubConsumer(&wgStub, consumptionStarted, rebalanceStarted, ids)

	testedConsumer := newTestedConsumer(t, client, topicDLQ, stub.consumeRecord, kgoconsumer.WithBackoffMax(100*time.Microsecond))

	consumerDoneCh := make(chan error)
	consumerCtx, cancelConsumer := context.WithCancel(t.Context())

	go func() {
		if err := testedConsumer.StartPolling(consumerCtx); err != nil {
			consumerDoneCh <- fmt.Errorf("polling: %w", err)
			return
		}
		consumerDoneCh <- nil
	}()

	<-consumptionStarted

	rebalanceInvokingConsumer := newConsumer(t, []string{topic}, consumerGroup)
	rebalanceInvokingConsumer.joinConsumerGroup(t)

	rebalanceInvokingConsumer.leaveConsumerGroup(t)

	wgStub.Wait()
	cancelConsumer()

	if err := <-consumerDoneCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("consumer polling: %v", err)
	}

	if !reflect.DeepEqual(stub.processedIDsByPartition, ids) {
		t.Fatalf(
			"processed IDs mismatch: got %v, want %v",
			stub.processedIDsByPartition,
			ids,
		)
	}
}

type rebalanceAwaitingStubConsumer struct {
	wg                      *sync.WaitGroup
	consumptionStarted      chan struct{}
	rebalanceStarted        chan struct{}
	processedIDsByPartition map[int32][]int
	firstConsumption        bool
}

func newRebalanceAwaitingStubConsumer(
	wg *sync.WaitGroup,
	consumptionStarted, rebalanceStarted chan struct{},
	toProcess map[int32][]int,
) *rebalanceAwaitingStubConsumer {
	toProcessAmount := lenValues(toProcess)
	wg.Add(toProcessAmount)

	return &rebalanceAwaitingStubConsumer{
		wg:                      wg,
		consumptionStarted:      consumptionStarted,
		rebalanceStarted:        rebalanceStarted,
		processedIDsByPartition: make(map[int32][]int),
		firstConsumption:        true,
	}
}

func (c *rebalanceAwaitingStubConsumer) consumeRecord(ctx context.Context, record *kgo.Record) error {
	if c.firstConsumption {
		close(c.consumptionStarted)

		<-c.rebalanceStarted

		c.firstConsumption = false
	}

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("record consuming: %w", err)
	}

	cmd := newCommand(record)

	ids := c.processedIDsByPartition[record.Partition]
	c.processedIDsByPartition[record.Partition] = append(ids, cmd.id)

	c.wg.Done()

	return nil
}

func TestConsumption(t *testing.T) {
	t.Parallel()

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
			t.Parallel()

			topic := newTopicName(t, "commands")
			createTopic(t, topic, len(tt.ids))

			topicDLQ := newTopicName(t, "commands-dlq")
			createTopic(t, topicDLQ, 1)

			prod := newProducer(t)
			produceCommands(t, prod, topic, tt.ids)

			consumerGroup := newConsumerGroupName(t, "commands")
			client := newKgoConsumerClient(t, consumerGroup, []string{topic})

			var wgStub sync.WaitGroup
			stub := newStubConsumer(&wgStub, tt.shouldFailTransientlyTimes, tt.shouldFailPermanently, tt.ids)

			testedConsumer := newTestedConsumer(t, client, topicDLQ, stub.consumeRecord, kgoconsumer.WithBackoffMax(100*time.Microsecond))

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

func newTestedConsumer(
	t *testing.T,
	client kgoconsumer.Client,
	topicDLQ string,
	recordConsumer kgoconsumer.RecordConsumer,
	opts ...kgoconsumer.Option,
) kgoconsumer.Consumer {
	cons, err := kgoconsumer.NewConsumer(client, topicDLQ, recordConsumer, opts...)
	if err != nil {
		t.Fatalf("creating new kgoconsumer consumer: %v", err)
	}
	t.Cleanup(cons.Close)

	return cons
}

func newKgoConsumerClient(tb testing.TB, consumerGroup string, topics []string, opts ...kgoconsumer.ClientOption) kgoconsumer.Client {
	clientOpts := []kgoconsumer.ClientOption{
		kgoconsumer.WithFetchMaxBytes(1),
		kgoconsumer.WithFetchMaxPartitionBytes(1),
	}

	clientOpts = append(clientOpts, opts...)

	client, err := kgoconsumer.NewClient(
		testKafkaBrokers,
		consumerGroup,
		topics,
		clientOpts...,
	)
	if err != nil {
		tb.Fatalf("creating new kgoconsumer client: %v", err)
	}
	return client
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
) *stubConsumer {
	failTimes := calculateFailTimes(shouldFailTransientlyTimes)
	toProcessAmount := lenValues(toProcess)

	wg.Add(failTimes + toProcessAmount)

	return &stubConsumer{
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
