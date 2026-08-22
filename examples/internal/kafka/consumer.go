package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Consumer struct {
	client           *kgo.Client
	dlqProducer      *dlqProducer
	cancelProcessing *cancelProcessingStore
	recordConsumer   RecordConsumer
	config           config
}

func NewConsumer(
	seeds []string,
	consumerGroup string,
	topic string,
	dlqTopic string,
	recordConsumer RecordConsumer,
	options ...Option,
) (Consumer, error) {
	cancelProcessing := newCancelProcessingStore()

	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsCallbackBlocked(func(ctx context.Context, client *kgo.Client) {
			cancelProcessing.cancel()
		}),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return Consumer{}, fmt.Errorf("creating new franz-go client: %w", err)
	}

	return Consumer{
		client:           client,
		dlqProducer:      newDlqProducer(client, dlqTopic),
		cancelProcessing: cancelProcessing,
		recordConsumer:   recordConsumer,
		config:           newConfig(options...),
	}, nil
}

func (c Consumer) StartPolling(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("polling: %w", err)
		}
		if err := c.pollFetches(ctx); err != nil {
			return err
		}
	}
}

func (c Consumer) pollFetches(ctx context.Context) error {
	defer c.client.AllowRebalance()

	fetches := c.client.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return nil
	}

	consumer := batchConsumer{
		recordConsumer:               c.recordConsumer,
		dlqProducer:                  c.dlqProducer,
		processedEpochOffsetsTracker: newProcessedOffsetsTracker(),
		backoff:                      c.newBackoff(),
	}

	consumerCtx, cancel := context.WithTimeout(ctx, c.config.processingConfig.timeout)
	c.cancelProcessing.store(cancel)
	defer c.cancelProcessing.cancel()

	if err := consumer.consumeFetches(consumerCtx, fetches); err != nil {
		return err
	}

	committable := consumer.processedEpochOffsetsTracker.committableEpochOffsets()

	var err error
	c.client.CommitOffsetsSync(ctx, committable, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, onDoneErr error) {
		err = onDoneErr
	})
	if err != nil {
		return fmt.Errorf("commiting offsets: %w", err)
	}
	return nil
}

func (c Consumer) newBackoff() *backoff {
	backoffConfiguration := c.config.backoffConfig
	return newBackoff(backoffConfiguration.base, backoffConfiguration.max, backoffConfiguration.factor)
}

type batchConsumer struct {
	recordConsumer               RecordConsumer
	dlqProducer                  *dlqProducer
	processedEpochOffsetsTracker *processedEpochOffsetsTracker
	backoff                      *backoff
}

func (bc batchConsumer) consumeFetches(ctx context.Context, fetches kgo.Fetches) (err error) {
	ctx, cancelPublishing := context.WithCancel(ctx)
	defer func() {
		cancelPublishing()
		dlqProcessed, publishingErr := bc.dlqProducer.drain()
		if publishingErr != nil {
			err = errors.Join(err, publishingErr)
			return
		}
		bc.processedEpochOffsetsTracker.registerAsProcessed(dlqProcessed...)
	}()

	var errs []error
	iter := fetches.RecordIter()

	record := bc.initRecord(iter)
	if record == nil {
		return nil
	}

	for !iter.Done() {
		if err = ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("consuming fetches: %w", err))
			return errors.Join(errs...)
		}

		err = bc.recordConsumer(ctx, record)
		if err == nil {
			bc.backoff.clear()
			bc.processedEpochOffsetsTracker.registerAsProcessed(record)
			record = iter.Next()
		} else if errors.Is(err, ErrTransient) {
			if err = bc.backoff.wait(ctx); err != nil {
				errs = append(errs, err)
				return errors.Join(errs...)
			}
			errs = append(errs, err)
		} else if errors.Is(err, ErrPermanent) {
			failed := failedRecord{record: record, cause: err}
			bc.dlqProducer.produce(ctx, failed)
			errs = append(errs, err)
			record = iter.Next()
		} else {
			errs = append(errs, newUnclassifiedErr(err))
			return errors.Join(errs...)
		}
	}

	return nil
}

func (bc batchConsumer) initRecord(iter *kgo.FetchesRecordIter) *kgo.Record {
	if !iter.Done() {
		return iter.Next()
	}
	return nil
}

func newUnclassifiedErr(err error) error {
	return fmt.Errorf("unclassified err: %w", err)
}

type RecordConsumer func(ctx context.Context, record *kgo.Record) error

var (
	ErrPermanent = errors.New("permanent err")
	ErrTransient = errors.New("transient err")
)
