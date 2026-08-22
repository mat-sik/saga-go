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
	topics []string,
	dlqTopic string,
	recordConsumer RecordConsumer,
	options ...Option,
) (Consumer, error) {
	cancelProcessing := newCancelProcessingStore()

	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topics...),
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

	consumerCtx, cancel := context.WithTimeout(ctx, c.config.processingConfig.timeout)
	c.cancelProcessing.store(cancel)
	defer c.cancelProcessing.cancel()

	consumer := c.newBatchConsumer()
	if err := consumer.consumeFetches(consumerCtx, fetches); err != nil {
		return err
	}

	committable := consumer.processedEpochOffsetsTracker.committableEpochOffsets()

	return c.commitOffsetsSync(ctx, committable)
}

func (c Consumer) newBatchConsumer() batchConsumer {
	return batchConsumer{
		recordConsumer:               c.recordConsumer,
		dlqProducer:                  c.dlqProducer,
		processedEpochOffsetsTracker: newProcessedOffsetsTracker(),
		backoff:                      c.newBackoff(),
	}
}

func (c Consumer) newBackoff() *backoff {
	backoffConfiguration := c.config.backoffConfig
	return newBackoff(backoffConfiguration.base, backoffConfiguration.max, backoffConfiguration.factor)
}

func (c Consumer) commitOffsetsSync(ctx context.Context, uncommitted map[string]map[int32]kgo.EpochOffset) error {
	var err error
	c.client.CommitOffsetsSync(ctx, uncommitted, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, onDoneErr error) {
		err = onDoneErr
	})
	if err != nil {
		return fmt.Errorf("commiting offsets: %w", err)
	}
	return nil
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
	for !iter.Done() {
		record := iter.Next()

		errs, err = bc.consumeWithRetry(ctx, record, errs)
		if err != nil {
			return errors.Join(append(errs, err)...)
		}
	}

	return nil
}

func (bc batchConsumer) consumeWithRetry(ctx context.Context, record *kgo.Record, errs []error) ([]error, error) {
	for {
		if err := ctx.Err(); err != nil {
			return errs, fmt.Errorf("consuming fetches: %w", err)
		}

		err := bc.recordConsumer(ctx, record)
		switch {
		case err == nil:
			bc.backoff.clear()
			bc.processedEpochOffsetsTracker.registerAsProcessed(record)
			return errs, nil
		case errors.Is(err, ErrTransient):
			errs = append(errs, err)
			if err = bc.backoff.wait(ctx); err != nil {
				return errs, err
			}
		case errors.Is(err, ErrPermanent):
			bc.dlqProducer.produce(ctx, failedRecord{record: record, cause: err})
			return append(errs, err), nil
		default:
			return errs, newUnclassifiedErr(err)
		}
	}
}

func newUnclassifiedErr(err error) error {
	return fmt.Errorf("unclassified err: %w", err)
}

type RecordConsumer func(ctx context.Context, record *kgo.Record) error

var (
	ErrPermanent = errors.New("permanent err")
	ErrTransient = errors.New("transient err")
)
