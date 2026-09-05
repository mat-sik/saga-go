package kgoconsumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Client struct {
	cancelProcessing *cancelProcessingStore
	kgoClient        *kgo.Client
}

func (c Client) ToKgo() *kgo.Client {
	return c.kgoClient
}

func NewClient(seeds []string, consumerGroup string, topics []string, opts ...ClientOption) (Client, error) {
	cfg := newClientConfig(opts...)
	cancelProcessing := newCancelProcessingStore()

	kgoOpts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsCallbackBlocked(func(ctx context.Context, client *kgo.Client) {
			cancelProcessing.cancel()
			cfg.onRebalanceBlocked()
		}),
	}
	kgoOpts = append(kgoOpts, cfg.toKgoOpts()...)

	client, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		return Client{}, fmt.Errorf("creating franz-go client: %w", err)
	}

	return Client{
		cancelProcessing: cancelProcessing,
		kgoClient:        client,
	}, nil
}

type Consumer struct {
	client           *kgo.Client
	dlqProducer      *dlqProducer
	cancelProcessing *cancelProcessingStore
	recordConsumer   RecordConsumer
	config           config
}

func NewConsumer(
	kafkaClient Client,
	dlqTopic string,
	recordConsumer RecordConsumer,
	options ...Option,
) (Consumer, error) {
	return Consumer{
		client:           kafkaClient.kgoClient,
		dlqProducer:      newDlqProducer(kafkaClient.kgoClient, dlqTopic),
		cancelProcessing: kafkaClient.cancelProcessing,
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

func (c Consumer) Close() {
	c.client.Close()
}

func (c Consumer) pollFetches(ctx context.Context) error {
	defer c.client.AllowRebalance()

	fetches := c.client.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return nil
	}

	if err := c.fetchesFatalError(fetches); err != nil {
		return err
	}

	consumerCtx, cancel := context.WithTimeout(ctx, c.config.processingConfig.timeout)
	c.cancelProcessing.store(cancel)
	defer c.cancelProcessing.cancel()

	consumer := c.newBatchConsumer()

	consumeErr := consumer.consumeFetches(consumerCtx, fetches)
	consumerCanceled := contextCanceled(consumeErr)
	if consumerCanceled {
		slog.Warn("consumption cancelled", "err", consumeErr)
	} else if consumeErr != nil {
		return consumeErr
	}

	committable := consumer.processedEpochOffsetsTracker.committableEpochOffsets()
	if err := c.commitOffsets(ctx, committable); err != nil {
		return err
	}

	if consumerCanceled {
		c.rewindToCommitted(fetches, committable)
	}

	return nil
}

func contextCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (c Consumer) rewindToCommitted(fetches kgo.Fetches, committable map[string]map[int32]kgo.EpochOffset) {
	resetOffsets := make(map[string]map[int32]kgo.EpochOffset)

	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if len(p.Records) == 0 {
			return
		}

		topicOffsets, ok := resetOffsets[p.Topic]
		if !ok {
			topicOffsets = make(map[int32]kgo.EpochOffset)
			resetOffsets[p.Topic] = topicOffsets
		}

		if committed, ok := committable[p.Topic][p.Partition]; ok {
			topicOffsets[p.Partition] = committed
			return
		}

		first := p.Records[0]
		topicOffsets[p.Partition] = kgo.EpochOffset{
			Epoch:  first.LeaderEpoch,
			Offset: first.Offset,
		}
	})

	c.client.SetOffsets(resetOffsets)
}

func (c Consumer) fetchesFatalError(fetches kgo.Fetches) error {
	allNonFatal := true

	var joinedErr error
	for _, fetchErr := range fetches.Errors() {
		err := fmt.Errorf("fetch error topic=%s partition=%d: %w", fetchErr.Topic, fetchErr.Partition, fetchErr.Err)
		if isFatalFetchErr(err) {
			allNonFatal = false
		}
		joinedErr = errors.Join(joinedErr, err)
	}

	if allNonFatal && joinedErr != nil {
		slog.Warn("non fatal fetching", "err", joinedErr)
		return nil
	}
	return joinedErr
}

func isFatalFetchErr(err error) bool {
	var dataLoss *kgo.ErrDataLoss
	if errors.As(err, &dataLoss) {
		return false
	}
	return true
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

func (c Consumer) commitOffsets(ctx context.Context, uncommitted map[string]map[int32]kgo.EpochOffset) error {
	var err error
	collectErr := func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, onDoneErr error) {
		err = onDoneErr
	}
	c.client.CommitOffsetsSync(ctx, uncommitted, collectErr)
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
	defer func() {
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
		default:
			bc.dlqProducer.produce(ctx, failedRecord{record: record, cause: err})
			return append(errs, err), nil
		}
	}
}

type RecordConsumer func(ctx context.Context, record *kgo.Record) error

var (
	ErrTransient = errors.New("transient err")
)
