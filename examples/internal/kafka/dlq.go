package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type dlqProducer struct {
	client   *kgo.Client
	dlqTopic string
	results  chan dlqProduceResult
	pending  int
}

func newDlqProducer(client *kgo.Client, dlqTopic string) *dlqProducer {
	return &dlqProducer{
		client:   client,
		dlqTopic: dlqTopic,
		results:  make(chan dlqProduceResult),
	}
}

func (p *dlqProducer) produce(ctx context.Context, failed failedRecord) {
	p.client.Produce(ctx, p.toDLQRecord(failed), func(_ *kgo.Record, err error) {
		go func() {
			p.results <- dlqProduceResult{
				err:          err,
				failedRecord: failed.record,
			}
		}()
	})
	p.pending++
}

func (p *dlqProducer) toDLQRecord(failed failedRecord) *kgo.Record {
	original := failed.record
	cause := failed.cause

	return &kgo.Record{
		Topic: p.dlqTopic,
		Key:   original.Key,
		Value: original.Value,
		Headers: append(original.Headers, kgo.RecordHeader{
			Key:   "dlq-reason",
			Value: []byte(cause.Error()),
		}),
	}
}

func (p *dlqProducer) drain() ([]*kgo.Record, error) {
	defer func() {
		p.pending = 0
	}()
	var processed []*kgo.Record
	var publishErrs []error
	for range p.pending {
		result := <-p.results
		if result.err != nil {
			publishErrs = append(publishErrs, result.err)
		} else {
			processed = append(processed, result.failedRecord)
		}
	}
	return processed, fmt.Errorf("publishing: %w", errors.Join(publishErrs...))
}

type dlqProduceResult struct {
	err          error
	failedRecord *kgo.Record
}

type failedRecord struct {
	record *kgo.Record
	cause  error
}
