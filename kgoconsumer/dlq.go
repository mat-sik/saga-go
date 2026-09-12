package kgoconsumer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type dlqProducer struct {
	client   *kgo.Client
	dlqTopic string
	tracer   trace.Tracer
	wg       sync.WaitGroup
	results  []*dlqProduceResult
}

func newDlqProducer(client *kgo.Client, dlqTopic string, tracer trace.Tracer) *dlqProducer {
	return &dlqProducer{
		client:   client,
		dlqTopic: dlqTopic,
		tracer:   tracer,
	}
}

func (p *dlqProducer) produce(ctx context.Context, failed failedRecord) {
	const (
		spanName = "kafka.produce.record.dlq"
		errDesc  = "produce to dlq failed"
	)

	ctx, span := p.tracer.Start(ctx, spanName)

	result := &dlqProduceResult{}
	p.results = append(p.results, result)

	p.wg.Add(1)
	p.client.Produce(ctx, p.toDLQRecord(failed), func(_ *kgo.Record, err error) {
		defer func() {
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, errDesc)
			}
			span.End()
			p.wg.Done()
		}()

		result.populate(err, failed.record)
	})
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
	p.wg.Wait()

	var processed []*kgo.Record
	var publishErrs []error

	for _, result := range p.results {
		if result.err != nil {
			publishErrs = append(publishErrs, result.err)
		} else {
			processed = append(processed, result.failedRecord)
		}
	}
	if len(publishErrs) > 0 {
		return processed, fmt.Errorf("publishing: %w", errors.Join(publishErrs...))
	}
	return processed, nil
}

type dlqProduceResult struct {
	err          error
	failedRecord *kgo.Record
}

func (r *dlqProduceResult) populate(err error, failed *kgo.Record) {
	r.err = err
	r.failedRecord = failed
}

type failedRecord struct {
	record *kgo.Record
	cause  error
}
