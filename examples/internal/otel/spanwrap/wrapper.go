package spanwrap

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func Wrap(
	ctx context.Context,
	tracer trace.Tracer,
	spanName string,
	errDesc string,
	work func(ctx context.Context) error,
) error {
	ctx, span := tracer.Start(ctx, spanName)
	defer span.End()

	if err := work(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, errDesc)
		return err
	}
	return nil
}

func WrapWithAttrs(
	ctx context.Context,
	tracer trace.Tracer,
	spanName string,
	errDesc string,
	work func(ctx context.Context) error,
	attributesFactory func() []attribute.KeyValue,
) error {
	ctx, span := tracer.Start(ctx, spanName)
	defer span.End()

	span.SetAttributes(attributesFactory()...)

	if err := work(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, errDesc)
		return err
	}
	return nil
}

func WrapWithAttrsResult[T any](
	ctx context.Context,
	tracer trace.Tracer,
	spanName string,
	errDesc string,
	work func(ctx context.Context) (T, error),
	attributesFactory func() []attribute.KeyValue,
) (T, error) {
	ctx, span := tracer.Start(ctx, spanName)
	defer span.End()

	span.SetAttributes(attributesFactory()...)

	result, err := work(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, errDesc)
		return result, err
	}
	return result, nil
}
