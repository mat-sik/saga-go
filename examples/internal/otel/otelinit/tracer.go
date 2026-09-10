package otelinit

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName = "github.com/mat-sik/saga-go/examples"
)

func NewTracer() trace.Tracer {
	return otel.Tracer(instrumentationName)
}
