package kotelinit

import (
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
)

func NewKOTel() *kotel.Kotel {
	tracer := kotel.NewTracer(
		kotel.TracerProvider(otel.GetTracerProvider()),
		kotel.TracerPropagator(otel.GetTextMapPropagator()),
	)
	meter := kotel.NewMeter(kotel.MeterProvider(otel.GetMeterProvider()))
	return kotel.NewKotel(kotel.WithTracer(tracer), kotel.WithMeter(meter))
}
