package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/saga-go/examples/internal/adapters/kafka"
	"github.com/mat-sik/saga-go/examples/internal/adapters/postgres"
	"github.com/mat-sik/saga-go/examples/internal/config"
	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"github.com/mat-sik/saga-go/examples/internal/idempotent"
	"github.com/mat-sik/saga-go/examples/internal/kgoconsumer"
	"github.com/mat-sik/saga-go/examples/internal/migrations"
	"github.com/mat-sik/saga-go/examples/internal/otel/oteldecorator"
	"github.com/mat-sik/saga-go/examples/internal/otel/otelinit"
	"github.com/mat-sik/saga-go/examples/internal/otel/otelobserver"
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	conf, err := config.NewTxValidator(ctx)
	if err != nil {
		slog.Error("reading tx-validator config", "err", err)
		return 1
	}

	if conf.OTelCollectorHost != "" {
		var shutdown otelinit.ShutdownFunc
		shutdown, err = otelinit.InitOTelSDK(ctx, conf.OTelCollectorHost, conf.OTelServiceName)
		if err != nil {
			slog.Error("initializing OTel SDK", "err", err)
			return 1
		}
		defer func() {
			if err = shutdown(context.Background()); err != nil {
				slog.Error("shutting down OTel SDK", "err", err)
			}
		}()
	}

	pool, err := pgxpool.New(ctx, conf.DatabaseURL)
	if err != nil {
		slog.Error("creating pgx pool", "err", err)
		return 1
	}
	defer pool.Close()

	if err = migrations.RunIdempotentConsumer(pool); err != nil {
		slog.Error("running tx-validator migrations", "err", err)
		return 1
	}

	consumerFactory := func() (kgoconsumer.Consumer, error) {
		return newTxConsumer(conf, pool)
	}

	if err := kgoconsumer.Run(ctx, conf.ConsumerCount, consumerFactory); err != nil {
		slog.Error("consumer", "err", errors.Join(err))
		return 1
	}

	return 0
}

func newTxConsumer(conf config.TxValidator, pool *pgxpool.Pool) (kgoconsumer.Consumer, error) {
	kafkaClient, err := kgoconsumer.NewClient(
		conf.KafkaSeeds,
		conf.TransactionsTopicConsumerGroup,
		[]string{conf.TransactionsTopic},
	)
	if err != nil {
		return kgoconsumer.Consumer{}, err
	}

	tracer := otelinit.NewTracer()

	validator := tx.NewValidator(kafka.NewRandomValidator(kafkaClient.ToKgo(), conf.TransactionsTopic, conf.CompensatePercent), otelobserver.NewValidatorObserver())
	tracedValidator := oteldecorator.NewTracedTxValidator(validator, tracer)

	recordConsumers := []func(context.Context, tx.RegisterCommand) error{
		tracedValidator.ValidateAndCompensate,
	}

	idempotentConsumer := idempotent.NewConsumer(recordConsumers, postgres.NewTxConsumerRepository())

	txRunner := func(ctx context.Context, fn func(context.Context) error) error {
		return postgres.WithTx(ctx, pool, fn)
	}

	return kafka.NewTxConsumer(kafkaClient, txRunner, conf.TransactionsDLQTopic, idempotentConsumer, kgoconsumer.WithTracer(tracer))
}
