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
	"github.com/mat-sik/saga-go/examples/internal/adapters/sagaadapters"
	"github.com/mat-sik/saga-go/examples/internal/adapters/static"
	"github.com/mat-sik/saga-go/examples/internal/config"
	"github.com/mat-sik/saga-go/examples/internal/domain/alarm"
	"github.com/mat-sik/saga-go/examples/internal/domain/count"
	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"github.com/mat-sik/saga-go/examples/internal/migrations"
	"github.com/mat-sik/saga-go/examples/internal/otel/oteldecorator"
	"github.com/mat-sik/saga-go/examples/internal/otel/otelinit"
	"github.com/mat-sik/saga-go/examples/internal/otel/otelobserver"
	"github.com/mat-sik/saga-go/kgoconsumer"
	"github.com/mat-sik/saga-go/saga"
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	conf, err := config.NewTxConsumer(ctx)
	if err != nil {
		slog.Error("reading tx-consumer config", "err", err)
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

	if err = migrations.RunTxConsumer(pool); err != nil {
		slog.Error("running tx-consumer migrations", "err", err)
		return 1
	}

	consumerFactory := func() (kgoconsumer.Consumer, error) {
		return newTxSagaConsumer(conf, pool)
	}

	if err := kgoconsumer.Run(ctx, conf.ConsumerCount, consumerFactory); err != nil {
		slog.Error("consumer", "err", errors.Join(err))
		return 1
	}

	return 0
}

func newTxSagaConsumer(conf config.TxConsumer, pool *pgxpool.Pool) (kgoconsumer.Consumer, error) {
	kafkaClient, err := kgoconsumer.NewClient(
		conf.KafkaSeeds,
		conf.TransactionsTopicConsumerGroup,
		[]string{conf.TransactionsTopic},
	)
	if err != nil {
		return kgoconsumer.Consumer{}, err
	}

	tracer := otelinit.NewTracer()

	aggregateRepository := postgres.NewAggregateRepository()
	alarmValueProvider := alarm.NewAlarmValueProvider(static.NewAlarmValueProvider(conf.AlarmValue))
	alarmRaiser := alarm.NewAlarmRaiser(kafka.NewAlarmProducer(kafkaClient.ToKgo(), conf.AlarmTopic))

	aggregate := tx.NewAggregate(aggregateRepository, alarmValueProvider, alarmRaiser, otelobserver.NewAggregateObserver())
	tracedAggregate := oteldecorator.NewTracedTxAggregate(aggregate, tracer)
	log := tx.NewLog(postgres.NewLogRepository())
	tracedLog := oteldecorator.NewTracedTxLog(log, tracer)
	txSagaAction := sagaadapters.NewTxSagaAction(tracedLog, tracedAggregate)
	tracedTxSagaAction := oteldecorator.NewTracedTxSagaAction(txSagaAction, tracer)

	counter := count.NewCounter(postgres.NewCountRepository())
	tracedCounter := oteldecorator.NewTracedCounter(counter, tracer)
	countSagaAction := sagaadapters.NewCountSagaAction[sagaadapters.RegisterSagaCommand, sagaadapters.UnregisterSagaCommand](tracedCounter)
	tracedCountSagaAction := oteldecorator.NewTracedCountSagaAction(countSagaAction, tracer)

	sagaActions := []saga.Action[sagaadapters.RegisterSagaCommand, sagaadapters.UnregisterSagaCommand]{
		tracedTxSagaAction,
		tracedCountSagaAction,
	}

	sagaConsumer := saga.NewConsumer(sagaActions, postgres.NewTxSagaConsumerRepository(), otelobserver.NewSagaConsumerObserver())
	tracedSagaConsumer := oteldecorator.NewTracedSagaConsumer(sagaConsumer, tracer)

	txRunner := func(ctx context.Context, fn func(context.Context) error) error {
		return postgres.WithTx(ctx, pool, fn)
	}

	return kafka.NewTxSagaConsumer(kafkaClient, txRunner, conf.TransactionsDLQTopic, tracedSagaConsumer, kgoconsumer.WithTracer(tracer))
}
