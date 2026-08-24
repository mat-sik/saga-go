package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/saga-go/examples/internal/adapters/kafka"
	"github.com/mat-sik/saga-go/examples/internal/adapters/postgres"
	"github.com/mat-sik/saga-go/examples/internal/adapters/static"
	"github.com/mat-sik/saga-go/examples/internal/config"
	"github.com/mat-sik/saga-go/examples/internal/domain/count"
	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"github.com/mat-sik/saga-go/examples/internal/kgoconsumer"
	"github.com/mat-sik/saga-go/examples/internal/migrations"
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

	pool, err := pgxpool.New(ctx, conf.DatabaseURL)
	if err != nil {
		slog.Error("creating pgx pool", "err", err)
		return 1
	}
	defer pool.Close()

	if err = migrations.Run(pool); err != nil {
		slog.Error("running tx-consumer migrations", "err", err)
		return 1
	}

	errCh := make(chan error, conf.ConsumerCount)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for range conf.ConsumerCount {
		wg.Go(func() {
			consumer, err := newConsumer(conf, pool)
			if err != nil {
				errCh <- fmt.Errorf("creating consumer: %w", err)
				return
			}
			defer consumer.Close()
			if err = consumer.StartPolling(ctx); err != nil {
				errCh <- fmt.Errorf("polling: %w", err)
				return
			}
			errCh <- nil
		})
	}

	var errs []error
	for range conf.ConsumerCount {
		if err = <-errCh; err != nil {
			cancel()
			errs = append(errs, err)
		}
	}

	wg.Wait()

	if len(errs) > 0 {
		slog.Error("consumer", "err", errors.Join(errs...))
		return 1
	}

	return 0
}

func newConsumer(conf config.TxConsumerConfig, pool *pgxpool.Pool) (kgoconsumer.Consumer, error) {
	kafkaClient, err := kgoconsumer.NewClient(
		conf.KafkaSeeds,
		conf.TransactionsTopicConsumerGroup,
		[]string{conf.TransactionsTopic},
	)
	if err != nil {
		return kgoconsumer.Consumer{}, err
	}

	aggregateRepository := postgres.NewAggregateRepository()
	alarmValueProvider := tx.NewAlarmValueProvider(static.NewAlarmValueProvider(conf.AlarmValue))
	alarmRaiser := tx.NewAlarmRaiser(kafka.NewAlarmProducer(kafkaClient.ToKgo(), conf.AlarmTopic))

	aggregateAction := tx.NewAggregateSagaAction(aggregateRepository, alarmValueProvider, alarmRaiser)

	logAction := tx.NewLogSagaAction(postgres.NewLogRepository())

	txAction := tx.NewSagaAction(logAction, aggregateAction)

	countAction := count.NewSagaAction(postgres.NewCountRepository())

	actions := []saga.Action[tx.RegisterCommand, tx.UnregisterCommand]{
		txAction,
		countAction,
	}

	sagaConsumer := saga.NewConsumer(actions, postgres.NewConsumerRepository())

	return kafka.NewSagaConsumer(kafkaClient, conf.DLQTopic, pool, sagaConsumer)
}
