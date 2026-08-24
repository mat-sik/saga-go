package main

import (
	"context"
	"log/slog"
	"os"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/saga-go/examples/internal/adapters/consumer"
	"github.com/mat-sik/saga-go/examples/internal/config"
	"github.com/mat-sik/saga-go/examples/internal/domain/count"
	"github.com/mat-sik/saga-go/examples/internal/kafka"
	"github.com/mat-sik/saga-go/examples/internal/migrations"
	"github.com/mat-sik/saga-go/saga"
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx := context.TODO()

	conf, err := config.NewTxConsumer(ctx)
	if err != nil {
		slog.Error("reading tx-consumer config", "err", err)
		return 1
	}

	var pool *pgxpool.Pool
	if conf.DatabaseURL != "" {
		pool, err = pgxpool.New(ctx, conf.DatabaseURL)
		if err != nil {
			slog.Error("creating pgx pool", "err", err)
			return 1
		}
		defer pool.Close()

		if err = migrations.Run(pool); err != nil {
			slog.Error("running tx-consumer migrations", "err", err)
			return 1
		}
	}

	consumerPortOut := consumer.Repository{}

	countAction := count.NewAction(count)

	sagaConsumer := saga.NewConsumer()

	var wg sync.WaitGroup
	for range conf.ConsumerCount {
		wg.Add(1)
		go func() {
			kafkaClient, _ := kafka.NewClient(
				conf.KafkaSeeds,
				conf.TransactionsTopicConsumerGroup,
				[]string{conf.TransactionsTopic},
			)

			consumer := consumer.NewKafkaSagaConsumer(kafkaClient, cancelProcessing, conf.DLQTopic, pool, nil)
		}()
	}

	return 0
}
