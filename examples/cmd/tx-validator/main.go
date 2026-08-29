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

	"github.com/mat-sik/saga-go/examples/internal/adapters/kafka"
	"github.com/mat-sik/saga-go/examples/internal/config"
	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"github.com/mat-sik/saga-go/examples/internal/kgoconsumer"
	"github.com/twmb/franz-go/pkg/kgo"
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

	errCh := make(chan error, conf.ConsumerCount)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for range conf.ConsumerCount {
		wg.Go(func() {
			consumer, err := newConsumer(conf)
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

func newConsumer(conf config.TxValidator) (kgoconsumer.Consumer, error) {
	kafkaClient, err := kgoconsumer.NewClient(
		conf.KafkaSeeds,
		conf.TransactionsTopicConsumerGroup,
		[]string{conf.TransactionsTopic},
	)
	if err != nil {
		return kgoconsumer.Consumer{}, err
	}

	validator := tx.NewValidator(kafka.NewRandomValidator(kafkaClient.ToKgo(), conf.TransactionsTopic, conf.CompensatePercent))

	recordConsumer := func(ctx context.Context, record *kgo.Record) error {
		return consumeRegisterRecord(ctx, validator, record)
	}

	return kgoconsumer.NewConsumer(kafkaClient, conf.TransactionsDLQTopic, recordConsumer)
}

func consumeRegisterRecord(ctx context.Context, validator tx.Validator, record *kgo.Record) error {
	registerCommand, ok, err := mapToRegisterCommand(record)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return validator.ValidateAndCompensate(ctx, registerCommand)
}

func mapToRegisterCommand(record *kgo.Record) (tx.RegisterCommand, bool, error) {
	command, err := kafka.MapToCommand(record)
	if err != nil {
		return tx.RegisterCommand{}, false, fmt.Errorf("mapping to command %v: %w", record, err)
	}

	registerCommand, ok := command.ToTransaction()
	if !ok {
		return tx.RegisterCommand{}, false, fmt.Errorf("mapping to register command %v: %w", command, err)
	}
	return registerCommand, true, nil
}
