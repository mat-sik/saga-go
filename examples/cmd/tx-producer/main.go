package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/mat-sik/saga-go/examples/internal/adapters/kafka"
	"github.com/mat-sik/saga-go/examples/internal/config"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	conf, err := config.NewTxProducer(ctx)
	if err != nil {
		slog.Error("reading tx-producer config", "err", err)
		return 1
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(conf.KafkaSeeds...),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		slog.Error("creating franz-go client", "err", err)
		return 1
	}
	defer client.Close()

	records := make([]*kgo.Record, conf.ProduceAmount)
	for i := range conf.ProduceAmount {
		transactionID, err := randomID()
		if err != nil {
			slog.Error("generating transactionID", "err", err)
			return 1
		}

		playerID, err := randomID()
		if err != nil {
			slog.Error("generating playerID", "err", err)
			return 1
		}

		randCurrency := string(randomCurrency())
		randValue := randomValue()
		txTime := time.Now()

		record, err := newRecord(conf.TransactionsTopic, transactionID, playerID, randCurrency, randValue, txTime)
		if err != nil {
			slog.Error("creating record", "err", err)
			return 1
		}

		records[i] = record
	}

	results := client.ProduceSync(ctx, records...)

	var produceErr error
	for _, result := range results {
		if err := result.Err; err != nil {
			produceErr = errors.Join(produceErr, err)
		}
	}

	if produceErr != nil {
		slog.Error("producing records", "err", produceErr)
		return 1
	}

	return 0
}

func newRecord(topic, transactionID, playerID, currency string, value int, txTime time.Time) (*kgo.Record, error) {
	registerRecord := kafka.RegisterRecord{
		PlayerID: playerID,
		Currency: currency,
		Value:    value,
		Time:     txTime,
	}

	body, err := json.Marshal(registerRecord)
	if err != nil {
		return nil, fmt.Errorf("marshaling %v: %w", registerRecord, err)
	}

	key := transactionID
	return &kgo.Record{
		Key:   []byte(key),
		Value: body,
		Topic: topic,
		Headers: []kgo.RecordHeader{
			{
				Key:   kafka.CmdTypeHeader,
				Value: []byte(kafka.CmdTypeRegister),
			},
		},
	}, nil
}

func randomValue() int {
	return rand.N(100) + 1
}

func randomID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generating UUIDv7: %w", err)
	}
	return id.String(), err
}

func randomCurrency() currency {
	currenciesCount := 3
	switch rand.N(currenciesCount) {
	case 0:
		return EUR
	case 1:
		return USD
	default:
		return PLN
	}
}

type currency string

var (
	EUR currency = "EUR"
	USD currency = "USD"
	PLN currency = "PLN"
)
