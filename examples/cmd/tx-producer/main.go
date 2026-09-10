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
	"github.com/mat-sik/saga-go/examples/internal/otel/kotelinit"
	"github.com/mat-sik/saga-go/examples/internal/otel/otelinit"
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

	kOTelService := kotelinit.NewKOTel()

	opts := []kgo.Opt{
		kgo.SeedBrokers(conf.KafkaSeeds...),
		kgo.WithHooks(kOTelService.Hooks()...),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		slog.Error("creating franz-go client", "err", err)
		return 1
	}
	defer client.Close()

	generator, err := newRecordGenerator(
		conf.TransactionsTopic,
		conf.Currencies,
		conf.PlayerIDAmount,
		conf.TransactionIDAmount,
		conf.DaysAmount,
		conf.MaxValue,
	)
	if err != nil {
		slog.Error("creating record generator", "err", err)
		return 1
	}

	records := make([]*kgo.Record, conf.ProduceAmount)
	for i := range conf.ProduceAmount {

		record, err := generator.generateRecord()
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

type recordGenerator struct {
	topic          string
	currencies     []string
	playerIDs      []string
	transactionIDs []string
	days           []time.Time
	maxValue       int
}

func newRecordGenerator(
	topic string,
	currencies []string,
	playersIDAmount int,
	transactionIDAmount int,
	daysAmount int,
	maxValue int,
) (recordGenerator, error) {
	playerIDs, err := randomIDs(playersIDAmount)
	if err != nil {
		return recordGenerator{}, err
	}

	transactionIDs, err := randomIDs(transactionIDAmount)
	if err != nil {
		return recordGenerator{}, err
	}

	return recordGenerator{
		topic:          topic,
		currencies:     currencies,
		playerIDs:      playerIDs,
		transactionIDs: transactionIDs,
		days:           nextNDays(daysAmount),
		maxValue:       maxValue,
	}, nil
}

func randomIDs(amount int) ([]string, error) {
	ids := make([]string, amount)
	for i := range amount {
		id, err := randomID()
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
}

func nextNDays(amount int) []time.Time {
	days := make([]time.Time, amount)
	now := time.Now()
	for i := range amount {
		day := now.AddDate(0, 0, i)
		days[i] = day
	}
	return days
}

func (g recordGenerator) generateRecord() (*kgo.Record, error) {
	registerRecord := kafka.NewRegisterRecord(g.pickPlayerID(), g.pickCurrency(), g.pickValue(), g.pickDay())
	return g.newRecord(g.pickTransactionID(), registerRecord)
}

func (g recordGenerator) newRecord(transactionID string, registerRecord kafka.RegisterRecord) (*kgo.Record, error) {
	body, err := json.Marshal(registerRecord)
	if err != nil {
		return nil, fmt.Errorf("marshaling %v: %w", registerRecord, err)
	}

	key := transactionID
	return &kgo.Record{
		Key:   []byte(key),
		Value: body,
		Headers: []kgo.RecordHeader{
			{
				Key:   kafka.CmdTypeHeader,
				Value: []byte(kafka.CmdTypeRegister),
			},
		},
		Topic: g.topic,
	}, nil
}

func (g recordGenerator) pickPlayerID() string {
	return pickElFromSlice(g.playerIDs)
}

func (g recordGenerator) pickCurrency() string {
	return pickElFromSlice(g.currencies)
}

func (g recordGenerator) pickDay() time.Time {
	return pickElFromSlice(g.days)
}

func (g recordGenerator) pickTransactionID() string {
	return pickElFromSlice(g.transactionIDs)
}

func (g recordGenerator) pickValue() int {
	return rand.N(g.maxValue) + 1
}

func pickElFromSlice[T any](slice []T) T {
	return slice[rand.N(len(slice))]
}

func randomID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generating transaction UUIDv7: %w", err)
	}
	return id.String(), err
}
