package config

import (
	"context"
	"fmt"

	"github.com/sethvargo/go-envconfig"
)

type TxProducer struct {
	KafkaSeeds          []string `env:"TX_PRODUCER_KAFKA_SEEDS"`
	TransactionsTopic   string   `env:"TX_PRODUCER_KAFKA_TRANSACTIONS_TOPIC"`
	ProduceAmount       int      `env:"TX_PRODUCER_PRODUCE_AMOUNT"`
	Currencies          []string `env:"TX_PRODUCER_GENERATOR_CURRENCIES"`
	PlayerIDAmount      int      `env:"TX_PRODUCER_GENERATOR_PLAYER_ID_AMOUNT"`
	TransactionIDAmount int      `env:"TX_PRODUCER_GENERATOR_TRANSACTION_ID_AMOUNT"`
	DaysAmount          int      `env:"TX_PRODUCER_GENERATOR_DAYS_AMOUNT"`
	MaxValue            int      `env:"TX_PRODUCER_GENERATOR_MAX_VALUE"`
}

func NewTxProducer(ctx context.Context) (TxProducer, error) {
	var config TxProducer
	if err := envconfig.Process(ctx, &config); err != nil {
		return TxProducer{}, fmt.Errorf("processing tx-producer env variables: %w", err)
	}
	return config, nil
}
