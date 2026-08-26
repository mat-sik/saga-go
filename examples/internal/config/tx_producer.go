package config

import (
	"context"
	"fmt"

	"github.com/sethvargo/go-envconfig"
)

type TxProducerConfig struct {
	KafkaSeeds        []string `env:"TX_PRODUCER_KAFKA_SEEDS"`
	TransactionsTopic string   `env:"TX_PRODUCER_KAFKA_TRANSACTIONS_TOPIC"`
	ProduceAmount     int      `env:"TX_PRODUCER_PRODUCE_AMOUNT"`
}

func NewTxProducer(ctx context.Context) (TxProducerConfig, error) {
	var config TxProducerConfig
	if err := envconfig.Process(ctx, &config); err != nil {
		return TxProducerConfig{}, fmt.Errorf("processing tx-producer env variables: %w", err)
	}

	return config, nil
}
