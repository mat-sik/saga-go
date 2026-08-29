package config

import (
	"context"
	"fmt"

	"github.com/sethvargo/go-envconfig"
)

type TxValidatorConfig struct {
	KafkaSeeds                     []string `env:"TX_VALIDATOR_KAFKA_SEEDS"`
	TransactionsTopic              string   `env:"TX_VALIDATOR_KAFKA_TRANSACTIONS_TOPIC"`
	TransactionsDLQTopic           string   `env:"TX_VALIDATOR_KAFKA_TRANSACTIONS_DLQ_TOPIC"`
	TransactionsTopicConsumerGroup string   `env:"TX_VALIDATOR_KAFKA_TRANSACTIONS_TOPIC_CONSUMER_GROUP"`
	ConsumerCount                  int      `env:"TX_VALIDATOR_KAFKA_CONSUMER_COUNT"`
	CompensatePercent              int      `env:"TX_VALIDATOR_KAFKA_COMPENSATE_PERCENT"`
}

func NewTxValidator(ctx context.Context) (TxValidatorConfig, error) {
	var config TxValidatorConfig
	if err := envconfig.Process(ctx, &config); err != nil {
		return TxValidatorConfig{}, fmt.Errorf("processing tx-validator env variables: %w", err)
	}

	return config, nil
}
