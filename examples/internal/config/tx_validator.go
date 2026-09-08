package config

import (
	"context"
	"fmt"

	"github.com/sethvargo/go-envconfig"
)

type TxValidator struct {
	DatabaseURL                    string   `env:"TX_VALIDATOR_DATABASE_URL"`
	KafkaSeeds                     []string `env:"TX_VALIDATOR_KAFKA_SEEDS"`
	TransactionsTopic              string   `env:"TX_VALIDATOR_KAFKA_TRANSACTIONS_TOPIC"`
	TransactionsDLQTopic           string   `env:"TX_VALIDATOR_KAFKA_TRANSACTIONS_DLQ_TOPIC"`
	TransactionsTopicConsumerGroup string   `env:"TX_VALIDATOR_KAFKA_TRANSACTIONS_TOPIC_CONSUMER_GROUP"`
	ConsumerCount                  int      `env:"TX_VALIDATOR_KAFKA_CONSUMER_COUNT"`
	CompensatePercent              int      `env:"TX_VALIDATOR_KAFKA_COMPENSATE_PERCENT"`
}

func NewTxValidator(ctx context.Context) (TxValidator, error) {
	var config TxValidator
	if err := envconfig.Process(ctx, &config); err != nil {
		return TxValidator{}, fmt.Errorf("processing tx-validator env variables: %w", err)
	}

	return config, nil
}
