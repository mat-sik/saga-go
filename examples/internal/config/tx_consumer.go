package config

import (
	"context"
	"fmt"

	"github.com/sethvargo/go-envconfig"
)

type TxConsumerConfig struct {
	DatabaseURL                    string   `env:"TX_CONSUMER_DATABASE_URL"`
	KafkaSeeds                     []string `env:"TX_CONSUMER_KAFKA_SEEDS"`
	TransactionsTopic              string   `env:"TX_CONSUMER_KAFKA_TRANSACTIONS_TOPIC"`
	TransactionsDLQTopic           string   `env:"TX_CONSUMER_KAFKA_TRANSACTIONS_DLQ_TOPIC"`
	TransactionsTopicConsumerGroup string   `env:"TX_CONSUMER_KAFKA_TRANSACTIONS_TOPIC_CONSUMER_GROUP"`
	AlarmTopic                     string   `env:"TX_CONSUMER_KAFKA_ALARM_TOPIC"`
	ConsumerCount                  int      `env:"TX_CONSUMER_KAFKA_CONSUMER_COUNT"`
	AlarmValue                     int      `env:"TX_CONSUMER_ALARM_VALUE"`
}

func NewTxConsumer(ctx context.Context) (TxConsumerConfig, error) {
	var config TxConsumerConfig
	if err := envconfig.Process(ctx, &config); err != nil {
		return TxConsumerConfig{}, fmt.Errorf("processing tx-consumer env variables: %w", err)
	}

	return config, nil
}
