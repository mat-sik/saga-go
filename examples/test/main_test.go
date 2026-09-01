package test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()

	kafkaContainer, err := kafka.Run(ctx, "confluentinc/confluent-local:8.0.7")
	if err != nil {
		panic(fmt.Errorf("creating kafka test container: %w", err))
	}

	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			panic(fmt.Errorf("terminating kafka test container: %w", err))
		}
	}()

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		panic(fmt.Errorf("reading kafka test container brokers: %w", err))
	}

	testKafkaBrokers = brokers

	testInfraClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		panic(fmt.Errorf("creating shared franz-go admin client: %w", err))
	}
	defer testInfraClient.Close()

	adminClient = kadm.NewClient(testInfraClient)

	return m.Run()
}

var (
	testKafkaBrokers []string
	adminClient      *kadm.Client
)

func createTopic(tb testing.TB, topic string, partitions int) {
	resp, err := adminClient.CreateTopic(tb.Context(), int32(partitions), 1, nil, topic)
	if err != nil {
		tb.Fatalf("creating topic %s partitions %d: %v", topic, partitions, err)
	}
	if resp.Err != nil {
		tb.Fatalf("creating topic %s partitions %d: %v", topic, partitions, resp.Err)
	}

	tb.Cleanup(func() {
		resp, err := adminClient.DeleteTopic(context.Background(), topic)
		if err != nil {
			tb.Fatalf("deleting topic %s: %v", topic, err)
		}
		if resp.Err != nil {
			tb.Fatalf("deleting topic %s: %v", topic, resp.Err)
		}
	})
}

func newTopicName(tb testing.TB, topic string) string {
	return tb.Name() + topic
}

func newConsumerGroupName(tb testing.TB, consumerGroup string) string {
	consumerGroup = tb.Name() + consumerGroup

	tb.Cleanup(func() {
		resp, err := adminClient.DeleteGroup(context.Background(), consumerGroup)
		if err != nil {
			tb.Fatalf("deleting consumer group %s: %v", consumerGroup, err)
		}
		if resp.Err != nil {
			tb.Fatalf("deleting consumer group %s: %v", consumerGroup, resp.Err)
		}
	})

	return consumerGroup
}

type producer struct {
	client *kgo.Client
}

func newProducer(tb testing.TB) producer {
	opts := []kgo.Opt{
		kgo.SeedBrokers(testKafkaBrokers...),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		tb.Fatalf("creating franz-go client for producer: %v", err)
	}
	tb.Cleanup(client.Close)

	return producer{client: client}
}

func (p producer) produce(tb testing.TB, record *kgo.Record) {
	result := p.client.ProduceSync(tb.Context(), record)
	if err := result.FirstErr(); err != nil {
		tb.Fatalf("producing test record %v: %v", record, err)
	}
}

type consumer struct {
	client *kgo.Client
}

func newConsumer(tb testing.TB, topics []string, consumerGroup string) consumer {
	opts := []kgo.Opt{
		kgo.SeedBrokers(testKafkaBrokers...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		tb.Fatalf("creating franz-go client for consumer: %v", err)
	}
	tb.Cleanup(client.Close)

	return consumer{client: client}
}

func (c consumer) joinConsumerGroup(tb testing.TB) {
	fetches := c.client.PollRecords(tb.Context(), 1)
	if err := fetches.Err(); err != nil {
		tb.Fatalf("joining consumer group: %v", err)
	}
}
