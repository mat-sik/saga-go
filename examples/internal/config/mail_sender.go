package config

import (
	"context"
	"fmt"

	"github.com/sethvargo/go-envconfig"
)

type MailSender struct {
	OTelCollectorHost        string   `env:"MAIL_SENDER_OTEL_COLLECTOR_HOST"`
	OTelServiceName          string   `env:"MAIL_SENDER_OTEL_SERVICE_NAME, default=mail-sender"`
	DatabaseURL              string   `env:"MAIL_SENDER_DATABASE_URL"`
	KafkaSeeds               []string `env:"MAIL_SENDER_KAFKA_SEEDS"`
	AlarmsTopic              string   `env:"MAIL_SENDER_KAFKA_ALARMS_TOPIC"`
	AlarmsDLQTopic           string   `env:"MAIL_SENDER_KAFKA_ALARMS_DLQ_TOPIC"`
	AlarmsTopicConsumerGroup string   `env:"MAIL_SENDER_KAFKA_ALARMS_TOPIC_CONSUMER_GROUP"`
	ConsumerCount            int      `env:"MAIL_SENDER_KAFKA_CONSUMER_COUNT"`
	SMTPHost                 string   `env:"MAIL_SENDER_SMTP_HOST"`
	SMTPPort                 string   `env:"MAIL_SENDER_SMTP_PORT,default=587"`
	SMTPUsername             string   `env:"MAIL_SENDER_SMTP_USERNAME"`
	SMTPPassword             string   `env:"MAIL_SENDER_SMTP_PASSWORD"`
	MailFrom                 string   `env:"MAIL_SENDER_MAIL_FROM"`
	MailTo                   string   `env:"MAIL_SENDER_MAIL_TO"`
}

func (c MailSender) SMTPAddr() string {
	return c.SMTPHost + ":" + c.SMTPPort
}

func NewMailSender(ctx context.Context) (MailSender, error) {
	var config MailSender
	if err := envconfig.Process(ctx, &config); err != nil {
		return MailSender{}, fmt.Errorf("processing mail-sender env variables: %w", err)
	}
	return config, nil
}
