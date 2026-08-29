package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/smtp"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/mat-sik/saga-go/examples/internal/adapters/kafka"
	"github.com/mat-sik/saga-go/examples/internal/adapters/mail"
	"github.com/mat-sik/saga-go/examples/internal/config"
	"github.com/mat-sik/saga-go/examples/internal/kgoconsumer"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	conf, err := config.NewMailSender(ctx)
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

func newConsumer(conf config.MailSender) (kgoconsumer.Consumer, error) {
	kafkaClient, err := kgoconsumer.NewClient(
		conf.KafkaSeeds,
		conf.AlarmsTopicConsumerGroup,
		[]string{conf.AlarmsTopic},
	)
	if err != nil {
		return kgoconsumer.Consumer{}, err
	}

	auth := smtp.PlainAuth("", conf.SMTPUsername, conf.SMTPPassword, conf.SMTPHost)
	mailSender := mail.NewAlarmMailSender(auth, conf.SMTPAddr(), conf.MailFrom, conf.MailTo)

	consumeFn := func(ctx context.Context, record *kgo.Record) error {
		return consumeAlarm(ctx, mailSender, record)
	}
	return kgoconsumer.NewConsumer(kafkaClient, conf.AlarmsDLQTopic, consumeFn)
}

func consumeAlarm(ctx context.Context, mailSender mail.AlarmMailSender, record *kgo.Record) error {
	raiseRecord, ok, err := kafka.MapToRaiseAlarmRecord(record)
	if err != nil {
		return fmt.Errorf("mapping record to alarmRaiseRecord %v: %w", record, err)
	}
	if ok {
		return mailSender.RaiseAlarm(ctx, raiseRecord.PlayerID, raiseRecord.AlarmValue, raiseRecord.Value)
	}

	clearRecord, ok, err := kafka.MapToClearAlarmRecord(record)
	if err != nil {
		return fmt.Errorf("mapping record to alarmClearRecord %v: %w", record, err)
	}
	if !ok {
		return fmt.Errorf("unparsable record %v", record)
	}
	return mailSender.ClearAlarm(ctx, clearRecord.PlayerID)
}
