package main

import (
	"context"
	"errors"
	"log/slog"
	"net/smtp"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mat-sik/saga-go/examples/internal/adapters/kafka"
	"github.com/mat-sik/saga-go/examples/internal/adapters/mail"
	"github.com/mat-sik/saga-go/examples/internal/adapters/postgres"
	"github.com/mat-sik/saga-go/examples/internal/adapters/sagaadapters"
	"github.com/mat-sik/saga-go/examples/internal/config"
	"github.com/mat-sik/saga-go/examples/internal/domain/alarm"
	"github.com/mat-sik/saga-go/examples/internal/kgoconsumer"
	"github.com/mat-sik/saga-go/examples/internal/migrations"
	"github.com/mat-sik/saga-go/examples/internal/otelinit"
	"github.com/mat-sik/saga-go/saga"
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	conf, err := config.NewMailSender(ctx)
	if err != nil {
		slog.Error("reading mail-sender config", "err", err)
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

	pool, err := pgxpool.New(ctx, conf.DatabaseURL)
	if err != nil {
		slog.Error("creating pgx pool", "err", err)
		return 1
	}
	defer pool.Close()

	if err = migrations.RunSagaConsumer(pool); err != nil {
		slog.Error("running mail-sender migrations", "err", err)
		return 1
	}

	consumerFactory := func() (kgoconsumer.Consumer, error) {
		return newAlarmSagaConsumer(conf, pool)
	}

	if err := kgoconsumer.Run(ctx, conf.ConsumerCount, consumerFactory); err != nil {
		slog.Error("consumer", "err", errors.Join(err))
		return 1
	}

	return 0
}

func newAlarmSagaConsumer(conf config.MailSender, pool *pgxpool.Pool) (kgoconsumer.Consumer, error) {
	kafkaClient, err := kgoconsumer.NewClient(
		conf.KafkaSeeds,
		conf.AlarmsTopicConsumerGroup,
		[]string{conf.AlarmsTopic},
	)
	if err != nil {
		return kgoconsumer.Consumer{}, err
	}

	var auth smtp.Auth
	if !isDev(conf) {
		auth = smtp.PlainAuth("", conf.SMTPUsername, conf.SMTPPassword, conf.SMTPHost)
	}
	mailSender := mail.NewAlarmMailSender(auth, conf.SMTPAddr(), conf.MailFrom, conf.MailTo)
	alarmRaiser := alarm.NewAlarmRaiser(mailSender)

	actions := []saga.Action[sagaadapters.RaiseAlarmSagaCommand, sagaadapters.ClearAlarmSagaCommand]{
		sagaadapters.NewAlarmAction(alarmRaiser),
	}

	sagaConsumer := saga.NewConsumer(actions, postgres.NewAlarmConsumerRepository())

	txRunner := func(ctx context.Context, fn func(context.Context) error) error {
		return postgres.WithTx(ctx, pool, fn)
	}

	return kafka.NewAlarmSagaConsumer(kafkaClient, txRunner, conf.AlarmsDLQTopic, sagaConsumer)
}

func isDev(conf config.MailSender) bool {
	return conf.SMTPUsername == ""
}
