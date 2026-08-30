package mail

import (
	"context"
	"fmt"
	"net/smtp"

	"github.com/mat-sik/saga-go/examples/internal/domain/alarm"
)

type AlarmMailSender struct {
	auth smtp.Auth
	addr string
	from string
	to   string
}

func NewAlarmMailSender(auth smtp.Auth, addr, from, to string) AlarmMailSender {
	return AlarmMailSender{
		auth: auth,
		addr: addr,
		from: from,
		to:   to,
	}
}

func (a AlarmMailSender) RaiseAlarm(_ context.Context, cmd alarm.RaiseAlarmCommand) error {
	msg := raiseAlarmMessage(a.from, a.to, cmd.PlayerID, cmd.AlarmValue, cmd.Value)
	return a.send(msg)
}

func raiseAlarmMessage(from, to, playerID string, alarmValue, value int) string {
	messageFormat := "From: %s\r\n" +
		"To: %s\r\n" +
		"Subject: Alarm for player %s %d/%d\r\n" +
		"\r\n" +
		"alarm value: %d\r\n" +
		"value: %d\r\n"

	return fmt.Sprintf(messageFormat, from, to, playerID, alarmValue, value, alarmValue, value)
}

func (a AlarmMailSender) ClearAlarm(_ context.Context, cmd alarm.ClearAlarmCommand) error {
	msg := clearAlarmMessage(a.from, a.to, cmd.PlayerID)
	return a.send(msg)
}

func clearAlarmMessage(from, to, playerID string) string {
	messageFormat := "From: %s\r\n" +
		"To: %s\r\n" +
		"Subject: Cleared alarm for player %s\r\n" +
		"\r\n" +
		"alarm cleared\r\n"

	return fmt.Sprintf(messageFormat, from, to, playerID)
}

func (a AlarmMailSender) send(msg string) error {
	return smtp.SendMail(a.addr, a.auth, a.from, []string{a.to}, []byte(msg))
}
