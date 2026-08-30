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
	msg := raiseAlarmMessage(cmd.PlayerID, cmd.AlarmValue, cmd.Value)
	return a.send(msg)
}

func raiseAlarmMessage(playerID string, alarmValue, value int) string {
	messageFormat := "Subject: Alarm for player %s %d/%d\r\n\r\n" +
		"alarm value: %d\r\nvalue: %d\r\n"

	return fmt.Sprintf(messageFormat, playerID, alarmValue, value, alarmValue, value)
}

func (a AlarmMailSender) ClearAlarm(_ context.Context, cmd alarm.ClearAlarmCommand) error {
	msg := clearAlarmMessage(cmd.PlayerID)
	return a.send(msg)
}

func clearAlarmMessage(playerID string) string {
	messageFormat := "Subject: Cleared alarm for player %s\r\n\r\n"
	return fmt.Sprintf(messageFormat, playerID)
}

func (a AlarmMailSender) send(msg string) error {
	return smtp.SendMail(a.addr, a.auth, a.from, []string{a.to}, []byte(msg))
}
