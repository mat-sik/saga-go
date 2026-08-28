package mail

import (
	"context"
	"fmt"
	"net/smtp"
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

func (a AlarmMailSender) RaiseAlarm(_ context.Context, playerID string, alarmValue, value int) error {
	msg := raiseAlarmMessage(playerID, alarmValue, value)
	return a.send(msg)
}

func raiseAlarmMessage(playerID string, alarmValue, value int) string {
	messageFormat := "Subject: Alarm for player %s %d/%d\r\n\r\n" +
		"alarm value: %d\r\nvalue: %d\r\n"

	return fmt.Sprintf(messageFormat, playerID, alarmValue, value, alarmValue, value)
}

func (a AlarmMailSender) ClearAlarm(_ context.Context, playerID string) error {
	msg := clearAlarmMessage(playerID)
	return a.send(msg)
}

func clearAlarmMessage(playerID string) string {
	messageFormat := "Subject: Cleared alarm for player %s\r\n\r\n"
	return fmt.Sprintf(messageFormat, playerID)
}

func (a AlarmMailSender) send(msg string) error {
	return smtp.SendMail(a.addr, a.auth, a.from, []string{a.to}, []byte(msg))
}
