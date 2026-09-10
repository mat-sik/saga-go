package sagaadapters

import (
	"context"

	"github.com/mat-sik/saga-go/examples/internal/domain/alarm"
)

type RaiseAlarmSagaCommand struct {
	alarm.RaiseAlarmCommand
}

func (r RaiseAlarmSagaCommand) ToTransaction() (RaiseAlarmSagaCommand, bool) {
	return r, true
}

func (r RaiseAlarmSagaCommand) ToCompensatingTransaction() (ClearAlarmSagaCommand, bool) {
	return ClearAlarmSagaCommand{}, false
}

type ClearAlarmSagaCommand struct {
	alarm.ClearAlarmCommand
}

func (c ClearAlarmSagaCommand) ToTransaction() (RaiseAlarmSagaCommand, bool) {
	return RaiseAlarmSagaCommand{}, false
}

func (c ClearAlarmSagaCommand) ToCompensatingTransaction() (ClearAlarmSagaCommand, bool) {
	return c, true
}

type AlarmSagaAction struct {
	alarmRaiser alarm.Raiser
}

func NewAlarmSagaAction(alarmRaiser alarm.Raiser) AlarmSagaAction {
	return AlarmSagaAction{
		alarmRaiser: alarmRaiser,
	}
}

func (a AlarmSagaAction) Execute(ctx context.Context, sagaCmd RaiseAlarmSagaCommand) error {
	return a.alarmRaiser.RaiseAlarm(ctx, sagaCmd.RaiseAlarmCommand)
}

func (a AlarmSagaAction) Compensate(ctx context.Context, sagaCmd ClearAlarmSagaCommand) error {
	return a.alarmRaiser.ClearAlarm(ctx, sagaCmd.ClearAlarmCommand)
}
