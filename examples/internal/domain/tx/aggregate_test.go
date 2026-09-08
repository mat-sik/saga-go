package tx

import (
	"context"
	"testing"

	"github.com/mat-sik/saga-go/examples/internal/domain/alarm"
)

type testCmd struct {
	register   *RegisterCommand
	unregister *UnregisterCommand
}

func TestAggregateSagaAction(t *testing.T) {
	tests := []struct {
		name      string
		initState initState
		cmd       []testCmd
		wantState endState
	}{
		{
			name: "alarm not raised",
			initState: initState{
				aggregatedValue: 0,
				alarmValue:      100,
				alarmRaised:     false,
			},
			cmd: []testCmd{
				{register: &RegisterCommand{Value: 50}},
				{register: &RegisterCommand{Value: 30}},
			},
			wantState: endState{
				aggregatedValue: 80,
				alarmRaised:     false,
			},
		},
		{
			name: "alarm raised",
			initState: initState{
				aggregatedValue: 20,
				alarmValue:      100,
				alarmRaised:     false,
			},
			cmd: []testCmd{
				{register: &RegisterCommand{Value: 50}},
				{register: &RegisterCommand{Value: 30}},
			},
			wantState: endState{
				aggregatedValue: 100,
				alarmRaised:     true,
			},
		},
		{
			name: "alarm raised and cleared",
			initState: initState{
				aggregatedValue: 0,
				alarmValue:      100,
				alarmRaised:     false,
			},
			cmd: []testCmd{
				{register: &RegisterCommand{Value: 100}},
				{unregister: &UnregisterCommand{
					RegisterCommand: RegisterCommand{Value: 100},
				}},
			},
			wantState: endState{
				aggregatedValue: 0,
				alarmRaised:     false,
			},
		},
		{
			name: "init alarm raised, alarm not cleared",
			initState: initState{
				aggregatedValue: 100,
				alarmValue:      100,
				alarmRaised:     true,
			},
			cmd: []testCmd{
				{register: &RegisterCommand{Value: 100}},
				{unregister: &UnregisterCommand{
					RegisterCommand: RegisterCommand{Value: 100},
				}},
			},
			wantState: endState{
				aggregatedValue: 100,
				alarmRaised:     true,
			},
		},
		{
			name: "alarm raised and not cleared",
			initState: initState{
				aggregatedValue: 100,
				alarmValue:      100,
				alarmRaised:     true,
			},
			cmd: []testCmd{
				{register: &RegisterCommand{Value: 100}},
				{register: &RegisterCommand{Value: 50}},
				{unregister: &UnregisterCommand{
					RegisterCommand: RegisterCommand{Value: 50},
				}},
			},
			wantState: endState{
				aggregatedValue: 200,
				alarmRaised:     true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggregatePortOut, alarmValueProviderPortOut, alarmRaiserPortOut := writeInitState(tt.initState)

			aggregateSagaAction := NewAggregateSagaAction(
				&aggregatePortOut,
				alarm.NewAlarmValueProvider(alarmValueProviderPortOut),
				alarm.NewAlarmRaiser(&alarmRaiserPortOut),
			)

			for _, cmd := range tt.cmd {
				switch {
				case cmd.register != nil:
					_ = aggregateSagaAction.Register(context.Background(), *cmd.register)
				case cmd.unregister != nil:
					_ = aggregateSagaAction.Unregister(context.Background(), *cmd.unregister)
				default:
					t.Fatalf("unsupported cmd: %v", cmd)
				}
			}

			gotState := readEndState(aggregatePortOut, alarmRaiserPortOut)

			if gotState != tt.wantState {
				t.Errorf("got: %v, want: %v", gotState, tt.wantState)
			}
		})
	}
}

func writeInitState(state initState) (testAggregatePortOut, testAlarmValueProviderPortOut, testAlarmRaiserPortOut) {
	aggregatePortOut := testAggregatePortOut{
		aggregatedValue: state.aggregatedValue,
	}

	alarmValueProviderPortOut := testAlarmValueProviderPortOut{
		threshold: state.alarmValue,
	}

	alarmRaiserPortOut := testAlarmRaiserPortOut{
		alarmRaised: state.alarmRaised,
	}

	return aggregatePortOut, alarmValueProviderPortOut, alarmRaiserPortOut
}

type initState struct {
	aggregatedValue int
	alarmValue      int
	alarmRaised     bool
}

func readEndState(
	aggregatePortOut testAggregatePortOut,
	alarmRaiserPortOut testAlarmRaiserPortOut,
) endState {
	return endState{
		aggregatedValue: aggregatePortOut.aggregatedValue,
		alarmRaised:     alarmRaiserPortOut.alarmRaised,
	}
}

type endState struct {
	aggregatedValue int
	alarmRaised     bool
}

type testAggregatePortOut struct {
	aggregatedValue int
}

func (t *testAggregatePortOut) Upsert(_ context.Context, _ RegisterID, value int) (int, error) {
	t.aggregatedValue = t.aggregatedValue + value
	return t.aggregatedValue, nil
}

func (t *testAggregatePortOut) Subtract(_ context.Context, _ RegisterID, value int) (int, error) {
	t.aggregatedValue = t.aggregatedValue - value
	return t.aggregatedValue, nil
}

type testAlarmRaiserPortOut struct {
	alarmRaised bool
}

func (t *testAlarmRaiserPortOut) RaiseAlarm(context.Context, alarm.RaiseAlarmCommand) error {
	t.alarmRaised = true
	return nil
}

func (t *testAlarmRaiserPortOut) ClearAlarm(context.Context, alarm.ClearAlarmCommand) error {
	t.alarmRaised = false
	return nil
}

type testAlarmValueProviderPortOut struct {
	threshold int
}

func (t testAlarmValueProviderPortOut) AlarmValue(context.Context, string) (int, error) {
	return t.threshold, nil
}
