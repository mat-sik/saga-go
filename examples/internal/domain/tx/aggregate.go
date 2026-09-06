package tx

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/mat-sik/saga-go/examples/internal/domain/alarm"
)

type AggregatePortOut interface {
	Upsert(ctx context.Context, id RegisterID, value int) (int, error)
	Subtract(ctx context.Context, id RegisterID, value int) (int, error)
}

type AggregateSagaAction struct {
	portOut            AggregatePortOut
	alarmValueProvider alarm.ValueProvider
	alarmRaiser        alarm.Raiser
}

func NewAggregateSagaAction(
	portOut AggregatePortOut,
	alarmValueProvider alarm.ValueProvider,
	alarmRaiser alarm.Raiser,
) AggregateSagaAction {
	return AggregateSagaAction{
		portOut:            portOut,
		alarmValueProvider: alarmValueProvider,
		alarmRaiser:        alarmRaiser,
	}
}

func (a AggregateSagaAction) Register(ctx context.Context, cmd RegisterCommand) error {
	updatedValue, err := a.portOut.Upsert(ctx, cmd.RegisterID, cmd.Value)
	if err != nil {
		return fmt.Errorf("upserting tx: %w", err)
	}

	playerID := cmd.RegisterID.ID.PlayerID
	if err = a.updateAlarmState(ctx, playerID, cmd.Value, updatedValue); err != nil {
		return err
	}
	return nil
}

func (a AggregateSagaAction) Unregister(ctx context.Context, cmd UnregisterCommand) error {
	regCmd := cmd.RegisterCommand
	updatedValue, err := a.portOut.Subtract(ctx, regCmd.RegisterID, regCmd.Value)
	if err != nil {
		return fmt.Errorf("subtracting from tx: %w", err)
	}

	playerID := regCmd.RegisterID.ID.PlayerID
	if err = a.updateAlarmState(ctx, playerID, -regCmd.Value, updatedValue); err != nil {
		return err
	}
	return nil
}

func (a AggregateSagaAction) updateAlarmState(ctx context.Context, playerID string, deltaValue, updatedValue int) error {
	alarmValue, err := a.alarmValueProvider.Provide(ctx, playerID)
	if err != nil {
		return err
	}

	previousValue := updatedValue - deltaValue
	switch crossing(previousValue, updatedValue, alarmValue) {
	case crossedAbove:
		id, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("generating raise alarm UUIDv7: %w", err)
		}
		return a.alarmRaiser.RaiseAlarm(ctx, alarm.NewRaiseAlarmCommand(id.String(), playerID, alarmValue, updatedValue))
	case crossedBelow:
		id, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("generating clear alarm UUIDv7: %w", err)
		}
		return a.alarmRaiser.ClearAlarm(ctx, alarm.NewClearAlarmCommand(id.String(), playerID))
	default:
		return nil
	}
}

func crossing(previousValue, updatedValue, threshold int) alarmCrossing {
	wasAbove := previousValue >= threshold
	isAbove := updatedValue >= threshold

	switch {
	case !wasAbove && isAbove:
		return crossedAbove
	case wasAbove && !isAbove:
		return crossedBelow
	default:
		return noCrossing
	}
}

type alarmCrossing int

const (
	noCrossing alarmCrossing = iota
	crossedAbove
	crossedBelow
)
