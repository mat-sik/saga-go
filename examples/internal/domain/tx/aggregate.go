package tx

import (
	"context"
	"fmt"
)

type AggregateSagaAction struct {
	portOut            AggregatePortOut
	alarmValueProvider alarmValueProvider
	alarmRaiser        alarmRaiser
}

func NewAggregateSagaAction(
	portOut AggregatePortOut,
	alarmValueProvider alarmValueProvider,
	alarmRaiser alarmRaiser,
) AggregateSagaAction {
	return AggregateSagaAction{
		portOut:            portOut,
		alarmValueProvider: alarmValueProvider,
		alarmRaiser:        alarmRaiser,
	}
}

func (a AggregateSagaAction) Execute(ctx context.Context, cmd RegisterCommand) error {
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

func (a AggregateSagaAction) Compensate(ctx context.Context, cmd UnregisterCommand) error {
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
	alarmValue, err := a.alarmValueProvider.provide(ctx, playerID)
	if err != nil {
		return err
	}

	previousValue := updatedValue - deltaValue
	switch crossing(previousValue, updatedValue, alarmValue) {
	case crossedBelow:
		return a.alarmRaiser.ClearAlarm(ctx, playerID)
	case crossedAbove:
		return a.alarmRaiser.RaiseAlarm(ctx, playerID, alarmValue, updatedValue)
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
