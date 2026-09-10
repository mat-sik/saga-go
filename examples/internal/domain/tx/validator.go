package tx

import (
	"context"
	"fmt"
)

type ValidatorPortOut interface {
	Validate(ctx context.Context, registerCommand RegisterCommand) (bool, error)
	Unregister(ctx context.Context, registerCommand RegisterCommand) error
}

type Validator struct {
	portOut  ValidatorPortOut
	observer ValidatorObserver
}

func NewValidator(portOut ValidatorPortOut, observer ValidatorObserver) Validator {
	return Validator{
		portOut:  portOut,
		observer: observer,
	}
}

func (v Validator) ValidateAndCompensate(ctx context.Context, registerCommand RegisterCommand) error {
	valid, err := v.portOut.Validate(ctx, registerCommand)
	if err != nil {
		return fmt.Errorf("validating %v: %w", registerCommand, err)
	}

	if valid {
		return nil
	}

	v.observer.Observe(ctx, ValidatorEventValidationUnsuccessful)

	if err = v.portOut.Unregister(ctx, registerCommand); err != nil {
		return fmt.Errorf("compensating %v: %w", registerCommand, err)
	}
	return nil
}

type ValidatorObserver interface {
	Observe(context.Context, ValidatorEvent)
}

type ValidatorEvent int

const (
	ValidatorEventValidationUnsuccessful ValidatorEvent = iota
)
