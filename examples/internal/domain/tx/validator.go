package tx

import (
	"context"
	"fmt"
)

type ValidatorPortOut interface {
	Validate(ctx context.Context, registerCommand RegisterCommand) (bool, error)
	Compensate(ctx context.Context, registerCommand RegisterCommand) error
}

type Validator struct {
	portOut ValidatorPortOut
}

func NewValidator(portOut ValidatorPortOut) Validator {
	return Validator{
		portOut: portOut,
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

	if err = v.portOut.Compensate(ctx, registerCommand); err != nil {
		return fmt.Errorf("compensating %v: %w", registerCommand, err)
	}
	return nil
}
