package tx

import (
	"time"
)

type ID struct {
	TransactionID string
	PlayerID      string
	Currency      string
}

type RegisterID struct {
	ID   ID
	Time time.Time
}

type RegisterCommand struct {
	RegisterID RegisterID
	Value      int
}

func NewRegisterCommand(transactionID, playerID, currency string, time time.Time, value int) RegisterCommand {
	return RegisterCommand{
		RegisterID: RegisterID{
			ID: ID{
				TransactionID: transactionID,
				PlayerID:      playerID,
				Currency:      currency,
			},
			Time: time,
		},
		Value: value,
	}
}

func (r RegisterCommand) ToTransaction() (RegisterCommand, bool) {
	return r, true
}

func (r RegisterCommand) ToCompensatingTransaction() (UnregisterCommand, bool) {
	return UnregisterCommand{}, false
}

type UnregisterCommand struct {
	ID              string
	RegisterCommand RegisterCommand
	Time            time.Time
}

func NewUnregisterCommand(transactionID string, time time.Time, registerCommand RegisterCommand) UnregisterCommand {
	return UnregisterCommand{
		ID:              transactionID,
		RegisterCommand: registerCommand,
		Time:            time,
	}
}

func (u UnregisterCommand) ToTransaction() (RegisterCommand, bool) {
	return RegisterCommand{}, false
}

func (u UnregisterCommand) ToCompensatingTransaction() (UnregisterCommand, bool) {
	return u, true
}
