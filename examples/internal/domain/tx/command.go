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
