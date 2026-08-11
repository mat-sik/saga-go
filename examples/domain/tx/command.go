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

func (r RegisterCommand) Id() string {
	return r.RegisterID.ID.TransactionID
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

func (u UnregisterCommand) Id() string {
	return u.ID
}

func (u UnregisterCommand) ToTransaction() (RegisterCommand, bool) {
	return RegisterCommand{}, false
}

func (u UnregisterCommand) ToCompensatingTransaction() (UnregisterCommand, bool) {
	return u, true
}
