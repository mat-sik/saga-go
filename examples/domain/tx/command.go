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

type UnregisterCommand struct {
	RegisterCommand RegisterCommand
	Time            time.Time
}
