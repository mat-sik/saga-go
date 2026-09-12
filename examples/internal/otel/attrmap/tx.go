package attrmap

import (
	"time"

	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"go.opentelemetry.io/otel/attribute"
)

func TxRegister(cmd tx.RegisterCommand) []attribute.KeyValue {
	return registerAttributes(cmd)
}

func TxUnregister(cmd tx.UnregisterCommand) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("command.tx.unregister.id", cmd.ID),
		attribute.String("command.tx.unregister.time", cmd.Time.Format(time.RFC3339Nano)),
	}
	return append(attrs, registerAttributes(cmd.RegisterCommand)...)
}

func registerAttributes(cmd tx.RegisterCommand) []attribute.KeyValue {
	id := cmd.RegisterID.ID

	return []attribute.KeyValue{
		attribute.String("command.tx.register.id", id.TransactionID),
		attribute.String("command.tx.register.player.id", id.PlayerID),
		attribute.String("command.tx.register.currency", id.Currency),
		attribute.String("command.tx.register.time", cmd.RegisterID.Time.Format(time.RFC3339Nano)),
		attribute.Int("command.tx.register.value", cmd.Value),
	}
}
