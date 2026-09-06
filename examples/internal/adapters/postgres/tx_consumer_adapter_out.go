package postgres

import (
	"context"
	"fmt"

	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"github.com/mat-sik/saga-go/examples/internal/txctx"
)

type TxConsumerRepository struct {
	idempotentConsumerRepository
}

func NewTxConsumerRepository() TxConsumerRepository {
	return TxConsumerRepository{
		idempotentConsumerRepository: idempotentConsumerRepository{},
	}
}

func (r TxConsumerRepository) MessageAlreadyHandled(ctx context.Context, command tx.RegisterCommand) (bool, error) {
	dbTx, err := txctx.FromContext(ctx)
	if err != nil {
		return false, fmt.Errorf("extracting tx from ctx in consumer repository: %w", err)
	}

	transactionID := command.RegisterID.ID.TransactionID
	return r.commandAlreadyHandled(ctx, dbTx, transactionID)
}

func (r TxConsumerRepository) MarkMessageAsHandled(ctx context.Context, command tx.RegisterCommand) error {
	dbTx, err := txctx.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("extracting tx from ctx in consumer repository: %w", err)
	}

	transactionID := command.RegisterID.ID.TransactionID
	return r.markCommandAsHandled(ctx, dbTx, transactionID)
}
