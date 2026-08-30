package postgres

import (
	"context"
	"fmt"

	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"github.com/mat-sik/saga-go/examples/internal/txctx"
	"github.com/mat-sik/saga-go/saga"
)

type TxConsumerRepository struct {
}

func NewTxConsumerRepository() TxConsumerRepository {
	return TxConsumerRepository{}
}

func (r TxConsumerRepository) CommandAlreadyHandled(
	ctx context.Context,
	command saga.Command[tx.RegisterCommand, tx.UnregisterCommand],
) (bool, error) {
	dbTx, err := txctx.FromContext(ctx)
	if err != nil {
		return false, fmt.Errorf("extracting tx from ctx in consumer repository: %w", err)
	}

	var transactionID string
	transactionID, err = extractTransactionID(command)
	if err != nil {
		return false, err
	}

	return commandAlreadyHandled(ctx, dbTx, transactionID)
}

func extractTransactionID(command saga.Command[tx.RegisterCommand, tx.UnregisterCommand]) (string, error) {
	transactionID, _, err := extractTransactionIDs(command)
	return transactionID, err
}

func (r TxConsumerRepository) MarkCommandAsHandled(
	ctx context.Context,
	command saga.Command[tx.RegisterCommand, tx.UnregisterCommand],
) error {
	dbTx, err := txctx.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("extracting tx from ctx in consumer repository: %w", err)
	}

	transactionID, compensatedTransactionID, err := extractTransactionIDs(command)
	if err != nil {
		return err
	}

	return markCommandAsHandled(ctx, dbTx, transactionID, compensatedTransactionID)
}

func extractTransactionIDs(command saga.Command[tx.RegisterCommand, tx.UnregisterCommand]) (string, *string, error) {
	if transaction, ok := command.ToTransaction(); ok {
		return transaction.RegisterID.ID.TransactionID, nil, nil
	}
	if compensatingTransaction, ok := command.ToCompensatingTransaction(); ok {
		return compensatingTransaction.ID, &compensatingTransaction.RegisterCommand.RegisterID.ID.TransactionID, nil
	}
	return "", nil, fmt.Errorf("command %v is not transaction nor compensating transaction", command)
}

func (r TxConsumerRepository) TransactionCompensated(ctx context.Context, transaction tx.RegisterCommand) (bool, error) {
	dbTx, err := txctx.FromContext(ctx)
	if err != nil {
		return false, fmt.Errorf("extracting tx from ctx in consumer repository: %w", err)
	}

	transactionID := transaction.RegisterID.ID.TransactionID

	return transactionCompensated(ctx, dbTx, transactionID)
}
