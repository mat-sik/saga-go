package postgres

import (
	"context"
	"fmt"

	"github.com/mat-sik/saga-go/examples/internal/adapters/sagaadapters"
	"github.com/mat-sik/saga-go/examples/internal/txctx"
	"github.com/mat-sik/saga-go/saga"
)

type TxSagaConsumerRepository struct {
	sagaConsumerRepository
}

func NewTxSagaConsumerRepository() TxSagaConsumerRepository {
	return TxSagaConsumerRepository{
		sagaConsumerRepository: sagaConsumerRepository{},
	}
}

func (r TxSagaConsumerRepository) CommandAlreadyHandled(
	ctx context.Context,
	command saga.Command[sagaadapters.RegisterSagaCommand, sagaadapters.UnregisterSagaCommand],
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

	return r.commandAlreadyHandled(ctx, dbTx, transactionID)
}

func extractTransactionID(command saga.Command[sagaadapters.RegisterSagaCommand, sagaadapters.UnregisterSagaCommand]) (string, error) {
	transactionID, _, err := extractTransactionIDs(command)
	return transactionID, err
}

func (r TxSagaConsumerRepository) MarkCommandAsHandled(
	ctx context.Context,
	command saga.Command[sagaadapters.RegisterSagaCommand, sagaadapters.UnregisterSagaCommand],
) error {
	dbTx, err := txctx.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("extracting tx from ctx in consumer repository: %w", err)
	}

	transactionID, compensatedTransactionID, err := extractTransactionIDs(command)
	if err != nil {
		return err
	}

	return r.markCommandAsHandled(ctx, dbTx, transactionID, compensatedTransactionID)
}

func extractTransactionIDs(command saga.Command[sagaadapters.RegisterSagaCommand, sagaadapters.UnregisterSagaCommand]) (string, *string, error) {
	if transaction, ok := command.ToTransaction(); ok {
		return transaction.RegisterID.ID.TransactionID, nil, nil
	}
	if compensatingTransaction, ok := command.ToCompensatingTransaction(); ok {
		return compensatingTransaction.ID, &compensatingTransaction.RegisterCommand.RegisterID.ID.TransactionID, nil
	}
	return "", nil, fmt.Errorf("command %v is not transaction nor compensating transaction", command)
}

func (r TxSagaConsumerRepository) TransactionCompensated(ctx context.Context, transaction sagaadapters.RegisterSagaCommand) (bool, error) {
	dbTx, err := txctx.FromContext(ctx)
	if err != nil {
		return false, fmt.Errorf("extracting tx from ctx in consumer repository: %w", err)
	}

	transactionID := transaction.RegisterID.ID.TransactionID

	return r.transactionCompensated(ctx, dbTx, transactionID)
}
