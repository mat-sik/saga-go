package consumer

import (
	"context"
	"fmt"

	"github.com/mat-sik/saga-go/examples/internal/domain/tx"
	"github.com/mat-sik/saga-go/examples/internal/txctx"
	"github.com/mat-sik/saga-go/saga"
)

type Repository struct {
}

func (r Repository) CommandAlreadyHandled(
	ctx context.Context,
	command saga.Command[tx.RegisterCommand, tx.UnregisterCommand],
) (bool, error) {
	dbTx, err := txctx.FromContext(ctx)
	if err != nil {
		return false, fmt.Errorf("extracting tx from ctx in consumer repository: %w", err)
	}

	const query = `
    	SELECT EXISTS(SELECT 1 FROM processed_transactions WHERE transaction_id = $1)
	`

	var transactionID string
	transactionID, err = extractTransactionID(command)
	if err != nil {
		return false, err
	}

	var alreadyHandled bool
	if err = dbTx.QueryRow(ctx, query, transactionID).Scan(&alreadyHandled); err != nil {
		return false, fmt.Errorf("querying for already handled transaction '%s': %w", transactionID, err)
	}
	return alreadyHandled, nil
}

func extractTransactionID(command saga.Command[tx.RegisterCommand, tx.UnregisterCommand]) (string, error) {
	transactionID, _, err := extractIDs(command)
	return transactionID, err
}

func (r Repository) MarkCommandAsHandled(
	ctx context.Context,
	command saga.Command[tx.RegisterCommand, tx.UnregisterCommand],
) error {
	dbTx, err := txctx.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("extracting tx from ctx in consumer repository: %w", err)
	}

	const query = `
		INSERT INTO processed_transactions (transaction_id, compensated_transaction_id)
		VALUES ($1, $2)
	`

	var transactionID string
	var compensatedTransactionID *string
	transactionID, compensatedTransactionID, err = extractIDs(command)
	if err != nil {
		return err
	}

	if _, err = dbTx.Exec(ctx, query, transactionID, compensatedTransactionID); err != nil {
		return fmt.Errorf("marking command '%v' as handled: %w", command, err)
	}

	return nil
}

func extractIDs(command saga.Command[tx.RegisterCommand, tx.UnregisterCommand]) (string, *string, error) {
	if transaction, ok := command.ToTransaction(); ok {
		return transaction.RegisterID.ID.TransactionID, nil, nil
	}
	if compensatingTransaction, ok := command.ToCompensatingTransaction(); ok {
		return compensatingTransaction.ID, &compensatingTransaction.RegisterCommand.RegisterID.ID.TransactionID, nil
	}
	return "", nil, fmt.Errorf("command '%v' is not transaction nor compensating transaction", command)
}

func (r Repository) TransactionCompensated(ctx context.Context, transaction tx.RegisterCommand) (bool, error) {
	dbTx, err := txctx.FromContext(ctx)
	if err != nil {
		return false, fmt.Errorf("extracting tx from ctx in consumer repository: %w", err)
	}

	const query = `
    	SELECT EXISTS(SELECT 1 FROM processed_transactions WHERE compensated_transaction_id = $1)
	`

	transactionID := transaction.RegisterID.ID.TransactionID

	var alreadyCompensated bool
	if err = dbTx.QueryRow(ctx, query, transactionID).Scan(&alreadyCompensated); err != nil {
		return false, fmt.Errorf("querying for already compensated transaction '%s': %w", transactionID, err)
	}
	return alreadyCompensated, nil
}
