package postgres

import (
	"context"
	"fmt"

	"github.com/mat-sik/saga-go/examples/internal/adapters/sagaadapters"
	"github.com/mat-sik/saga-go/examples/internal/txctx"
	"github.com/mat-sik/saga-go/saga"
)

type AlarmConsumerRepository struct {
	sagaConsumerRepository
}

func NewAlarmConsumerRepository() AlarmConsumerRepository {
	return AlarmConsumerRepository{
		sagaConsumerRepository: sagaConsumerRepository{},
	}
}

func (r AlarmConsumerRepository) CommandAlreadyHandled(
	ctx context.Context,
	command saga.Command[sagaadapters.RaiseAlarmSagaCommand, sagaadapters.ClearAlarmSagaCommand],
) (bool, error) {
	tx, err := txctx.FromContext(ctx)
	if err != nil {
		return false, fmt.Errorf("extracting tx from ctx in consumer repository: %w", err)
	}

	alarmID, err := extractAlarmID(command)
	if err != nil {
		return false, err
	}

	return r.commandAlreadyHandled(ctx, tx, alarmID)
}

func extractAlarmID(command saga.Command[sagaadapters.RaiseAlarmSagaCommand, sagaadapters.ClearAlarmSagaCommand]) (string, error) {
	alarmID, _, err := extractAlarmIDs(command)
	return alarmID, err
}

func (r AlarmConsumerRepository) MarkCommandAsHandled(
	ctx context.Context,
	command saga.Command[sagaadapters.RaiseAlarmSagaCommand, sagaadapters.ClearAlarmSagaCommand],
) error {
	tx, err := txctx.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("extracting tx from ctx in consumer repository: %w", err)
	}

	alarmID, compensatedAlarmID, err := extractAlarmIDs(command)
	if err != nil {
		return err
	}

	return r.markCommandAsHandled(ctx, tx, alarmID, compensatedAlarmID)
}

func extractAlarmIDs(command saga.Command[sagaadapters.RaiseAlarmSagaCommand, sagaadapters.ClearAlarmSagaCommand]) (string, *string, error) {
	if transaction, ok := command.ToTransaction(); ok {
		return transaction.ID, nil, nil
	}
	if compensatingTransaction, ok := command.ToCompensatingTransaction(); ok {
		return compensatingTransaction.ID, &compensatingTransaction.PlayerID, nil
	}
	return "", nil, fmt.Errorf("command %v is not transaction nor compensating transaction", command)
}

func (r AlarmConsumerRepository) TransactionCompensated(ctx context.Context, cmd sagaadapters.RaiseAlarmSagaCommand) (bool, error) {
	tx, err := txctx.FromContext(ctx)
	if err != nil {
		return false, fmt.Errorf("extracting tx from ctx in consumer repository: %w", err)
	}

	return r.transactionCompensated(ctx, tx, cmd.PlayerID)
}
