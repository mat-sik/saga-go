-- +goose Up
CREATE TABLE processed_transactions
(
    transaction_id             uuid NOT NULL,
    compensated_transaction_id uuid DEFAULT NULL,
    PRIMARY KEY (transaction_id)
);

CREATE INDEX idx_processed_transactions_compensated_transaction_id
    ON processed_transactions (compensated_transaction_id);

-- +goose Down
DROP TABLE processed_transactions;
