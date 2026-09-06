-- +goose Up
CREATE TABLE processed_transactions
(
    transaction_id             uuid NOT NULL,
    PRIMARY KEY (transaction_id)
);

-- +goose Down
DROP TABLE processed_transactions;
