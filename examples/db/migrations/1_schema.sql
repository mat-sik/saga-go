-- +goose Up
CREATE TABLE counts
(
    count BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE transactions_aggregates
(
    player_id  uuid        NOT NULL,
    currency   text        NOT NULL,
    day        date        NOT NULL,
    value      int         NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (player_id, currency, day)
);

CREATE TABLE transaction_logs
(
    transaction_id uuid        NOT NULL,
    player_id      uuid        NOT NULL,
    currency       text        NOT NULL,
    day            date        NOT NULL,
    value          int         NOT NULL,
    created_at     timestamptz NOT NULL,
    PRIMARY KEY (transaction_id),
    FOREIGN KEY (player_id, currency, day)
        REFERENCES transactions_aggregates (player_id, currency, day)
);

CREATE TABLE compensating_transaction_logs
(
    transaction_id             uuid        NOT NULL,
    compensated_transaction_id uuid        NOT NULL,
    created_at                 timestamptz NOT NULL,
    PRIMARY KEY (transaction_id)
);

CREATE INDEX idx_compensating_transaction_logs_compensated_transaction_id
    ON compensating_transaction_logs (compensated_transaction_id);

CREATE TABLE processed_transactions
(
    transaction_id             uuid NOT NULL,
    compensated_transaction_id uuid DEFAULT NULL,
    PRIMARY KEY (transaction_id)
)

CREATE INDEX idx_processed_transactions_compensated_transaction_id
    ON processed_transactions (compensated_transaction_id);

-- +goose Down
DROP TABLE compensating_transaction_logs;
DROP TABLE transaction_logs;
DROP TABLE transactions_aggregates;
DROP TABLE counts;
