package migrations

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/pressly/goose/v3"
)

func Run(pool *pgxpool.Pool) error {
	return run(pool, "db-schema/migrations")
}

func run(pool *pgxpool.Pool, dir string) (err error) {
	wrappedPool := stdlib.OpenDBFromPool(pool)
	defer func() {
		closeErr := wrappedPool.Close()
		if closeErr != nil {
			closeErr = fmt.Errorf("closing goose wrapped pool: %w", closeErr)
		}
		err = errors.Join(err, closeErr)
	}()

	if err = goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("setting goose dialect: %w", err)
	}

	return goose.Up(wrappedPool, dir)
}
