package postgres

import (
	"errors"
	"io"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mat-sik/saga-go/examples/internal/kgoconsumer"
)

func wrapIfTransient(err error) error {
	if err == nil || !isTransient(err) {
		return err
	}
	return errors.Join(err, kgoconsumer.ErrTransient)
}

func isTransient(err error) bool {
	switch {
	case errors.Is(err, pgconn.ErrConnClosed):
		return true
	case isOperatorIntervention(err):
		return true
	case isConnectError(err):
		return true
	case isNetworkFailure(err):
		return true
	}
	return false
}

func isOperatorIntervention(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		operatorInterventionClass := "57"
		return strings.HasPrefix(pgErr.Code, operatorInterventionClass)
	}
	return false
}

func isConnectError(err error) bool {
	var connErr *pgconn.ConnectError
	return errors.As(err, &connErr)
}

func isNetworkFailure(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}
