package kgoconsumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

func Run(ctx context.Context, consumerCount int, newConsumer func() (Consumer, error)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, consumerCount)

	for range consumerCount {
		wg.Go(func() {
			consumer, err := newConsumer()
			if err != nil {
				errCh <- fmt.Errorf("creating consumer: %w", err)
				return
			}
			defer consumer.Close()
			err = consumer.StartPolling(ctx)
			errCh <- fmt.Errorf("polling: %w", err)
		})
	}

	var errs []error
	for range consumerCount {
		err := <-errCh
		cancel()
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}
