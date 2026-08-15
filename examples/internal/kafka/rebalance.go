package kafka

import (
	"context"
	"sync"
)

type cancelProcessingStore struct {
	mu     sync.Mutex
	cancel context.CancelFunc
}

func newRebalanceCancelStore() *cancelProcessingStore {
	return &cancelProcessingStore{
		mu:     sync.Mutex{},
		cancel: func() {},
	}
}

func (s *cancelProcessingStore) store(cancel context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancel = cancel
}

func (s *cancelProcessingStore) cancelStored() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
}
