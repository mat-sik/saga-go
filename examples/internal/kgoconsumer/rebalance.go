package kgoconsumer

import (
	"context"
	"sync"
)

type cancelProcessingStore struct {
	mu         sync.Mutex
	cancelFunc context.CancelFunc
}

func newCancelProcessingStore() *cancelProcessingStore {
	return &cancelProcessingStore{
		cancelFunc: func() {},
	}
}

func (s *cancelProcessingStore) store(cancel context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancelFunc = cancel
}

func (s *cancelProcessingStore) cancel() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
}
