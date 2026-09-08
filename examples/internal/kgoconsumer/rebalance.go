package kgoconsumer

import (
	"context"
	"sync"
)

type cancelProcessingStore struct {
	mu           sync.Mutex
	cancelFunc   context.CancelFunc
	shouldCancel bool
}

func newCancelProcessingStore() *cancelProcessingStore {
	return &cancelProcessingStore{}
}

func (s *cancelProcessingStore) store(cancel context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cancelFunc = cancel
	if s.shouldCancel {
		s.cancelFunc()
	}
}

func (s *cancelProcessingStore) cancel() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cancelFunc != nil {
		s.cancelFunc()
	} else {
		s.shouldCancel = true
	}
}

func (s *cancelProcessingStore) release() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cancelFunc()
	s.cancelFunc = nil
	s.shouldCancel = false
}
