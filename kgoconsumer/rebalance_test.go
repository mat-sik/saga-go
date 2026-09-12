package kgoconsumer

import (
	"testing"
)

func TestCancelProcessingStore_StoreRelease(t *testing.T) {
	s := newCancelProcessingStore()

	cancelled := false
	s.store(func() {
		cancelled = true
	})

	s.release()

	if !cancelled {
		t.Fatal("expected pending cancel to fire as soon as store() is called")
	}
}

func TestCancelProcessingStore_StoreCancelRelease(t *testing.T) {
	s := newCancelProcessingStore()

	cancelled := false
	s.store(func() {
		cancelled = true
	})

	s.cancel()

	s.release()

	if !cancelled {
		t.Fatal("expected cancel to fire immediately when a batch is active")
	}
}

func TestCancelProcessingStore_CancelStoreRelease(t *testing.T) {
	s := newCancelProcessingStore()

	s.cancel()

	cancelled := false
	s.store(func() {
		cancelled = true
	})

	s.release()

	if !cancelled {
		t.Fatal("expected pending cancel to fire as soon as store() is called")
	}
}

func TestCancelProcessingStore_ReleaseClearsState(t *testing.T) {
	s := newCancelProcessingStore()

	firstCancelCalls := 0
	s.store(func() { firstCancelCalls++ })
	s.release()

	// A cancel() after release(), with no new store(), must be treated
	// as "nothing active" — i.e. remembered as pending, not misapplied
	// to the already-released batch.
	s.cancel()

	secondCancelCalls := 0
	s.store(func() { secondCancelCalls++ })

	if firstCancelCalls != 1 {
		t.Fatalf("release should have fired the first batch's cancel exactly once, got %d", firstCancelCalls)
	}
	if secondCancelCalls != 1 {
		t.Fatalf("pending cancel after release should carry over to the next store(), got %d", secondCancelCalls)
	}
}

func TestCancelProcessingStore_NoSpuriousCancelAfterNormalRelease(t *testing.T) {
	s := newCancelProcessingStore()

	s.store(func() {})
	s.cancel()  // external interrupt while active
	s.release() // normal end-of-batch cleanup runs afterward

	secondCancelCalls := 0
	s.store(func() { secondCancelCalls++ })

	if secondCancelCalls != 0 {
		t.Fatal("release() after an already-handled cancel() must not poison the next store()")
	}
}

func TestCancelProcessingStore_MultipleCancelsAreIdempotentWhenPending(t *testing.T) {
	s := newCancelProcessingStore()

	s.cancel()
	s.cancel() // calling cancel twice with nothing active shouldn't panic or double-fire

	calls := 0
	s.store(func() { calls++ })

	if calls != 1 {
		t.Fatalf("expected exactly one fire from a pending cancel, got %d", calls)
	}
}
