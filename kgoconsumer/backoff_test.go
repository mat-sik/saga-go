package kgoconsumer

import (
	"context"
	"testing"
	"time"
)

func TestBackoff_ClearResetsAttempt(t *testing.T) {
	b := newBackoff(10*time.Millisecond, 100*time.Millisecond, 2)

	ctx := context.Background()
	_ = b.wait(ctx)
	_ = b.wait(ctx)

	if b.attempt == 0 {
		t.Fatal("expected attempt to have incremented before clear")
	}

	b.clear()

	if b.attempt != 0 {
		t.Fatalf("expected attempt to reset to 0 after clear, got %d", b.attempt)
	}
}

func TestBackoff_WaitRespectsCancellation(t *testing.T) {
	b := newBackoff(10*time.Second, time.Minute, 2)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := b.wait(ctx)
	if err == nil {
		t.Fatal("expected wait to return an error when context is already cancelled")
	}
}

func TestBackoff_DelayNeverExceedsMax(t *testing.T) {
	maxDuration := 50 * time.Millisecond
	b := newBackoff(10*time.Millisecond, maxDuration, 2)

	for i := range 20 {
		d := b.sleepDuration()
		if d > maxDuration {
			t.Fatalf("attempt %d: sleepDuration %v exceeded max %v", i, d, maxDuration)
		}
		b.attempt++
	}
}
