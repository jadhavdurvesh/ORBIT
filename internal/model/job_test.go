package model

import (
	"errors"
	"testing"
	"time"
)

func TestTransitionLifecycle(t *testing.T) {
	job := Job{ID: "j1", Kind: "echo", State: StateQueued}
	now := time.Now()
	if err := job.Transition(StateRunning, now); err != nil {
		t.Fatal(err)
	}
	if err := job.Transition(StateSucceeded, now); err != nil {
		t.Fatal(err)
	}
	if err := job.Transition(StateQueued, now); !errors.Is(err, ErrInvalidMove) {
		t.Fatalf("expected invalid transition, got %v", err)
	}
}
