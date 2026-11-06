package model

import (
	"errors"
	"fmt"
	"time"
)

type JobState string

const (
	StateQueued    JobState = "queued"
	StateRunning   JobState = "running"
	StateSucceeded JobState = "succeeded"
	StateFailed    JobState = "failed"
)

var (
	ErrInvalidJob  = errors.New("invalid job")
	ErrInvalidMove = errors.New("invalid job state transition")
)

type Job struct {
	ID        string            `json:"id"`
	Kind      string            `json:"kind"`
	Payload   map[string]string `json:"payload"`
	State     JobState          `json:"state"`
	WorkerID  string            `json:"worker_id,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

func (j Job) Validate() error {
	if j.ID == "" {
		return fmt.Errorf("%w: id is required", ErrInvalidJob)
	}
	if j.Kind == "" {
		return fmt.Errorf("%w: kind is required", ErrInvalidJob)
	}
	switch j.State {
	case StateQueued, StateRunning, StateSucceeded, StateFailed:
	default:
		return fmt.Errorf("%w: unknown state %q", ErrInvalidJob, j.State)
	}
	return nil
}

func CanTransition(from, to JobState) bool {
	switch from {
	case StateQueued:
		return to == StateRunning || to == StateFailed
	case StateRunning:
		return to == StateSucceeded || to == StateFailed || to == StateQueued
	default:
		return false
	}
}

func (j *Job) Transition(to JobState, now time.Time) error {
	if !CanTransition(j.State, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidMove, j.State, to)
	}
	j.State = to
	j.UpdatedAt = now.UTC()
	if to == StateQueued {
		j.WorkerID = ""
	}
	return nil
}
