package model

import (
	"errors"
	"fmt"
	"time"
)

type JobState string
const(
	StateQueued JobState="queued"
	StateRunning JobState="running"
	StateSucceeded JobState="succeeded"
	StateFailed JobState="failed"
)
var(ErrInvalidJob=errors.New("invalid job");ErrInvalidMove=errors.New("invalid job state transition"))

type Job struct{
	ID string `json:"id"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	Kind string `json:"kind"`
	Payload map[string]string `json:"payload"`
	Result map[string]string `json:"result,omitempty"`
	State JobState `json:"state"`
	WorkerID string `json:"worker_id,omitempty"`
	LeaseToken string `json:"lease_token,omitempty"`
	Attempts int `json:"attempts"`
	MaxAttempts int `json:"max_attempts"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`
	RunAt *time.Time `json:"run_at,omitempty"`
	LastError string `json:"last_error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
func(j Job)Validate()error{
	if j.ID==""{return fmt.Errorf("%w: id is required",ErrInvalidJob)}
	if j.Kind==""{return fmt.Errorf("%w: kind is required",ErrInvalidJob)}
	if j.MaxAttempts<1{return fmt.Errorf("%w: max_attempts must be positive",ErrInvalidJob)}
	if j.Attempts<0||j.Attempts>j.MaxAttempts{return fmt.Errorf("%w: invalid attempts",ErrInvalidJob)}
	switch j.State{case StateQueued,StateRunning,StateSucceeded,StateFailed:default:return fmt.Errorf("%w: unknown state %q",ErrInvalidJob,j.State)}
	if j.State==StateRunning&&(j.WorkerID==""||j.LeaseToken==""||j.LeaseExpiresAt==nil){return fmt.Errorf("%w: running jobs require worker, lease token, and expiry",ErrInvalidJob)}
	return nil
}
func CanTransition(from,to JobState)bool{
	switch from{case StateQueued:return to==StateRunning||to==StateFailed;case StateRunning:return to==StateSucceeded||to==StateFailed||to==StateQueued;default:return false}
}
func(j *Job)Transition(to JobState,now time.Time)error{
	if !CanTransition(j.State,to){return fmt.Errorf("%w: %s -> %s",ErrInvalidMove,j.State,to)}
	j.State=to;j.UpdatedAt=now.UTC()
	if to!=StateRunning{j.WorkerID="";j.LeaseToken="";j.LeaseExpiresAt=nil}
	return nil
}
