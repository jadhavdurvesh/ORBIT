package queue

import (
	"sort"
	"sync"
	"time"

	"github.com/jadhavdurvesh/ORBIT/internal/model"
)

type Memory struct {
	mu sync.RWMutex
	jobs map[string]model.Job
}

func NewMemory() *Memory { return &Memory{jobs: make(map[string]model.Job)} }

func (m *Memory) Enqueue(job model.Job) error {
	if err := job.Validate(); err != nil { return err }
	m.mu.Lock(); defer m.mu.Unlock()
	if _, exists := m.jobs[job.ID]; exists { return ErrConflict }
	m.jobs[job.ID] = job
	return nil
}

func (m *Memory) Get(id string) (model.Job, error) {
	m.mu.RLock(); defer m.mu.RUnlock()
	job, ok := m.jobs[id]; if !ok { return model.Job{}, ErrNotFound }; return job, nil
}

func (m *Memory) List() []model.Job {
	m.mu.RLock(); defer m.mu.RUnlock()
	out := make([]model.Job, 0, len(m.jobs))
	for _, job := range m.jobs { out = append(out, job) }
	sort.Slice(out, func(i,j int) bool { if out[i].CreatedAt.Equal(out[j].CreatedAt) { return out[i].ID < out[j].ID }; return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func (m *Memory) Claim(workerID string, lease time.Duration) (model.Job, error) {
	if workerID == "" || lease <= 0 { return model.Job{}, model.ErrInvalidJob }
	m.mu.Lock(); defer m.mu.Unlock()
	var selected *model.Job
	for _, job := range m.jobs {
		if job.State != model.StateQueued { continue }
		if selected == nil || job.CreatedAt.Before(selected.CreatedAt) || (job.CreatedAt.Equal(selected.CreatedAt) && job.ID < selected.ID) {
			copy := job; selected = &copy
		}
	}
	if selected == nil { return model.Job{}, ErrEmpty }
	now := time.Now().UTC()
	if err := selected.Transition(model.StateRunning, now); err != nil { return model.Job{}, err }
	selected.Attempts++
	selected.WorkerID = workerID
	expires := now.Add(lease); selected.LeaseExpiresAt = &expires
	if err := selected.Validate(); err != nil { return model.Job{}, err }
	m.jobs[selected.ID] = *selected
	return *selected, nil
}

func (m *Memory) Complete(id, workerID string, success bool, message string) (model.Job, error) {
	m.mu.Lock(); defer m.mu.Unlock()
	job, ok := m.jobs[id]; if !ok { return model.Job{}, ErrNotFound }
	if job.State != model.StateRunning || job.WorkerID != workerID { return model.Job{}, ErrNotOwner }
	now := time.Now().UTC()
	job.LastError = ""
	if success {
		if err := job.Transition(model.StateSucceeded, now); err != nil { return model.Job{}, err }
	} else {
		job.LastError = message
		if job.Attempts < job.MaxAttempts {
			if err := job.Transition(model.StateQueued, now); err != nil { return model.Job{}, err }
		} else {
			if err := job.Transition(model.StateFailed, now); err != nil { return model.Job{}, err }
		}
	}
	m.jobs[id] = job
	return job, nil
}

func (m *Memory) RecoverExpired(now time.Time) int {
	m.mu.Lock(); defer m.mu.Unlock()
	recovered := 0
	for id, job := range m.jobs {
		if job.State != model.StateRunning || job.LeaseExpiresAt == nil || job.LeaseExpiresAt.After(now) { continue }
		job.LastError = "worker lease expired"
		if job.Attempts < job.MaxAttempts { _ = job.Transition(model.StateQueued, now) } else { _ = job.Transition(model.StateFailed, now) }
		m.jobs[id] = job; recovered++
	}
	return recovered
}
