package queue

import (
	"sort"
	"sync"
	"time"

	"github.com/jadhavdurvesh/ORBIT/internal/model"
)

type Memory struct {
	mu   sync.RWMutex
	jobs map[string]model.Job
}

func NewMemory() *Memory {
	return &Memory{jobs: make(map[string]model.Job)}
}

func (m *Memory) Enqueue(job model.Job) error {
	if err := job.Validate(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.jobs[job.ID]; exists {
		return ErrConflict
	}
	m.jobs[job.ID] = job
	return nil
}

func (m *Memory) Get(id string) (model.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	job, ok := m.jobs[id]
	if !ok {
		return model.Job{}, ErrNotFound
	}
	return job, nil
}

func (m *Memory) List() []model.Job {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]model.Job, 0, len(m.jobs))
	for _, job := range m.jobs {
		out = append(out, job)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out
}

func (m *Memory) Claim() (model.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var selected *model.Job
	for _, job := range m.jobs {
		if job.State != model.StateQueued {
			continue
		}
		if selected == nil || job.CreatedAt.Before(selected.CreatedAt) ||
			(job.CreatedAt.Equal(selected.CreatedAt) && job.ID < selected.ID) {
			copy := job
			selected = &copy
		}
	}
	if selected == nil {
		return model.Job{}, ErrEmpty
	}
	if err := selected.Transition(model.StateRunning, time.Now().UTC()); err != nil {
		return model.Job{}, err
	}
	m.jobs[selected.ID] = *selected
	return *selected, nil
}

func (m *Memory) Update(job model.Job) error {
	if err := job.Validate(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.jobs[job.ID]; !ok {
		return ErrNotFound
	}
	m.jobs[job.ID] = job
	return nil
}
