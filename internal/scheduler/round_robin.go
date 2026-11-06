package scheduler

import "sync"

type Worker struct {
	ID       string   `json:"id"`
	Capacity int      `json:"capacity"`
	Labels   []string `json:"labels,omitempty"`
}

type Scheduler interface {
	Register(Worker)
	Remove(string)
	Next() (Worker, bool)
}

type RoundRobin struct {
	mu      sync.Mutex
	workers []Worker
	next    int
}

func NewRoundRobin() *RoundRobin { return &RoundRobin{} }

func (r *RoundRobin) Register(worker Worker) {
	if worker.ID == "" {
		return
	}
	if worker.Capacity < 1 {
		worker.Capacity = 1
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.workers {
		if r.workers[i].ID == worker.ID {
			r.workers[i] = worker
			if r.next >= len(r.workers) {
				r.next = 0
			}
			return
		}
	}
	r.workers = append(r.workers, worker)
}

func (r *RoundRobin) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, worker := range r.workers {
		if worker.ID == id {
			r.workers = append(r.workers[:i], r.workers[i+1:]...)
			if len(r.workers) == 0 {
				r.next = 0
			} else if r.next >= len(r.workers) {
				r.next = 0
			}
			return
		}
	}
}

func (r *RoundRobin) Next() (Worker, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.workers) == 0 {
		return Worker{}, false
	}
	worker := r.workers[r.next%len(r.workers)]
	r.next = (r.next + 1) % len(r.workers)
	return worker, true
}
