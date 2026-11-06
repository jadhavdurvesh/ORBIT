package scheduler

import (
	"sort"
	"sync"
	"time"
)

type Worker struct {
	ID string `json:"id"`
	Capacity int `json:"capacity"`
	Labels []string `json:"labels,omitempty"`
	LastSeen time.Time `json:"last_seen"`
}

type Registry struct {
	mu sync.Mutex
	workers map[string]Worker
	order []string
	next int
	ttl time.Duration
}

func NewRegistry(ttl time.Duration) *Registry {
	return &Registry{workers: make(map[string]Worker), ttl: ttl}
}

func (r *Registry) Register(w Worker) Worker {
	if w.Capacity < 1 { w.Capacity = 1 }
	r.mu.Lock(); defer r.mu.Unlock()
	if _, exists := r.workers[w.ID]; !exists { r.order = append(r.order, w.ID) }
	w.LastSeen = time.Now().UTC(); r.workers[w.ID] = w
	return w
}

func (r *Registry) Heartbeat(id string) (Worker, bool) {
	r.mu.Lock(); defer r.mu.Unlock()
	w, ok := r.workers[id]; if !ok { return Worker{}, false }
	w.LastSeen = time.Now().UTC(); r.workers[id] = w
	return w, true
}

func (r *Registry) Prune(now time.Time) int {
	r.mu.Lock(); defer r.mu.Unlock()
	removed := 0; kept := r.order[:0]
	for _, id := range r.order {
		w, ok := r.workers[id]
		if !ok { continue }
		if r.ttl > 0 && now.Sub(w.LastSeen) > r.ttl { delete(r.workers,id); removed++; continue }
		kept = append(kept,id)
	}
	r.order = kept
	if len(r.order)==0 { r.next=0 } else { r.next %= len(r.order) }
	return removed
}

func (r *Registry) Next() (Worker, bool) {
	r.mu.Lock(); defer r.mu.Unlock()
	if len(r.order)==0 { return Worker{}, false }
	for i:=0; i<len(r.order); i++ {
		id := r.order[r.next%len(r.order)]; r.next=(r.next+1)%len(r.order)
		if w,ok:=r.workers[id]; ok { return w,true }
	}
	return Worker{},false
}

func (r *Registry) List() []Worker {
	r.mu.Lock(); defer r.mu.Unlock()
	out:=make([]Worker,0,len(r.workers))
	for _,w:=range r.workers { out=append(out,w) }
	sort.Slice(out,func(i,j int)bool{return out[i].ID<out[j].ID})
	return out
}
