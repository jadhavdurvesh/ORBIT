package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jadhavdurvesh/ORBIT/internal/model"
	"github.com/jadhavdurvesh/ORBIT/internal/queue"
	"github.com/jadhavdurvesh/ORBIT/internal/scheduler"
)

type Server struct {
	jobs      queue.Store
	scheduler scheduler.Scheduler
	sequence  atomic.Uint64
}

func NewServer(jobs queue.Store, placement scheduler.Scheduler) *Server {
	return &Server{jobs: jobs, scheduler: placement}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "application/json")
	switch {
	case r.URL.Path == "/healthz" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case r.URL.Path == "/v1/jobs" && r.Method == http.MethodPost:
		s.submit(w, r)
	case r.URL.Path == "/v1/jobs" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, s.jobs.List())
	case strings.HasPrefix(r.URL.Path, "/v1/jobs/") && r.Method == http.MethodGet:
		s.inspect(w, strings.TrimPrefix(r.URL.Path, "/v1/jobs/"))
	case r.URL.Path == "/v1/dispatch" && r.Method == http.MethodPost:
		s.dispatch(w)
	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "route not found"})
	}
}

type submitRequest struct {
	Kind    string            `json:"kind"`
	Payload map[string]string `json:"payload"`
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var request submitRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	now := time.Now().UTC()
	job := model.Job{
		ID: fmt.Sprintf("job-%08d", s.sequence.Add(1)), Kind: request.Kind,
		Payload: request.Payload, State: model.StateQueued, CreatedAt: now, UpdatedAt: now,
	}
	if err := job.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.jobs.Enqueue(job); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) inspect(w http.ResponseWriter, id string) {
	if id == "" || strings.Contains(id, "/") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid job id"})
		return
	}
	job, err := s.jobs.Get(id)
	if errors.Is(err, queue.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) dispatch(w http.ResponseWriter) {
	job, err := s.jobs.Claim()
	if errors.Is(err, queue.ErrEmpty) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "queue is empty"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	worker, ok := s.scheduler.Next()
	if !ok {
		_ = job.Transition(model.StateQueued, time.Now().UTC())
		_ = s.jobs.Update(job)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no workers registered"})
		return
	}
	job.WorkerID = worker.ID
	job.UpdatedAt = time.Now().UTC()
	if err := s.jobs.Update(job); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job, "worker": worker})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
