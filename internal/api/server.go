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
	jobs queue.Store
	workers *scheduler.Registry
	lease time.Duration
	sequence atomic.Uint64
}

func NewServer(jobs queue.Store, workers *scheduler.Registry, lease time.Duration) *Server {
	return &Server{jobs: jobs, workers: workers, lease: lease}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "application/json")
	switch {
	case r.URL.Path == "/healthz" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]string{"status":"ok"})
	case r.URL.Path == "/v1/jobs" && r.Method == http.MethodPost: s.submit(w,r)
	case r.URL.Path == "/v1/jobs" && r.Method == http.MethodGet: writeJSON(w,http.StatusOK,s.jobs.List())
	case r.URL.Path == "/v1/workers" && r.Method == http.MethodPost: s.registerWorker(w,r)
	case r.URL.Path == "/v1/workers" && r.Method == http.MethodGet: writeJSON(w,http.StatusOK,s.workers.List())
	case strings.HasPrefix(r.URL.Path,"/v1/workers/") && strings.HasSuffix(r.URL.Path,"/heartbeat") && r.Method == http.MethodPost:
		id:=strings.TrimSuffix(strings.TrimPrefix(r.URL.Path,"/v1/workers/"),"/heartbeat"); s.heartbeat(w,id)
	case strings.HasPrefix(r.URL.Path,"/v1/workers/") && strings.HasSuffix(r.URL.Path,"/claim") && r.Method == http.MethodPost:
		id:=strings.TrimSuffix(strings.TrimPrefix(r.URL.Path,"/v1/workers/"),"/claim"); s.claim(w,id)
	case strings.HasPrefix(r.URL.Path,"/v1/jobs/") && strings.HasSuffix(r.URL.Path,"/complete") && r.Method == http.MethodPost:
		id:=strings.TrimSuffix(strings.TrimPrefix(r.URL.Path,"/v1/jobs/"),"/complete"); s.complete(w,r,id)
	case strings.HasPrefix(r.URL.Path,"/v1/jobs/") && r.Method == http.MethodGet:
		s.inspect(w,strings.TrimPrefix(r.URL.Path,"/v1/jobs/"))
	default: writeJSON(w,http.StatusNotFound,map[string]string{"error":"route not found"})
	}
}

type submitRequest struct {
	Kind string `json:"kind"`
	Payload map[string]string `json:"payload"`
	MaxAttempts int `json:"max_attempts"`
}

func decode(w http.ResponseWriter,r *http.Request,dst any) bool {
	r.Body=http.MaxBytesReader(w,r.Body,1<<20)
	dec:=json.NewDecoder(r.Body); dec.DisallowUnknownFields()
	if err:=dec.Decode(dst); err!=nil { writeJSON(w,http.StatusBadRequest,map[string]string{"error":"invalid JSON: "+err.Error()}); return false }
	return true
}

func (s *Server) submit(w http.ResponseWriter,r *http.Request) {
	var req submitRequest
	if !decode(w,r,&req) { return }
	if req.MaxAttempts==0 { req.MaxAttempts=3 }
	now:=time.Now().UTC()
	job:=model.Job{ID:fmt.Sprintf("job-%08d",s.sequence.Add(1)),Kind:req.Kind,Payload:req.Payload,State:model.StateQueued,MaxAttempts:req.MaxAttempts,CreatedAt:now,UpdatedAt:now}
	if err:=job.Validate(); err!=nil { writeJSON(w,http.StatusBadRequest,map[string]string{"error":err.Error()}); return }
	if err:=s.jobs.Enqueue(job); err!=nil { writeJSON(w,http.StatusConflict,map[string]string{"error":err.Error()}); return }
	writeJSON(w,http.StatusCreated,job)
}

func (s *Server) registerWorker(w http.ResponseWriter,r *http.Request) {
	var worker scheduler.Worker
	if !decode(w,r,&worker) { return }
	if strings.TrimSpace(worker.ID)=="" { writeJSON(w,http.StatusBadRequest,map[string]string{"error":"worker id is required"}); return }
	writeJSON(w,http.StatusCreated,s.workers.Register(worker))
}

func (s *Server) heartbeat(w http.ResponseWriter,id string) {
	if id=="" || strings.Contains(id,"/") { writeJSON(w,http.StatusBadRequest,map[string]string{"error":"invalid worker id"}); return }
	worker,ok:=s.workers.Heartbeat(id)
	if !ok { writeJSON(w,http.StatusNotFound,map[string]string{"error":"worker not registered"}); return }
	writeJSON(w,http.StatusOK,worker)
}

func (s *Server) claim(w http.ResponseWriter,id string) {
	if id=="" || strings.Contains(id,"/") { writeJSON(w,http.StatusBadRequest,map[string]string{"error":"invalid worker id"}); return }
	if _,ok:=s.workers.Heartbeat(id); !ok { writeJSON(w,http.StatusNotFound,map[string]string{"error":"worker not registered"}); return }
	job,err:=s.jobs.Claim(id,s.lease)
	if errors.Is(err,queue.ErrEmpty) { writeJSON(w,http.StatusNoContent,map[string]string{"status":"empty"}); return }
	if err!=nil { writeJSON(w,http.StatusInternalServerError,map[string]string{"error":err.Error()}); return }
	writeJSON(w,http.StatusOK,job)
}

type completionRequest struct {
	WorkerID string `json:"worker_id"`
	Success bool `json:"success"`
	Error string `json:"error,omitempty"`
}

func (s *Server) complete(w http.ResponseWriter,r *http.Request,id string) {
	if id=="" || strings.Contains(id,"/") { writeJSON(w,http.StatusBadRequest,map[string]string{"error":"invalid job id"}); return }
	var req completionRequest
	if !decode(w,r,&req) { return }
	job,err:=s.jobs.Complete(id,req.WorkerID,req.Success,req.Error)
	switch {
	case errors.Is(err,queue.ErrNotFound): writeJSON(w,http.StatusNotFound,map[string]string{"error":"job not found"})
	case errors.Is(err,queue.ErrNotOwner): writeJSON(w,http.StatusConflict,map[string]string{"error":"job is not owned by this worker"})
	case err!=nil: writeJSON(w,http.StatusInternalServerError,map[string]string{"error":err.Error()})
	default: writeJSON(w,http.StatusOK,job)
	}
}

func (s *Server) inspect(w http.ResponseWriter,id string) {
	if id=="" || strings.Contains(id,"/") { writeJSON(w,http.StatusBadRequest,map[string]string{"error":"invalid job id"}); return }
	job,err:=s.jobs.Get(id)
	if errors.Is(err,queue.ErrNotFound) { writeJSON(w,http.StatusNotFound,map[string]string{"error":"job not found"}); return }
	if err!=nil { writeJSON(w,http.StatusInternalServerError,map[string]string{"error":"internal error"}); return }
	writeJSON(w,http.StatusOK,job)
}

func writeJSON(w http.ResponseWriter,status int,value any) {
	w.WriteHeader(status)
	if status!=http.StatusNoContent { _=json.NewEncoder(w).Encode(value) }
}
