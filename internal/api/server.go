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

type Server struct { jobs queue.Store; workers *scheduler.Registry; lease time.Duration; sequence atomic.Uint64 }
func NewServer(jobs queue.Store,workers *scheduler.Registry,lease time.Duration)*Server{return &Server{jobs:jobs,workers:workers,lease:lease}}

func(s *Server)ServeHTTP(w http.ResponseWriter,r *http.Request){
	w.Header().Set("content-type","application/json")
	switch{
	case r.URL.Path=="/healthz"&&r.Method==http.MethodGet:writeJSON(w,200,map[string]string{"status":"ok"})
	case r.URL.Path=="/metrics"&&r.Method==http.MethodGet:s.metrics(w)
	case r.URL.Path=="/v1/jobs"&&r.Method==http.MethodPost:s.submit(w,r)
	case r.URL.Path=="/v1/jobs"&&r.Method==http.MethodGet:writeJSON(w,200,s.jobs.List())
	case r.URL.Path=="/v1/workers"&&r.Method==http.MethodPost:s.registerWorker(w,r)
	case r.URL.Path=="/v1/workers"&&r.Method==http.MethodGet:writeJSON(w,200,s.workers.List())
	case strings.HasPrefix(r.URL.Path,"/v1/workers/")&&strings.HasSuffix(r.URL.Path,"/heartbeat")&&r.Method==http.MethodPost:
		s.heartbeat(w,strings.TrimSuffix(strings.TrimPrefix(r.URL.Path,"/v1/workers/"),"/heartbeat"))
	case strings.HasPrefix(r.URL.Path,"/v1/workers/")&&strings.HasSuffix(r.URL.Path,"/claim")&&r.Method==http.MethodPost:
		s.claim(w,strings.TrimSuffix(strings.TrimPrefix(r.URL.Path,"/v1/workers/"),"/claim"))
	case strings.HasPrefix(r.URL.Path,"/v1/jobs/")&&strings.HasSuffix(r.URL.Path,"/complete")&&r.Method==http.MethodPost:
		s.complete(w,r,strings.TrimSuffix(strings.TrimPrefix(r.URL.Path,"/v1/jobs/"),"/complete"))
	case strings.HasPrefix(r.URL.Path,"/v1/jobs/")&&r.Method==http.MethodGet:s.inspect(w,strings.TrimPrefix(r.URL.Path,"/v1/jobs/"))
	default:writeJSON(w,http.StatusNotFound,map[string]string{"error":"route not found"})
	}
}

func decode(w http.ResponseWriter,r *http.Request,dst any)bool{
	r.Body=http.MaxBytesReader(w,r.Body,1<<20);d:=json.NewDecoder(r.Body);d.DisallowUnknownFields()
	if err:=d.Decode(dst);err!=nil{writeJSON(w,400,map[string]string{"error":"invalid JSON: "+err.Error()});return false};return true
}
type submitRequest struct{Kind string `json:"kind"`;Payload map[string]string `json:"payload"`;MaxAttempts int `json:"max_attempts"`}
func(s *Server)submit(w http.ResponseWriter,r *http.Request){
	var req submitRequest;if !decode(w,r,&req){return};if req.MaxAttempts==0{req.MaxAttempts=3}
	now:=time.Now().UTC();j:=model.Job{ID:fmt.Sprintf("job-%08d",s.sequence.Add(1)),Kind:req.Kind,Payload:req.Payload,State:model.StateQueued,MaxAttempts:req.MaxAttempts,CreatedAt:now,UpdatedAt:now}
	if err:=j.Validate();err!=nil{writeJSON(w,400,map[string]string{"error":err.Error()});return};if err:=s.jobs.Enqueue(j);err!=nil{writeJSON(w,409,map[string]string{"error":err.Error()});return};writeJSON(w,201,j)
}
func(s *Server)registerWorker(w http.ResponseWriter,r *http.Request){var worker scheduler.Worker;if !decode(w,r,&worker){return};if strings.TrimSpace(worker.ID)==""{writeJSON(w,400,map[string]string{"error":"worker id is required"});return};writeJSON(w,201,s.workers.Register(worker))}
func(s *Server)heartbeat(w http.ResponseWriter,id string){if id==""||strings.Contains(id,"/"){writeJSON(w,400,map[string]string{"error":"invalid worker id"});return};worker,ok:=s.workers.Heartbeat(id);if !ok{writeJSON(w,404,map[string]string{"error":"worker not registered"});return};writeJSON(w,200,worker)}
func(s *Server)claim(w http.ResponseWriter,id string){if id==""||strings.Contains(id,"/"){writeJSON(w,400,map[string]string{"error":"invalid worker id"});return};if _,ok:=s.workers.Heartbeat(id);!ok{writeJSON(w,404,map[string]string{"error":"worker not registered"});return};j,err:=s.jobs.Claim(id,s.lease)
	if errors.Is(err,queue.ErrEmpty){w.WriteHeader(http.StatusNoContent);return};if err!=nil{writeJSON(w,500,map[string]string{"error":err.Error()});return};writeJSON(w,200,j)
}
type completionRequest struct{WorkerID string `json:"worker_id"`;Success bool `json:"success"`;Error string `json:"error,omitempty"`;Result map[string]string `json:"result,omitempty"`}
func(s *Server)complete(w http.ResponseWriter,r *http.Request,id string){
	if id==""||strings.Contains(id,"/"){writeJSON(w,400,map[string]string{"error":"invalid job id"});return};var req completionRequest;if !decode(w,r,&req){return}
	j,err:=s.jobs.Complete(id,req.WorkerID,req.Success,req.Error,req.Result)
	switch{case errors.Is(err,queue.ErrNotFound):writeJSON(w,404,map[string]string{"error":"job not found"});case errors.Is(err,queue.ErrNotOwner):writeJSON(w,409,map[string]string{"error":"job is not owned by this worker"});case err!=nil:writeJSON(w,500,map[string]string{"error":err.Error()});default:writeJSON(w,200,j)}
}
func(s *Server)inspect(w http.ResponseWriter,id string){if id==""||strings.Contains(id,"/"){writeJSON(w,400,map[string]string{"error":"invalid job id"});return};j,err:=s.jobs.Get(id);if errors.Is(err,queue.ErrNotFound){writeJSON(w,404,map[string]string{"error":"job not found"});return};if err!=nil{writeJSON(w,500,map[string]string{"error":"internal error"});return};writeJSON(w,200,j)}
func(s *Server)metrics(w http.ResponseWriter){
	counts:=map[model.JobState]int{};for _,j:=range s.jobs.List(){counts[j.State]++}
	w.Header().Set("content-type","text/plain; version=0.0.4")
	fmt.Fprintf(w,"orbit_jobs_total %d\n",len(s.jobs.List()))
	for _,state:=range []model.JobState{model.StateQueued,model.StateRunning,model.StateSucceeded,model.StateFailed}{fmt.Fprintf(w,"orbit_jobs{state=%q} %d\n",state,counts[state])}
	fmt.Fprintf(w,"orbit_workers_registered %d\n",len(s.workers.List()))
}
func writeJSON(w http.ResponseWriter,status int,value any){w.WriteHeader(status);if status!=http.StatusNoContent{_ = json.NewEncoder(w).Encode(value)}}
