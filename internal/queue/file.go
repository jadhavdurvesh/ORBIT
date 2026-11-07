package queue

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jadhavdurvesh/ORBIT/internal/model"
)

// FileStore persists the queue as an atomically replaced JSON snapshot.
// It is intended for a single controller process, not shared multi-node access.
type FileStore struct {
	mu sync.Mutex
	mem *Memory
	path string
}

func NewFileStore(path string) (*FileStore,error) {
	if path=="" { return nil,fmt.Errorf("queue file path is required") }
	store:=&FileStore{mem:NewMemory(),path:path}
	data,err:=os.ReadFile(path)
	if err!=nil && !os.IsNotExist(err) { return nil,err }
	if err==nil {
		var jobs []model.Job
		if err:=json.Unmarshal(data,&jobs);err!=nil{return nil,fmt.Errorf("decode queue snapshot: %w",err)}
		for _,job:=range jobs {
			if err:=job.Validate();err!=nil{return nil,fmt.Errorf("invalid persisted job %s: %w",job.ID,err)}
			if _,exists:=store.mem.jobs[job.ID];exists{return nil,fmt.Errorf("duplicate persisted job %s",job.ID)}
			store.mem.jobs[job.ID]=job
		}
	}
	return store,nil
}

func(f *FileStore) snapshot() map[string]model.Job {
	f.mem.mu.RLock();defer f.mem.mu.RUnlock()
	copy:=make(map[string]model.Job,len(f.mem.jobs))
	for id,job:=range f.mem.jobs {
		if job.Payload!=nil { p:=make(map[string]string,len(job.Payload));for k,v:=range job.Payload{p[k]=v};job.Payload=p }
		if job.Result!=nil { p:=make(map[string]string,len(job.Result));for k,v:=range job.Result{p[k]=v};job.Result=p }
		if job.LeaseExpiresAt!=nil { t:=*job.LeaseExpiresAt;job.LeaseExpiresAt=&t }
		if job.RunAt!=nil { t:=*job.RunAt;job.RunAt=&t }
		if job.RetryAt!=nil { t:=*job.RetryAt;job.RetryAt=&t }
		copy[id]=job
	}
	return copy
}

func(f *FileStore) restore(snapshot map[string]model.Job) {
	f.mem.mu.Lock();defer f.mem.mu.Unlock();f.mem.jobs=snapshot
}

func(f *FileStore) persist() error {
	jobs:=f.mem.List()
	data,err:=json.MarshalIndent(jobs,"","  ");if err!=nil{return err}
	dir:=filepath.Dir(f.path)
	if err:=os.MkdirAll(dir,0755);err!=nil{return err}
	tmp:=f.path+".tmp"
	out,err:=os.OpenFile(tmp,os.O_CREATE|os.O_TRUNC|os.O_WRONLY,0600);if err!=nil{return err}
	if _,err=out.Write(data);err!=nil{_ = out.Close();_ = os.Remove(tmp);return err}
	if err=out.Sync();err!=nil{_ = out.Close();_ = os.Remove(tmp);return err}
	if err=out.Close();err!=nil{_ = os.Remove(tmp);return err}
	if err=os.Rename(tmp,f.path);err!=nil{_ = os.Remove(tmp);return err}
	return nil
}

func(f *FileStore) mutate(fn func()error)error {
	f.mu.Lock();defer f.mu.Unlock()
	before:=f.snapshot()
	if err:=fn();err!=nil{return err}
	if err:=f.persist();err!=nil{f.restore(before);return err}
	return nil
}
func(f *FileStore) Enqueue(job model.Job)error{return f.mutate(func()error{return f.mem.Enqueue(job)})}
func(f *FileStore) EnqueueIdempotent(job model.Job)(model.Job,bool,error){
	f.mu.Lock();defer f.mu.Unlock()
	before:=f.snapshot()
	stored,created,err:=f.mem.EnqueueIdempotent(job)
	if err!=nil||!created{return stored,created,err}
	if err=f.persist();err!=nil{f.restore(before);return model.Job{},false,err}
	return stored,true,nil
}
func(f *FileStore) Get(id string)(model.Job,error){return f.mem.Get(id)}
func(f *FileStore) FindByIdempotencyKey(key string)(model.Job,error){return f.mem.FindByIdempotencyKey(key)}
func(f *FileStore) List()[]model.Job{return f.mem.List()}
func(f *FileStore) Claim(worker string,lease time.Duration)(job model.Job,err error){
	err=f.mutate(func()error{var e error;job,e=f.mem.Claim(worker,lease);return e});return
}
func(f *FileStore) Complete(id,worker,token string,success bool,message string,result map[string]string)(job model.Job,err error){
	err=f.mutate(func()error{var e error;job,e=f.mem.Complete(id,worker,token,success,message,result);return e});return
}
func(f *FileStore) RecoverExpired(now time.Time)(count int){
	f.mu.Lock();defer f.mu.Unlock()
	before:=f.snapshot()
	count=f.mem.RecoverExpired(now)
	if count>0 { if err:=f.persist();err!=nil{f.restore(before);return 0} }
	return
}

func(f *FileStore) RenewLease(id,worker,token string,lease time.Duration)(job model.Job,err error){
 err=f.mutate(func()error{var e error;job,e=f.mem.RenewLease(id,worker,token,lease);return e});return
}
