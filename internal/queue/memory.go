package queue

import(
	"crypto/rand"
	"encoding/hex"
	"sort"
	"sync"
	"time"
	"github.com/jadhavdurvesh/ORBIT/internal/model"
)
type Memory struct{mu sync.RWMutex;jobs map[string]model.Job}
func NewMemory()*Memory{return &Memory{jobs:make(map[string]model.Job)}}
func(m *Memory)Enqueue(j model.Job)error{if err:=j.Validate();err!=nil{return err};m.mu.Lock();defer m.mu.Unlock();if _,ok:=m.jobs[j.ID];ok{return ErrConflict};m.jobs[j.ID]=j;return nil}
func(m *Memory)Get(id string)(model.Job,error){m.mu.RLock();defer m.mu.RUnlock();j,ok:=m.jobs[id];if !ok{return model.Job{},ErrNotFound};return j,nil}
func(m *Memory)List()[]model.Job{m.mu.RLock();defer m.mu.RUnlock();out:=make([]model.Job,0,len(m.jobs));for _,j:=range m.jobs{out=append(out,j)};sort.Slice(out,func(i,j int)bool{if out[i].CreatedAt.Equal(out[j].CreatedAt){return out[i].ID<out[j].ID};return out[i].CreatedAt.Before(out[j].CreatedAt)});return out}
func(m *Memory)Claim(workerID string,lease time.Duration)(model.Job,error){
	if workerID==""||lease<=0{return model.Job{},model.ErrInvalidJob}
	m.mu.Lock();defer m.mu.Unlock();var selected *model.Job
	for _,j:=range m.jobs{if j.State!=model.StateQueued{continue};if selected==nil||j.CreatedAt.Before(selected.CreatedAt)||(j.CreatedAt.Equal(selected.CreatedAt)&&j.ID<selected.ID){copy:=j;selected=&copy}}
	if selected==nil{return model.Job{},ErrEmpty}
	token:=make([]byte,24);if _,err:=rand.Read(token);err!=nil{return model.Job{},err}
	now:=time.Now().UTC();if err:=selected.Transition(model.StateRunning,now);err!=nil{return model.Job{},err}
	selected.Attempts++;selected.WorkerID=workerID;selected.LeaseToken=hex.EncodeToString(token);expiry:=now.Add(lease);selected.LeaseExpiresAt=&expiry
	if err:=selected.Validate();err!=nil{return model.Job{},err};m.jobs[selected.ID]=*selected;return *selected,nil
}
func(m *Memory)Complete(id,workerID,token string,success bool,message string,result map[string]string)(model.Job,error){
	m.mu.Lock();defer m.mu.Unlock();j,ok:=m.jobs[id];if !ok{return model.Job{},ErrNotFound}
	if j.State!=model.StateRunning||j.WorkerID!=workerID||j.LeaseToken==""||j.LeaseToken!=token{return model.Job{},ErrNotOwner}
	now:=time.Now().UTC();j.LastError=""
	if success{if err:=j.Transition(model.StateSucceeded,now);err!=nil{return model.Job{},err};j.Result=result
	}else{j.LastError=message;if j.Attempts<j.MaxAttempts{if err:=j.Transition(model.StateQueued,now);err!=nil{return model.Job{},err}}else{if err:=j.Transition(model.StateFailed,now);err!=nil{return model.Job{},err}}}
	m.jobs[id]=j;return j,nil
}
func(m *Memory)RecoverExpired(now time.Time)int{m.mu.Lock();defer m.mu.Unlock();n:=0;for id,j:=range m.jobs{if j.State!=model.StateRunning||j.LeaseExpiresAt==nil||j.LeaseExpiresAt.After(now){continue};j.LastError="worker lease expired";if j.Attempts<j.MaxAttempts{_ = j.Transition(model.StateQueued,now)}else{_ = j.Transition(model.StateFailed,now)};m.jobs[id]=j;n++};return n}
