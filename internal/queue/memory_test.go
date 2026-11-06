package queue

import (
	"errors"
	"testing"
	"time"

	"github.com/jadhavdurvesh/ORBIT/internal/model"
)

func testJob(id string) model.Job {
	now:=time.Now().UTC()
	return model.Job{ID:id,Kind:"echo",State:model.StateQueued,MaxAttempts:2,CreatedAt:now,UpdatedAt:now}
}

func TestClaimCompleteAndRetry(t *testing.T) {
	q:=NewMemory(); if err:=q.Enqueue(testJob("a")); err!=nil { t.Fatal(err) }
	job,err:=q.Claim("worker-1",time.Minute); if err!=nil { t.Fatal(err) }
	if job.State!=model.StateRunning || job.Attempts!=1 || job.WorkerID!="worker-1" { t.Fatalf("unexpected claim: %+v",job) }
	if _,err=q.Complete("a","wrong",true,""); !errors.Is(err,ErrNotOwner) { t.Fatalf("expected ownership error, got %v",err) }
	job,err=q.Complete("a","worker-1",false,"transient"); if err!=nil { t.Fatal(err) }
	if job.State!=model.StateQueued || job.LastError!="transient" { t.Fatalf("expected retry queue: %+v",job) }
	job,err=q.Claim("worker-2",time.Minute); if err!=nil { t.Fatal(err) }
	job,err=q.Complete("a","worker-2",false,"permanent"); if err!=nil { t.Fatal(err) }
	if job.State!=model.StateFailed { t.Fatalf("expected terminal failure: %+v",job) }
}

func TestExpiredLeaseRecovery(t *testing.T) {
	q:=NewMemory(); _=q.Enqueue(testJob("lease"))
	job,err:=q.Claim("w",time.Millisecond); if err!=nil { t.Fatal(err) }
	n:=time.Now().Add(time.Second)
	if count:=q.RecoverExpired(n); count!=1 { t.Fatalf("recovered %d jobs",count) }
	got,_:=q.Get(job.ID)
	if got.State!=model.StateQueued || got.WorkerID!="" { t.Fatalf("lease not recovered: %+v",got) }
}
