package queue

import (
	"errors"
	"testing"
	"time"

	"github.com/jadhavdurvesh/ORBIT/internal/model"
)

func testJob(id string)model.Job{now:=time.Now().UTC();return model.Job{ID:id,Kind:"echo",State:model.StateQueued,MaxAttempts:2,CreatedAt:now,UpdatedAt:now}}
func TestClaimCompleteAndRetry(t *testing.T){
	q:=NewMemory();if err:=q.Enqueue(testJob("a"));err!=nil{t.Fatal(err)}
	j,err:=q.Claim("worker-1",time.Minute);if err!=nil{t.Fatal(err)}
	if j.State!=model.StateRunning||j.Attempts!=1||j.WorkerID!="worker-1"{t.Fatalf("unexpected claim: %+v",j)}
	firstToken:=j.LeaseToken
	if _,err=q.Complete("a","wrong","",true,"",nil);!errors.Is(err,ErrNotOwner){t.Fatalf("expected ownership error, got %v",err)}
	j,err=q.Complete("a","worker-1",j.LeaseToken,false,"transient",nil);if err!=nil{t.Fatal(err)}
	if j.State!=model.StateQueued||j.LastError!="transient"{t.Fatalf("expected retry queue: %+v",j)}
	oldToken:=firstToken
	j,err=q.Claim("worker-2",time.Minute);if err!=nil{t.Fatal(err)}
	if j.LeaseToken==oldToken{t.Fatal("lease token must rotate on every claim")}
	if _,err=q.Complete("a","worker-2",oldToken,true,"",nil);!errors.Is(err,ErrNotOwner){t.Fatalf("stale token accepted: %v",err)}
	j,err=q.Complete("a","worker-2",j.LeaseToken,true,"",map[string]string{"message":"done"});if err!=nil{t.Fatal(err)}
	if j.State!=model.StateSucceeded||j.Result["message"]!="done"{t.Fatalf("result missing: %+v",j)}
}
func TestExpiredLeaseRecovery(t *testing.T){
	q:=NewMemory();_ = q.Enqueue(testJob("lease"));j,err:=q.Claim("w",time.Millisecond);if err!=nil{t.Fatal(err)}
	if n:=q.RecoverExpired(time.Now().Add(time.Second));n!=1{t.Fatalf("recovered %d jobs",n)}
	got,_:=q.Get(j.ID);if got.State!=model.StateQueued||got.WorkerID!=""{t.Fatalf("lease not recovered: %+v",got)}
}

func TestScheduledJobIsNotClaimedEarly(t *testing.T) {
	q := NewMemory()
	job := testJob("scheduled")
	runAt := time.Now().Add(2 * time.Hour).UTC()
	job.RunAt = &runAt
	if err := q.Enqueue(job); err != nil { t.Fatal(err) }
	if _, err := q.Claim("worker", time.Minute); !errors.Is(err, ErrEmpty) {
		t.Fatalf("future job should not be claimable, got %v", err)
	}
	job.RunAt = nil
	if err := q.Enqueue(testJob("ready")); err != nil { t.Fatal(err) }
	if got, err := q.Claim("worker", time.Minute); err != nil || got.ID != "ready" {
		t.Fatalf("ready job should be claimable while scheduled job waits: job=%+v err=%v", got, err)
	}
}
