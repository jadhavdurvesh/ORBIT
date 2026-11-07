package queue

import(
	"path/filepath"
	"testing"
	"time"
	"github.com/jadhavdurvesh/ORBIT/internal/model"
)

func TestFileStorePersistsJobLifecycle(t *testing.T){
	path:=filepath.Join(t.TempDir(),"state","jobs.json")
	store,err:=NewFileStore(path);if err!=nil{t.Fatal(err)}
	now:=time.Now().UTC()
	job:=model.Job{ID:"persisted",Kind:"echo",Payload:map[string]string{"message":"hello"},State:model.StateQueued,MaxAttempts:2,CreatedAt:now,UpdatedAt:now}
	if err:=store.Enqueue(job);err!=nil{t.Fatal(err)}
	claimed,err:=store.Claim("worker-a",time.Minute);if err!=nil{t.Fatal(err)}
	reopened,err:=NewFileStore(path);if err!=nil{t.Fatal(err)}
	got,err:=reopened.Get("persisted");if err!=nil{t.Fatal(err)}
	if got.State!=model.StateRunning||got.LeaseToken!=claimed.LeaseToken{t.Fatalf("state not persisted: %+v",got)}
	completed,err:=reopened.Complete(got.ID,"worker-a",got.LeaseToken,true,"",map[string]string{"answer":"42"});if err!=nil{t.Fatal(err)}
	final,err:=NewFileStore(path);if err!=nil{t.Fatal(err)}
	got,err=final.Get("persisted");if err!=nil{t.Fatal(err)}
	if got.State!=model.StateSucceeded||got.Result["answer"]!="42"||completed.State!=model.StateSucceeded{t.Fatalf("completion not persisted: %+v",got)}
}

func TestFileStorePersistsRetryAvailability(t *testing.T) {
 path:=filepath.Join(t.TempDir(),"jobs.json")
 store,err:=NewFileStore(path);if err!=nil{t.Fatal(err)}
 if err=store.Enqueue(testJob("retry-persist"));err!=nil{t.Fatal(err)}
 job,err:=store.Claim("worker-a",time.Minute);if err!=nil{t.Fatal(err)}
 job,err=store.Complete(job.ID,"worker-a",job.LeaseToken,false,"temporary",nil);if err!=nil{t.Fatal(err)}
 if job.RetryAt==nil{t.Fatal("retry timestamp missing")}
 reopened,err:=NewFileStore(path);if err!=nil{t.Fatal(err)}
 loaded,err:=reopened.Get(job.ID);if err!=nil{t.Fatal(err)}
 if loaded.RetryAt==nil||!loaded.RetryAt.Equal(*job.RetryAt){t.Fatalf("retry timestamp not persisted: got %v want %v",loaded.RetryAt,job.RetryAt)}
}
