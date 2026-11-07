package queue

import("errors";"time";"github.com/jadhavdurvesh/ORBIT/internal/model")
var(ErrNotFound=errors.New("job not found");ErrConflict=errors.New("job already exists");ErrEmpty=errors.New("no queued jobs");ErrNotOwner=errors.New("job lease does not belong to this worker"))
type Store interface{
	Enqueue(model.Job)error
	EnqueueIdempotent(model.Job)(model.Job,bool,error)
	Get(string)(model.Job,error)
	FindByIdempotencyKey(string)(model.Job,error)
	List()[]model.Job
	Claim(workerID string,lease time.Duration)(model.Job,error)
	Complete(id,workerID,leaseToken string,success bool,message string,result map[string]string)(model.Job,error)
	RecoverExpired(now time.Time)int
}
