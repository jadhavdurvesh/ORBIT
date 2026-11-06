package queue

import (
	"errors"

	"github.com/jadhavdurvesh/ORBIT/internal/model"
)

var (
	ErrNotFound = errors.New("job not found")
	ErrConflict = errors.New("job already exists")
	ErrEmpty    = errors.New("no queued jobs")
)

type Store interface {
	Enqueue(model.Job) error
	Get(string) (model.Job, error)
	List() []model.Job
	Claim() (model.Job, error)
	Update(model.Job) error
}
