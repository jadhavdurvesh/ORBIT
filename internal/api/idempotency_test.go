package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jadhavdurvesh/ORBIT/internal/model"
	"github.com/jadhavdurvesh/ORBIT/internal/queue"
	"github.com/jadhavdurvesh/ORBIT/internal/scheduler"
)

func TestSubmitIsIdempotent(t *testing.T) {
	store := queue.NewMemory()
	server := NewServer(store, scheduler.NewRegistry(time.Minute), 30*time.Second)
	body := `{"kind":"report","payload":{"format":"json"},"max_attempts":2}`

	submit := func() model.Job {
		req := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(body))
		req.Header.Set("Idempotency-Key", "report-2026-11-07")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated && rec.Code != http.StatusOK {
			t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
		}
		var job model.Job
		if err := json.Unmarshal(rec.Body.Bytes(), &job); err != nil {
			t.Fatal(err)
		}
		return job
	}

	first := submit()
	second := submit()
	if first.ID != second.ID {
		t.Fatalf("idempotent retry created a different job: %s != %s", first.ID, second.ID)
	}
	if second.IdempotencyKey != "report-2026-11-07" {
		t.Fatalf("idempotency key not persisted: %q", second.IdempotencyKey)
	}
	if got := len(store.List()); got != 1 {
		t.Fatalf("expected one stored job, got %d", got)
	}
}
