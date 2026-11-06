package api

import (
 "bytes"
 "encoding/json"
 "net/http"
 "net/http/httptest"
 "testing"
 "time"
 "github.com/jadhavdurvesh/ORBIT/internal/queue"
 "github.com/jadhavdurvesh/ORBIT/internal/scheduler"
)

func newTestServer() *Server { return NewServer(queue.NewMemory(), scheduler.NewRegistry(time.Minute), time.Minute) }
func request(t *testing.T, s *Server, method, path, body string) *httptest.ResponseRecorder {
 t.Helper()
 req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
 rec := httptest.NewRecorder()
 s.ServeHTTP(rec, req)
 return rec
}
func TestWorkerJobLifecycleHTTP(t *testing.T) {
 s := newTestServer()
 w := request(t, s, http.MethodPost, "/v1/workers", `{"id":"worker-a","capacity":2}`)
 if w.Code != http.StatusCreated { t.Fatalf("register: %d %s", w.Code, w.Body) }
 sub := request(t, s, http.MethodPost, "/v1/jobs", `{"kind":"echo","payload":{"text":"hello"}}`)
 if sub.Code != http.StatusCreated { t.Fatalf("submit: %d %s", sub.Code, sub.Body) }
 var created struct { ID string `json:"id"` }
 if err := json.Unmarshal(sub.Body.Bytes(), &created); err != nil { t.Fatal(err) }
 claim := request(t, s, http.MethodPost, "/v1/workers/worker-a/claim", "{}")
 if claim.Code != http.StatusOK { t.Fatalf("claim: %d %s", claim.Code, claim.Body) }
 var claimed struct { LeaseToken string `json:"lease_token"` }
 if err := json.Unmarshal(claim.Body.Bytes(), &claimed); err != nil { t.Fatal(err) }
 payload, _ := json.Marshal(map[string]any{"worker_id":"worker-a","lease_token":claimed.LeaseToken,"success":true})
 done := request(t, s, http.MethodPost, "/v1/jobs/"+created.ID+"/complete", string(payload))
 if done.Code != http.StatusOK { t.Fatalf("complete: %d %s", done.Code, done.Body) }
}
func TestUnknownWorkerCannotClaim(t *testing.T) {
 s := newTestServer()
 rec := request(t, s, http.MethodPost, "/v1/workers/missing/claim", "{}")
 if rec.Code != http.StatusNotFound { t.Fatalf("got %d", rec.Code) }
}
