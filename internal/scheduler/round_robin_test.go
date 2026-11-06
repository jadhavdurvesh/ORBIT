package scheduler

import "testing"

func TestRoundRobinCyclesWorkers(t *testing.T) {
	r := NewRoundRobin()
	r.Register(Worker{ID: "a"})
	r.Register(Worker{ID: "b"})
	for _, want := range []string{"a", "b", "a"} {
		got, ok := r.Next()
		if !ok || got.ID != want {
			t.Fatalf("Next() = %q, %v; want %q", got.ID, ok, want)
		}
	}
}
