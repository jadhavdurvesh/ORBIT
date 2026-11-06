package scheduler

import (
	"testing"
	"time"
)

func TestHeartbeatAndPrune(t *testing.T) {
	r:=NewRegistry(time.Second)
	r.Register(Worker{ID:"w1"})
	if _,ok:=r.Heartbeat("w1"); !ok { t.Fatal("heartbeat should find worker") }
	if n:=r.Prune(time.Now().Add(2*time.Second)); n!=1 { t.Fatalf("expected prune, got %d",n) }
	if _,ok:=r.Next(); ok { t.Fatal("expected empty registry") }
}
