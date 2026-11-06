package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jadhavdurvesh/ORBIT/internal/api"
	"github.com/jadhavdurvesh/ORBIT/internal/queue"
	"github.com/jadhavdurvesh/ORBIT/internal/scheduler"
)

func envDuration(key string, fallback time.Duration) time.Duration {
	value:=os.Getenv(key)
	if value=="" { return fallback }
	d,err:=time.ParseDuration(value); if err!=nil || d<=0 { log.Printf("invalid %s=%q; using %s",key,value,fallback); return fallback }
	return d
}

func main() {
	addr:=os.Getenv("ORBIT_ADDR"); if addr=="" { addr=":8080" }
	lease:=envDuration("ORBIT_LEASE_DURATION",30*time.Second)
	workerTTL:=envDuration("ORBIT_WORKER_TTL",90*time.Second)
	dataFile:=os.Getenv("ORBIT_DATA_FILE");if dataFile==""{dataFile="./data/orbit.json"}
	jobs,err:=queue.NewFileStore(dataFile);if err!=nil{log.Fatalf("open job store: %v",err)}
	workers:=scheduler.NewRegistry(workerTTL)
	handler:=api.NewServer(jobs,workers,lease)
	server:=&http.Server{Addr:addr,Handler:handler,ReadHeaderTimeout:5*time.Second,ReadTimeout:10*time.Second,WriteTimeout:15*time.Second,IdleTimeout:60*time.Second}

	stop:=make(chan os.Signal,1); signal.Notify(stop,syscall.SIGINT,syscall.SIGTERM)
	go func() {
		ticker:=time.NewTicker(time.Second); defer ticker.Stop()
		for range ticker.C { now:=time.Now().UTC(); jobs.RecoverExpired(now); workers.Prune(now) }
	}()
	go func(){ log.Printf("ORBIT control plane listening on %s",addr); if err:=server.ListenAndServe(); err!=nil && err!=http.ErrServerClosed { log.Fatal(err) } }()
	<-stop
	log.Printf("ORBIT shutting down")
	_ = server.Close()
}
