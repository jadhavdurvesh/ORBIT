package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jadhavdurvesh/ORBIT/internal/api"
	"github.com/jadhavdurvesh/ORBIT/internal/queue"
	"github.com/jadhavdurvesh/ORBIT/internal/scheduler"
)

func main() {
	addr := os.Getenv("ORBIT_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	jobs := queue.NewMemory()
	placement := scheduler.NewRoundRobin()
	handler := api.NewServer(jobs, placement)

	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("ORBIT control plane listening on %s", addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
