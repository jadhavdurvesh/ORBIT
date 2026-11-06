.PHONY: test build run-controller run-worker fmt vet

test:
	go test -race ./...

build:
	go build ./cmd/orbitd ./cmd/orbit-worker

run-controller:
	go run ./cmd/orbitd

run-worker:
	go run ./cmd/orbit-worker

fmt:
	gofmt -w .

vet:
	go vet ./...
