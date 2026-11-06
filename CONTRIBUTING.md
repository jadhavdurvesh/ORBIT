# Contributing

1. Keep packages focused and interfaces small.
2. Add tests for state transitions, concurrency, and failure paths.
3. Run `gofmt -w .`, `go test ./...`, and `go vet ./...` before submitting.
4. Do not claim durability, exactly-once execution, or fault tolerance until backed by tests.
5. Document behavior changes and update ROADMAP.md.
