# ORBIT roadmap

## M0 — Control plane
- [x] Job model and lifecycle validation
- [x] Thread-safe in-memory queue
- [x] Deterministic job ordering
- [x] HTTP submit and inspection API

## M1 — Worker execution
- [x] Worker registration and heartbeat
- [x] Worker liveness TTL
- [x] Lease-based atomic job claim
- [x] Worker completion and result reporting
- [x] Retry limit and terminal failure
- [x] Expired-lease recovery
- [x] Standalone worker executable
- [x] Built-in task handlers
- [x] HTTP integration and queue tests

## M2 — Operations and packaging
- [x] Health endpoint
- [x] Prometheus text metrics
- [x] Graceful controller shutdown
- [x] Go race-test CI workflow
- [ ] Structured logging and request IDs
- [ ] Config file and environment validation
- [ ] Docker images and Compose example
- [ ] OpenTelemetry tracing

## M3 — Durable execution
- [ ] SQLite/PostgreSQL store
- [ ] Durable queue and transactional claim
- [ ] Idempotency keys
- [ ] Delayed jobs and exponential backoff
- [ ] Dead-letter queue and replay
- [ ] Integration tests against real database

## M4 — Distributed control plane
- [ ] Resource-aware scheduling and worker concurrency limits
- [ ] Multi-controller membership
- [ ] Leader election and replicated metadata
- [ ] Partition and chaos testing
- [ ] Horizontal scaling benchmarks
