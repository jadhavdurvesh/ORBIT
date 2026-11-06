# ORBIT roadmap

## M0 — Control-plane skeleton (current)
- [x] Job model and validated lifecycle
- [x] Thread-safe in-memory queue
- [x] Deterministic round-robin worker selection
- [x] HTTP submit, inspect, list, and dispatch endpoints
- [x] Initial unit tests

## M1 — Worker protocol
- [ ] Worker registration and heartbeat endpoints
- [ ] Worker capability and capacity metadata
- [ ] Job acknowledgement and completion reporting
- [ ] Integration tests for worker lifecycle

## M2 — Reliability
- [ ] Lease expiry and abandoned-job recovery
- [ ] Bounded retries with exponential backoff
- [ ] Dead-letter queue
- [ ] Idempotency keys and duplicate submission protection

## M3 — Persistence and operations
- [ ] SQLite/PostgreSQL storage adapter
- [ ] Structured logs, metrics, and tracing
- [ ] Graceful shutdown and configuration
- [ ] Docker Compose deployment

## M4 — Distributed control plane
- [ ] Resource-aware scheduling
- [ ] Controller membership and leader election
- [ ] Replicated metadata log
- [ ] Chaos and fault-injection test suite
