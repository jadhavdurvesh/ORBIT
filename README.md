# ORBIT

**Distributed job orchestration, built in Go.**

ORBIT is an educational, production-minded control plane for dispatching work to a pool of worker agents. The first milestone establishes the domain model, deterministic scheduling, an in-memory queue, and an HTTP control API. Later milestones add worker heartbeats, leases, persistence, retries, resource-aware placement, and replicated controller state.

## Design goals

- Explicit job lifecycle and validated state transitions
- At-least-once execution semantics with lease-based recovery (planned)
- Stateless HTTP API around a replaceable queue and scheduler
- Small interfaces so storage and transport implementations remain swappable
- Observable behavior and deterministic tests over magic behavior

## Current status

| Capability | Status |
|---|---|
| Job model and lifecycle validation | Implemented |
| In-memory queue | Implemented |
| Round-robin worker scheduler | Implemented |
| HTTP API: submit, inspect, list, dispatch | Implemented |
| Persistent queue / worker leases | Planned |
| Heartbeats / node discovery | Planned |
| Retry policy / dead-letter queue | Planned |
| Replicated metadata / leader election | Planned |

This repository is intentionally developed in verifiable increments. Features marked planned are not represented as implemented.

## Architecture

```text
                 +----------------------+
 Client -------->| ORBIT HTTP API       |
                 | submit / inspect     |
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 | Job Queue            |
                 | enqueue / claim      |
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 | Scheduler            |
                 | worker selection     |
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 | Worker Registry      |
                 | (in-memory, v1)      |
                 +----------------------+
```

## Quick start

Requires Go 1.22 or newer.

```bash
go test ./...
go run ./cmd/orbitd
```

The API listens on `:8080` by default.

### Submit a job

```bash
curl -sS -X POST http://localhost:8080/v1/jobs \
  -H 'content-type: application/json' \
  -d '{"kind":"echo","payload":{"message":"hello orbit"}}'
```

### List jobs

```bash
curl -sS http://localhost:8080/v1/jobs
```

### Dispatch a queued job

```bash
curl -sS -X POST http://localhost:8080/v1/dispatch
```

## API

- `POST /v1/jobs` — validate and enqueue a job
- `GET /v1/jobs` — list known jobs
- `GET /v1/jobs/{id}` — inspect one job
- `POST /v1/dispatch` — claim the next job and select a worker

## Development

```bash
gofmt -w .
go test ./...
go vet ./...
```

See [ROADMAP.md](ROADMAP.md), [docs/architecture.md](docs/architecture.md), and [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT. See [LICENSE](LICENSE).
