# ORBIT

**A distributed job orchestration system written in Go.**

ORBIT provides a control-plane API, worker registry, lease-based job claims, retries, abandoned-work recovery, and a standalone worker agent. The current release is a single-controller deployment with an atomically persisted JSON job store, intended for local development and systems experimentation.

## Features

- HTTP API for job submission, inspection, and worker registration
- Standalone worker process with heartbeat and polling
- Atomic in-memory job claims with expiring leases
- Worker ownership checks on completion
- Retry limits and terminal failure state
- Automatic recovery of jobs whose worker lease expires
- Built-in task handlers: `echo`, `uppercase`, `reverse`, and bounded `sleep`
- Basic Prometheus text metrics at `/metrics`
- Graceful controller shutdown and Go race-test CI

## Architecture

```text
             Client
               |
               v
       +------------------+
       | ORBIT Controller |
       | HTTP API         |
       +--------+---------+
                |
       +--------+---------+
       |                  |
       v                  v
+--------------+   +----------------+
| Job Store    |   | Worker Registry|
| queue/leases |   | heartbeat/TTL  |
+------+-------+   +--------+-------+
       |                    ^
       | claim              | register / heartbeat
       v                    |
  +-------------------------------+
  | ORBIT Worker Agent(s)         |
  | execute -> acknowledge result |
  +-------------------------------+
```

## Requirements

- Go 1.22+

## Run locally

Terminal 1 — controller:

```bash
go run ./cmd/orbitd
```

Terminal 2 — worker:

```bash
ORBIT_WORKER_ID=worker-01 go run ./cmd/orbit-worker
```

On PowerShell:

```powershell
$env:ORBIT_WORKER_ID = "worker-01"
go run ./cmd/orbit-worker
```

The controller listens on `:8080` by default. Configure it with:

| Variable | Default | Purpose |
|---|---:|---|
| `ORBIT_ADDR` | `:8080` | HTTP listen address |
| `ORBIT_LEASE_DURATION` | `30s` | Job claim lease |
| `ORBIT_WORKER_TTL` | `90s` | Worker expiry threshold |
| `ORBIT_DATA_FILE` | `./data/orbit.json` | Durable job snapshot path |
| `ORBIT_URL` | `http://127.0.0.1:8080` | Worker controller URL |
| `ORBIT_WORKER_ID` | hostname | Stable worker identifier |

## Run with Docker Compose

```bash
docker compose up --build
```

This starts one controller and one worker, and persists job snapshots in the named `orbit-data` volume. Stop with `docker compose down`; the volume remains until explicitly removed.

## Submit work

```bash
curl -X POST http://localhost:8080/v1/jobs \
  -H 'content-type: application/json' \
  -d '{"kind":"uppercase","payload":{"value":"hello orbit"},"max_attempts":3}'
```

Supported job payloads:

| Kind | Payload | Result |
|---|---|---|
| `echo` | `{"message":"hello"}` | Same message |
| `uppercase` | `{"value":"hello"}` | `HELLO` |
| `reverse` | `{"value":"orbit"}` | `tibro` |
| `sleep` | `{"duration":"250ms"}` | Completed duration |

Inspect jobs and workers:

```bash
curl http://localhost:8080/v1/jobs
curl http://localhost:8080/v1/workers
curl http://localhost:8080/v1/jobs/job-00000001
curl http://localhost:8080/metrics
``

## HTTP API

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/healthz` | Liveness |
| GET | `/metrics` | Prometheus-style counters |
| POST | `/v1/jobs` | Submit job |
| GET | `/v1/jobs` | List jobs |
| GET | `/v1/jobs/{id}` | Inspect job |
| POST | `/v1/workers` | Register worker |
| GET | `/v1/workers` | List workers |
| POST | `/v1/workers/{id}/heartbeat` | Refresh worker liveness |
| POST | `/v1/workers/{id}/claim` | Claim next job |
| POST | `/v1/jobs/{id}/complete` | Acknowledge result |

## Reliability model

ORBIT currently offers **at-least-once dispatch semantics within one controller process**. A lease timeout makes a running job eligible for retry; a worker that completes after losing its lease cannot acknowledge a job it no longer owns. Task handlers should therefore be idempotent.

The current store is volatile: controller restart loses jobs and worker registrations. This release does not claim durable persistence, multi-controller consensus, exactly-once execution, or production security. Those require the persistence and distributed-control milestones below.

## Development

```bash
gofmt -w .
go test -race ./...
go vet ./...
``

See [ROADMAP.md](ROADMAP.md) and [docs/architecture.md](docs/architecture.md).

## License

MIT.
