# Architecture notes

## Components

- **API** validates client requests and exposes job operations.
- **Queue store** owns job records and atomically claims queued work.
- **Scheduler** selects a worker independently of queue storage.
- **Worker agent** will execute a claimed job and report its outcome (M1).

## Invariants

1. A job has a non-empty identifier and kind.
2. A queued job can become running or failed.
3. A running job can succeed, fail, or return to queued during recovery.
4. Terminal jobs cannot be silently reopened.
5. Queue implementations must make claim atomic.

## Delivery semantics

The intended distributed execution model is at-least-once. A worker may finish a task while its acknowledgement is lost; therefore handlers must be idempotent or use application-level deduplication. Lease-based claims and fencing tokens will be introduced before claiming crash recovery guarantees.

## Current limitations

The current queue and worker registry are process-local. Restarting the controller loses state, and dispatch only assigns work; it does not yet execute a remote task. The HTTP API is a development control surface, not an authenticated production endpoint.
