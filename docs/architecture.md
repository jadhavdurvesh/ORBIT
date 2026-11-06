# ORBIT architecture

## Runtime components

- **Controller API:** accepts jobs, registers workers, grants claims, validates acknowledgements.
- **Job store:** owns state transitions and atomically claims queued jobs.
- **Worker registry:** tracks worker metadata and expires stale registrations.
- **Worker agent:** polls for a job, executes a supported handler, and reports success or failure.
- **Lease reaper:** periodically recovers jobs whose claim lease has expired.

## Job lifecycle

```text
queued --claim--> running --success--> succeeded
                     |
                     +--failure, attempts remain--> queued
                     |
                     +--failure, attempts exhausted--> failed
                     |
                     +--lease expires, attempts remain--> queued
                     |
                     +--lease expires, attempts exhausted--> failed
```

## Safety invariants

1. A job is claimed while holding the store's exclusive lock.
2. Only the worker currently recorded on a running job may acknowledge it, and every claim receives a fresh cryptographically random lease token. A stale token from an earlier attempt is rejected.
3. Each claim increments the attempt counter.
4. A retry is possible only while attempts remain below the configured maximum.
5. Terminal jobs cannot transition back to queued.
6. A running job must have both a worker identity and lease expiry.

## Delivery semantics

A lease provides recovery, not exactly-once execution. The per-claim fencing token prevents a stale attempt from acknowledging a later retry, including when the same worker identifier receives the job again. A worker can perform an external side effect and crash before acknowledging completion; the controller may then retry the job. Production handlers must be idempotent or use external idempotency keys. The current implementation does not provide fencing tokens across controller restarts.

## Current deployment boundary

The job store and worker registry are in-memory and local to one controller process. A restart loses all state. The worker agent communicates over plain HTTP and the API has no authentication or authorization. Do not expose this development deployment to an untrusted network.

## Next engineering milestones

Durable storage must use transactional claim semantics. Multi-controller operation requires a defined consistency model and leader/lease ownership; simply adding more API instances would create split-brain risks.
