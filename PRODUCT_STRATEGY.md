# ORBIT — Independent Product Strategy

## Product identity
ORBIT is a general-purpose distributed job and workflow orchestration engine. It coordinates work across workers and services; it is not a robot fleet controller and does not own application-specific business policy.

## Product boundary
- **Core:** job lifecycle, scheduling, leases, worker registration, retries, recovery, persistence, observability, and controller APIs.
- **Adapters:** pluggable task executors, storage backends, queue transports, and application-specific scheduling policies.
- **Applications:** standalone examples and integrations maintained outside the core.

The core must not depend on FireBot, robot concepts, SETU, PRAVAH, or FireBot schemas. FireBot support belongs in an optional adapter.

## Independent use cases
1. Data-processing pipeline: distribute independent transformation jobs across worker processes.
2. Backend operations: coordinate report generation, scheduled maintenance, and asynchronous service tasks.
3. Build automation: execute bounded build/test tasks across a worker pool.

## Product requirements
- Durable job state and transactional claiming before claiming restart-safe execution.
- Idempotency keys and explicit at-least-once semantics.
- Pluggable task handlers with bounded execution and cancellation.
- Config validation, structured errors, metrics, and trace context.
- Documented HTTP/API contracts and version compatibility.
- Independent build, test, release, and quick-start workflows.

## Non-goals
ORBIT does not provide hard real-time control, exactly-once side effects, consensus guarantees, or application-specific safety policy by default.

## FireBot adapter
An optional adapter may map FireBot mission tasks to generic ORBIT jobs. It must remain removable and must not put FireBot types in the ORBIT core.

## Readiness gates
- [ ] Clean standalone setup without FireBot.
- [ ] Durable storage and idempotent submission.
- [ ] Two non-FireBot examples with tests.
- [ ] Failure/restart/duplicate/partition tests.
- [ ] Versioned API and independent release notes.
