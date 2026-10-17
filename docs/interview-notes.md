# Interview notes

Questions about Tasklane's design, with model answers. Grows with every milestone.

## Q1. A worker leases a job with a 30 s visibility timeout, and the job takes 45 s. Nothing crashes. What happens?

At 30 s the lease expires. The server can't tell a slow worker from a dead one, so the reaper makes the job available again and a second worker leases it: the job now runs twice.

- **Heartbeats prevent it** in the normal case: the worker extends its lease while it works.
- **The fencing token protects the queue's state** when heartbeats fail (GC pause, network blip): the second lease gets a new token, so the first worker's late ack is rejected (`409 FAILED_PRECONDITION`) and only the current holder can complete or fail the job.
- **Idempotent workers protect the outside world:** the side effect (the email) may still happen twice, which is inherent to at-least-once delivery. Workers use the job ID as an idempotency key, e.g. with the email provider.

Key words: heartbeat (prevent), fencing token (protect state), idempotent worker (protect side effects).
