# Local runtime semantics

- Status: M2 semantics contract; checkpoint 1 covers the strict-pull linear core
- Depends on: [ADR 0004](../architecture/0004-csharp-api-baseline.md) §4-§5, [ROADMAP](../ROADMAP.md) M2

The local runtime is the semantic reference implementation: the fast harness
that later runtimes (Orleans, M3) must agree with observably. Checkpoint 1
executes linear local graphs under the strongest possible bound; buffers,
overflow policies, parallel operators, and time arrive in later checkpoints
inside this contract.

## Materialization

`LocalDataflowHost.MaterializeAsync(RunnableGraph, CancellationToken)`
validates the document against `LocalStageCatalog` through `GraphCompiler`
(defense: documents this API builds always pass), starts the run, and
returns the `RunHandle`. Materializing one graph twice yields two
independent runs: fresh source enumeration, fresh aggregate state, no shared
mutable anything.

## Execution model (checkpoint 1)

Strict pull. One asynchronous loop pulls one element from the source,
applies the stage functions in order, delivers to the sink, and repeats.
Exactly one element is in flight at any moment; there is no buffer anywhere.
This is deliberately the degenerate case of the demand protocol (credit
fixed at one), so later buffered checkpoints relax a bound rather than
introduce a new model. The source enumerator is created per run and disposed
on every terminal path.

## Terminal states

| Trigger | `Completion` | Result slots | Source enumerator |
|---|---|---|---|
| Source ends | RanToCompletion | resolve with final values | disposed |
| Stage or enumerator throws | Faulted with that exception, unwrapped | fault with the same exception | disposed |
| Materialization token / `DisposeAsync` | Canceled | cancel | disposed |
| `ShutdownAsync` | RanToCompletion | resolve with state so far | disposed |

Shutdown and cancellation are distinct on purpose: shutdown is "stop pulling
and keep what you have" (the seed of drain), cancellation is "abandon the
run". No element is observed after a failing one. Implementation-refined
rules, all tested:

- `DisposeAsync` and `ShutdownAsync` never throw at all — teardown must not
  replace the caller's own exception under `await using`; the outcome stays
  readable on `Completion` and the result task. Both await termination and
  are idempotent.
- A token already canceled at materialization still yields a handle: the
  run starts, observes the token before the first pull, and ends Canceled
  without ever touching the source. Cancellation is an outcome of a run,
  not a failure of materialization.
- The source enumerator is obtained lazily at the first pull and disposed
  on every terminal path. A `Dispose` that throws faults an
  otherwise-successful run but never replaces an existing failure or a
  requested cancellation; a `GetEnumerator` returning null is reported as a
  sentence, not a `NullReferenceException`.
- The loop runs on a dedicated long-running thread: stages are synchronous
  author delegates and pulls are synchronous calls, either may block
  indefinitely, and a blocked thread-pool thread would starve the process.
- Fresh state per run means the aggregate seed; state captured inside the
  author's own lambdas is the author's to keep fresh, and the limit is
  stated rather than implied.

## Slot resolution

`RunHandle.GetValueAsync(slot)` accepts a slot exactly when the slot's
fingerprint equals the run's document fingerprint AND the slot's authoring
nonce equals the run's graph instance nonce (ADR 0004 §4). A foreign slot is
an `ArgumentException` naming which identity failed; the nonce is described
as instance identity without printing its value. The optional cancellation
token cancels the caller's wait, never the run. Resolution is callable
before, during, and after termination and always observes the terminal
state.

## Threading

The run loop is one async flow. All public `RunHandle` members are safe to
call concurrently; concurrent observers of `Completion` and the same or
different slots all see one terminal state.

## Not in checkpoint 1

- buffers, overflow policies, and credit above one;
- parallel or async-callback operators;
- time (no timers, no delays);
- pause/resume;
- runtime metrics and monitors;
- executing documents not built by this process (foreign documents fail
  validation or slot binding by design).
