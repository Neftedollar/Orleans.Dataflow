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

## Checkpoint 5 contract (pause, probes, ValueTask) — as implemented

- **Quiescence** is: every segment has stopped at a point from which it
  takes no further step until resumed, and no callback is executing. An
  element already produced and waiting — in a buffer, in a writer's hand at
  a full boundary, in an async window, at a sink nobody has asked — is
  *held*, not in flight; the strict "nothing exists between stages" reading
  would deadlock pause against backpressure (a source parked in a full
  buffer waits for room only a running downstream segment can make).
- `PauseAsync` returns at quiescence; `ResumeAsync` returns when no segment
  is still held — "moving again" is a fact, not a permission. Stopping
  always wins: a pause never delays cancellation, shutdown, failure, or a
  natural end, and `IsPaused` (observational, best-effort, deliberately not
  a state enum — M5 owns that vocabulary) is false for any stopped run. A
  paused run's control slots keep working per their own policies.
- **Demand-aware probes** live in the `Orleans.Dataflow.Testing` package
  over control slots: `TestSource.Probe` (emit-by-emit lockstep;
  `PullsObserved` is the demand meter) composes the ingress-queue machinery
  rather than duplicating it; `TestSink.Probe` is a rendezvous — the run
  delivers nothing without a `ReceiveAsync` — with terminal expectations
  that return the run's own failure instance. No pending probe wait ever
  hangs: it faults with `ProbeTerminatedException` naming the outcome. One
  documented exception: a graceful shutdown with an element at a probe sink
  and nobody receiving discards that element — the alternative turns
  `ShutdownAsync` into a hang.
- **`SelectValueTaskAsync`/`SelectValueTaskAsyncUnordered`** share the Task
  family's driver through a boundary conversion awaiting each `ValueTask`
  exactly once (pinned with an `IValueTaskSource` that throws on a second
  consumption).

## Not in checkpoint 1

- buffers, overflow policies, and credit above one;
- parallel or async-callback operators;
- time (no timers, no delays);
- pause/resume;
- runtime metrics and monitors;
- executing documents not built by this process (foreign documents fail
  validation or slot binding by design).

## Checkpoint 2 contract (buffers and async stages) — design ahead of code

Checkpoint 2 relaxes the credit-of-one bound only where the author asks for
it, and nowhere else.

**Fusion is the default.** Adjacent synchronous stages keep executing fused
in one pull loop exactly as in checkpoint 1. A boundary exists only where
the author placed a `Buffer` or an async stage; each boundary is one bounded
channel, and each segment between boundaries is one loop. No boundary, no
queue — the operator-fusion row of the capability matrix is this rule.

**`Buffer(BufferOptions)`.** `Capacity >= 1` is required — there is no
unbounded spelling. `OverflowPolicy` is one of: `Backpressure` (default:
the upstream segment waits; this is prefetch, not loss), `DropOldest`
(evicts the oldest buffered element), `DropNewest` (drops the arriving
element; the buffer keeps its contents), `DropBuffer`, `Fail` (faults the
run with `BufferOverflowException`). Drop policies count dropped elements
observably, never silently. Policy semantics apply at the moment the
upstream segment offers an element to a full buffer.

Implementation-refined rules, all tested: a `Buffer` immediately before an
async stage becomes that stage's input channel — one channel, not two, so
`Buffer(8).SelectAsync(...)` holds 8 and "total memory is the sum of
declared capacities" stays literally true (two adjacent `Buffer`s do not
merge; capacities add). Buffer capacity, overflow policy, and async
concurrency are document payload — serializable configuration, not
behavior — so two graphs differing only in capacity have different
fingerprints, and the planner reads the payload, never the authoring
descriptor: what the catalog validates is exactly what the runtime
executes. Every segment runs on its own dedicated long-running thread,
async segments included, because a segment's emission path runs fused
synchronous author stages; one execution model, one terminal discipline.
A failing terminal or callback releases a source parked in a full buffer's
offer — failure reaches it as cancellation, never as silence.

**`SelectAsync(ParallelismOptions, callback)`** — ordered: up to
`MaxConcurrency` callbacks in flight; outputs emitted in input order (head
of line blocks emission, not admission); the callback receives a
`CancellationToken` that is the run's token; a callback failure faults the
run, cancels the other in-flight callbacks, and no later element starts.
**`SelectAsyncUnordered`** — same bounds, emission in completion order.
`ParallelismOptions.MaxConcurrency >= 1` required. `MaxConcurrency = 1`
ordered is semantically the sequential async map.

## Checkpoint 3 contract (operator breadth and core adapters) — design ahead of code

All checkpoint-3 stages are synchronous local stages and fuse per the
checkpoint-2 rule; none introduces a boundary.

- **Stateful per-run operator state** (scan, take, skip, distinct) is
  allocated per materialization, like the aggregate seed; the invariant
  "fresh state per run, captured lambda state is the author's" extends
  unchanged.
- **Operators**: `Scan` (emits each intermediate state; seed not emitted),
  `Take(n)`/`Skip(n)` (n >= 0; Take(0) completes immediately after start),
  `TakeWhile`/`SkipWhile` (exclusive boundary per the naming rules;
  `TakeThrough` inclusive), `Distinct()` (bounded by an explicit required
  capacity — `Distinct(DistinctOptions)` with `MaxTrackedKeys` and a
  documented eviction rejection: exceeding the bound faults the run, because
  silent unbounded key tracking is forbidden and silent eviction changes
  semantics; relaxations arrive with the deduplication policies of M4).
- **Early completion**: `Take` reaching its bound completes the run the way
  a source end does — upstream segments observe downstream completion and
  stop pulling; the source's enumerator is released, including a source
  parked in a full buffer's offer (proven by the deadlock-pattern tests).
  In-flight async callbacks upstream of the take are drained, not
  cancelled — cancelling would surface `OperationCanceledException` inside
  author code to end a successful run; a callback that fails while being
  drained still faults the run, because failure wins. `Take(0)` resolves at
  plan time: the source is never enumerated. `TakeThrough` is the inclusive
  variant of `TakeWhile` over the same predicate polarity
  (`TakeWhile(v => v < 3)` on 1..5 yields 1,2; `TakeThrough` yields 1,2,3).
- **Sources**: `Source.Empty<T>()`, `Source.Single(value)`,
  `Source.Repeat(value, count)` (bounded; unbounded repeat arrives only
  together with `Take`-style bounds and is still spelled with an explicit
  count or a `Take`), `Source.Range(start, count)`, `Source.FromTask(task)`
  (one element, or the run faults with the task's original exception
  unwrapped; a task cancelled elsewhere faults the run — the run's own
  token was never the cause, so reporting cancellation would misattribute
  it), `Source.Failed<T>(exception)`, and `Source.Unfold(seed, generator)`
  where the generator is the try-shape delegate
  `bool UnfoldGenerator<TState, T>(TState state, out T value, out TState next)`
  — chosen over a nullable step struct because the try-shape infers both
  type arguments at the call site, names its outputs, and cannot confuse
  "no more elements" with an element equal to `default(T)`. Author-bounded.
- **Sinks**: `Sink.ForEach(Action<T>)` (awaited per element, the sequential
  callback boundary), `Sink.ForEachAsync(ParallelismOptions, callback)`
  (bounded-parallel callback with the async-stage semantics),
  `Sink.First<T>()`/`Sink.Count<T>()` as result-bearing sinks
  (`First` completes the run early like `Take(1)` and faults on an empty
  source with a documented exception; `FirstOrDefault` variant returns the
  default honestly).
- **`Choose` naming is an open question**: C# has no idiomatic
  option-returning map; `Where`+`Select` covers the semantics today, F#'s
  `Flow.choose` arrives with the F# frontend over the algebra, and a C#
  spelling (nullable-based pair `Choose`/`ChooseValue` or a tuple form) is
  decided in M4 with the operator-breadth ADR rather than guessed now.
- **Controllable time** stays out: the first time-dependent operator (M4
  timing group) brings the clock abstraction with it; nothing in
  checkpoint 3 reads a clock.

## Checkpoint 4 contract (asynchronous ingress and adapters) — as implemented

- **Two stop signals.** A run carries a RunToken (cancellation) and a
  StopToken (cancellation or shutdown). Only the runtime's own waits observe
  StopToken — the ingress queue, the channel reader, `Never` — so shutdown
  releases them; author-owned waits (`FromTask`, `FromAsyncEnumerable`,
  `UnfoldAsync`, `FromAsyncFactory`) receive RunToken alone, preserving the
  slow-source rule: a source that ignores its token delays the stop until it
  yields.
- **`Source.FromAsyncEnumerable<T>`**: fresh `GetAsyncEnumerator(runToken)`
  per run; `DisposeAsync` awaited on every terminal path; cooperative
  cancellation.
- **Control slots.** `Source.Queue<T>(BufferOptions, controlName)` declares
  a control result slot on the queue node's `control` port
  (`local-control@v1`). `RunnableGraph.Control<TControl>(name)` /
  `TryGetControl` recover the typed slot against an authoring-side type
  registry (mismatches name both types). The general rule, now stated on
  `GetValueAsync`: a slot's task completes when its value becomes
  available — terminal results at the end of the run, controls at its
  start. A control task never faults: a run that dies at start still hands
  out its queue, whose every later offer answers `Closed` — the truth a
  producer needs.
- **`IIngressQueue<T>`**: `OfferAsync` returns `QueueOfferOutcome` and never
  throws for queue state — `Accepted`, `Dropped`, `Closed`, `Failed` are
  values. The outcome describes the offered element: `DropOldest` and
  `DropBuffer` evict queued elements and admit this one (`Accepted`; the
  loss lands on the run's drop counter), `Dropped` is `DropNewest`'s
  answer, `Backpressure` waits. `Complete()` ends the source normally and
  drains what was queued; `Fail(exception)` faults the run and abandons —
  the drain-versus-abandon split one level down. `OverflowPolicy.Fail`
  answers `Failed` and faults the run with `BufferOverflowException`.
- **`Source.FromChannel<T>`**: the reader is external state; two runs of one
  channel-reader graph compete for elements, the split is undefined, and
  that is the author's channel to own (union and non-duplication are the
  guarantees). Channel completion completes the run; a faulted channel
  faults it.
- **`Sink.ToChannel<T>`**: `WriteAsync` per element (the writer's
  backpressure is the sink's), `TryComplete(writer)` on completion — early
  termination included — and `TryComplete(writer, exception)` on failure.
  Write-accepted is not consumed. A teardown that throws (a hostile
  writer's `TryComplete`) is guarded: it faults an otherwise-successful run
  and never replaces an existing outcome or hangs `Completion`.
- **Other sources**: `FromFactory`/`FromAsyncFactory` invoke per
  materialization; `Never` parks on a kernel wait; `Cycle` uses a fresh
  enumerator per lap (disposed per lap and on every terminal path) and
  faults on an empty sequence rather than looping silently; `UnfoldAsync`
  takes `Task<UnfoldStep<TState, T>?>` (a named delegate and a nullable
  step record — the async position cannot use the sync try-shape, and both
  type arguments are written once at the call site).
- **Sinks**: `Last`/`LastOrDefault` (First-style empty semantics),
  `Collect(CollectOptions)` with a required element cap that faults with
  `CollectOverflowException` rather than truncating.

**Terminal semantics extend unchanged**: shutdown drains boundaries (a
buffer's contents are delivered, in-flight async callbacks are awaited,
then the run completes with what it has); cancellation abandons buffered
and in-flight work; failure wins over everything queued behind it. The
one-element-in-flight test of checkpoint 1 becomes per-segment:
in-flight-per-segment never exceeds the declared bound, and total memory is
the sum of declared capacities — provable per boundary with gated probes.

## M4 DAG execution model — design ahead of code

The engine grows from a line to a graph. What follows is the shape M4.1
implements, checkpointed like M2 was; ADR 0005 fixes the junction
contracts this model must keep, and nothing below weakens one.

### Plan

- **Channels are keyed by edge, not by chain position.** The linear plan's
  `_channels[index]` — where a segment's position named both its input and
  its output — becomes a table from `GraphEdge` to its bounded channel.
  Everything the boundary machinery does today (policies, closing on
  completion, the offer discipline) is unchanged per channel; what changes
  is only how one is found.
- **Fusion survives inside the branches.** A maximal junction-free chain
  fuses exactly as a linear graph does today, with the same rules for
  where a boundary is mandatory. A junction never fuses: it is its own
  segment, because its pump shape is what defines it.
- **Two new pump shapes join the existing three.** `Pull` (head), `Push`
  (linear), and `Map` (async) are joined by `FanIn` (N readers, one
  delivery path) and `FanOut` (one reader, N writers). A junction is one
  of the two shapes plus a strategy — merge, zip, and interleave are all
  `FanIn` pumps differing in when they pull which reader and when a
  delivery is ready; broadcast, balance, partition, and unzip are `FanOut`
  pumps differing in which writers must have room before the pull and
  which receive the element. The strategies are small and synchronous; the
  waiting stays in the pump, on the segment's own thread, exactly as
  today.

### Completion and stopping across a graph

- The linear watermark (`_completedAt` as an index) becomes per-edge
  state: a stream completes *along an edge*. A segment completes its
  output edges when its junction rule says its inputs are done — all of
  them for merge and interleave, the first for zip, the last for concat —
  and closing an edge's channel is what carries the completion downstream,
  as it already does.
- Stopping propagates upstream per edge, and a fan-out segment stops
  pulling only when *every* output edge has stopped; until then a
  completed leg merely leaves the delivery set. This is ADR 0005's rule 3
  as engine mechanics.
- **Terminals are counted, not singular.** A graph may end in several
  sinks; the run completes when every terminal has completed, and each
  result slot resolves from its own terminal's fold. The countdown that
  terminalizes a linear run generalizes to a count over terminals with no
  change of meaning.

### Cycles

Validation refuses a cycle unless it passes a boundary whose policy can
answer without downstream room (ADR 0005). Execution needs nothing new
beyond that rule: a legal cycle is edges and channels like any others, and
the boundary that made it legal is what breaks the wait. The subtle part
is completion — a cycle's segments feed each other, so "inputs done" can
only arrive from outside the cycle; the plan detects strongly connected
components at validation time and completion enters a cycle only when
every edge into the component has completed, at which point the component
drains and its channels close in dependency order.

### Controls, pause, and probes

The pause gate, the control slots, the kill switch, and the demand-aware
probes generalize without new concepts: quiescence is still "every
segment parked or idle and no callback in flight" — the counters never
depended on the plan being linear — and a probe attaches to an edge.

### Checkpoints for M4.1

1. **DAG plan and fan-out**: edge-keyed channels, multiple terminals,
   broadcast/balance/unzip, multiple named result slots resolving
   per-terminal. Proves the plan model and the FanOut pump.
2. **Simple fan-in**: merge, concat, interleave — the FanIn pump with
   strategies that never hold a partial row.
3. **Row-building fan-in**: zip and combine-latest — held rows, eager
   completion for zip, frozen-leg semantics for combine-latest.
4. **Partition and cycles**: the routed-element hold, out-of-range
   failure, SCC detection at validation, a legal cycle executing and
   completing from outside in.
5. **Control-plane generalization**: pause quiescence, shutdown drain,
   cancellation, and probes proven across branching topologies; the
   bounded-memory suite extended to junctions (held elements counted
   against ADR 0005's stated bounds).

Each checkpoint lands with its tests and its as-implemented notes here,
replacing this section's future tense the way M2's checkpoints replaced
theirs.
