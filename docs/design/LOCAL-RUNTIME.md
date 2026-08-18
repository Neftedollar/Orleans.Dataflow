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
the boundary that made it legal is what breaks the wait.

**Corrected by checkpoint 4 rather than left standing.** This paragraph used
to continue: "completion enters a cycle only when every edge into the
component has completed, at which point the component drains and its
channels close in dependency order". That is wrong, and wrong in the
direction that destroys work. The elements circulating inside a loop are a
live stream whether or not anything outside it is still producing, and for
an iterative computation that is the normal state rather than an edge case:
the external input is a seed, and everything after the seed is the loop
talking to itself. Ending the loop because its external inputs ended would
kill a computation that is still running. Closing every edge into a cycle
therefore says nothing at all about when that cycle ends. What is true is
stated in checkpoint 4's section below — a cycle ends from inside, by a
stage **on the loop** that ends its own stream, or from outside by a stop,
and otherwise it does not end.

### Controls, pause, and probes

The pause gate, the control slots, the kill switch, and the demand-aware
probes generalize without new concepts: quiescence is still "every
segment parked or idle and no callback in flight" — the counters never
depended on the plan being linear — and a probe attaches to an edge.

**Half of that survived checkpoint 5 and half of it did not.** The counters
really are shape-blind: they count segments and callbacks, and a junction is a
segment, so nothing in `LocalPause` needed a line. What the claim quietly
assumed is that every wait a segment can take reports itself, and one did not —
a channel sink's write, which is this runtime's own wait on a channel the
author owns. That is a hole the linear suite could have found and never looked
for, and it is described with its fix in checkpoint 5's section below. "A probe
attaches to an edge" is the half that was aspirational: a probe is a stage, so
it attaches where a stage can stand, which is the head or the end of a branch.
In a graph that turns out to be enough for every contract this milestone
states — a branch end is one leg of a split or one input of a join — so the
sentence is downgraded to what is true rather than paid for with machinery
nothing needed.

### Checkpoints for M4.1

1. **DAG plan and fan-out** — *as implemented, see below*: edge-keyed channels,
   multiple terminals, broadcast/balance/unzip, multiple named result slots
   resolving per-terminal. Proves the plan model and the FanOut pump.
2. **Simple fan-in** — *as implemented, see below*: merge, concat, interleave —
   the FanIn pump with strategies that never hold a partial row.
3. **Row-building fan-in** — *as implemented, see below*: zip and
   combine-latest — held rows, eager completion for zip, frozen-leg
   semantics for combine-latest.
4. **Partition and cycles** — *as implemented, see below*: the routed-element
   hold, out-of-range failure, cycle detection at validation, a legal cycle
   executing, and the corrected statement of when one ends.
5. **Control-plane generalization** — *as implemented, see below*: pause
   quiescence, shutdown drain, cancellation, and probes proven across branching
   topologies; the bounded-memory suite extended to junctions (held elements
   counted against ADR 0005's stated bounds).

Each checkpoint lands with its tests and its as-implemented notes here,
replacing this section's future tense the way M2's checkpoints replaced
theirs.

## M4.1 checkpoint 1 (DAG plan and fan-out) — as implemented

The plan above is what was built; what follows is where it needed a decision it
did not already contain, and what this checkpoint does not do.

**A junction's arity is its edges, and its legs are ports.** A stage
specification declares a port list rather than an arity, so `broadcast` and
`balance` declare `out-0` … `out-7` — the first two required, the rest
ignorable — and `unzip` declares `left` and `right`, both required. Which legs
an occurrence has is therefore stated by the edges that reach them and by
nothing else: there is no arity payload, because a number written beside the
edges would be a second statement of the same fact and two statements can
disagree. Eight is a stated ceiling rather than a natural one, exactly as the
four digits of an automatic node name are.

This is also the whole of "junction-aware validation": the graph compiler
needed no new rule. *Connected exactly per its cardinality* already falls out
of the two rules that were there — a document structurally carries at most one
edge per port address, and the compiler requires an edge at every port that is
not optional or ignorable. What was missing was a stage whose port list has
more than one output in it.

**Channels are keyed by plan edge.** A segment says which channels it reads and
which it writes; the planner is where a document's `GraphEdge` becomes one of
those indices, and nothing below it needs the edge again. Everything the
boundary machinery does is unchanged per channel.

**Completion walks upstream per edge.** A segment that stops closes every
channel it was reading; each closed channel reaches the segment that was
writing into it and lowers that segment's count of live outputs; a segment
whose count reaches zero stops in its turn. A junction with a live leg left
therefore keeps feeding it, which is ADR 0005's third shared rule as mechanics,
and a linear plan is the degenerate case — one output per segment, so the walk
runs to the source exactly as the old watermark did.

**Terminals are counted.** A plan carries one *ending* per sink: the segment it
stops at, the seed its terminal folds from, and the slot it resolves. The run
keeps one state and one settled slot per ending and settles when every segment
has stopped. The outcome stays single: a failure anywhere, a cancellation, or a
first-element sink with no element faults or cancels **every** slot, because a
run ends once.

**The FanOut pump asks for room and then pulls,** which is both the demand rule
and the held-element bound: the one element a junction ever holds outside a
declared buffer is the one it is placing. Broadcast and unzip need every live
leg to have room and are the same loop with a projection per leg; balance needs
one and places by rotation among the willing, holding its element rather than
losing it if the leg that reported room leaves first. A junction never fuses
with anything and is a boundary on its input and on every leg, so a `Buffer`
written immediately before one or immediately on a leg is that channel rather
than a second one behind an implicit handoff — the rule a buffer in front of an
asynchronous stage already followed.

**Two consequences worth stating rather than discovering.** An overflow policy
on a *broadcast* or *unzip* leg keeps working, because such a junction offers to
every live leg: a leg declared `DropOldest` drops rather than pacing its
siblings, and one declared `Fail` fails. An overflow policy on a *balance* leg
is unreachable, because a balance picks a leg that really has room and routes
around a full one whatever policy it declared — and a leg that declared a
dropping policy always has room, so a balance is drawn towards it.

**An unzip's halves are behavior.** Which member of a row is its left half is a
statement about an element type, and an element type never appears in a local
document; the document states that the node splits one stream into two, and the
binding states how. Unzip is therefore literally broadcast with a projection per
leg, which is what ADR 0005 says it is.

**What this checkpoint does not do.** There is no authoring spelling for a
junction — the C# graph builder is M4.2, and the tests here build documents and
binding tables directly, which is the durable half of a fan-out graph in any
case. There is no fan-in, no partition, and no cycle: two sources are refused
as two runs written in one document, a second edge into one node is refused
structurally, and a cycle is unreachable from the head and refused as a
component nothing reaches. Pause, shutdown, and cancellation are proven to work
across a fan-out rather than proven in general — the control-plane
generalization is checkpoint 5, and the pause of a branching run is claimed here
only as "it comes to rest and moves again".

## M4.1 checkpoint 2 (simple fan-in) — as implemented

The plan above is what was built; what follows is where it needed a decision it
did not already contain, where it turned out to be describing something the
engine cannot quite mean, and what this checkpoint does not do.

**Several sources are one graph exactly when they converge.** The planner's
old rule was "exactly one node begins a chain", and the rule that replaced it
is not "any number of them" but connectivity: a walk starts at every node
nothing feeds, and the document has to be one component when its edges are read
in both directions. Two chains side by side still fail, and the diagnostic now
says what is actually wrong with them — not that there are two sources, but
that no junction joins what they feed, so one outcome would have to speak for
two streams that never meet. Reachability alone could not have told the two
apart, because following the edges from every head reaches every node of both.

Three refusals are the whole of what stays forbidden. A node fed by more than
one stream that is not a fan-in — the shape a chain cannot execute, now stated
against the binding rather than against the edge count alone. A component
nothing joins. And a cycle, which is refused exactly as it was before and for a
sharper reason than before: every node of a cycle is fed, so no walk from a
source reaches one, and a fan-in whose input a cycle feeds is never built at
all, because the last of its arrivals never comes. Cycles stay checkpoint 4.

**A junction is built by the last branch that arrives at it.** A pump that
reads several channels cannot exist until every one of them does, so a branch
that runs into a fan-in ends there, closes whatever it was building at a
boundary, and records that channel against the port it arrived at; the arrival
that fills the last declared input is the one that allocates the segment. Which
input a channel is is therefore stated by the port an edge terminates at and by
nothing else — the mirror of a leg being stated by the port an edge leaves.
Both buffers behave the way a buffer in front of an asynchronous stage does: one
written immediately before a junction is that input's own channel, and one
written immediately below it is the junction's output channel, rather than a
second channel with an empty relay segment between them.

**The FanIn pump asks for room and then reads,** which is the mirror of the
fan-out rule and is again both the demand rule and the held-element bound: the
one element such a junction holds outside a declared buffer is the one it is
placing, because it never takes an element out of an input it has nowhere to put.
That is one number rather than an argument, and it is counted the way the buffer
suite counts — the greatest number of elements a run held at once, read after
the run is over rather than sampled at a moment that might have been one step
early. A junction that read first and waited afterwards holds one more, and
every bounded-memory test in the fan-in suite reports that difference.

**The three strategies are three answers to one question.** A merge scans from
a cursor for an input that has something and moves the cursor past the one it
took, which is round-robin among the ready ones: a producer that is merely
faster cannot keep an element that has already arrived at another input
waiting, and a junction that scanned in port order every time would starve
every input behind an input that never runs dry. When nothing is ready it waits
on every live input at once, with the waits cached per input for the reason the
fan-out caches its room-waits — a wait-any abandons the tasks it did not pick,
and abandoning a channel waiter per pass is a leak the channel remembers. A
concat reads one input to its end and moves to the next. An interleave reads a
declared number of elements from the input whose turn it is, waiting for that
input even when another has something ready; that head-of-line wait is what its
segment size buys, and it makes the output a function of the inputs rather than
of the scheduler. A completed input leaves the rotation and the remainder
continues in order. All three end when the last of their inputs has ended.

**Failure needed no code at all.** ADR 0005's first shared rule — an input's
failure fails the run whether or not that input was the one being read — is the
engine's ordinary one seen from a new position: the failure is recorded by the
segment that was feeding the input, recording it cancels the run's token, and
every wait this pump takes is taken on that token. A junction asleep on the
inputs that are healthy is woken by the failure of one that is not.

**Where the design was describing something this engine cannot mean.** ADR 0005
says a concat gives demand only to the active input, "so a source that is
expensive to start is not started early". This engine starts every segment when
the run starts, so the sources of the inputs behind the active one are running
from the first moment; what the junction does is not read their channels. The
honest form of the promise is therefore backpressure rather than laziness: such
an input fills its own bounded channel, its source parks holding one more
element, and no third element is ever pulled for as long as the junction is
busy elsewhere — one channel plus one hand, exactly the bound a declared buffer
on that input widens. The consequence the ADR names is real but arrives earlier
than its sentence suggests: a source that fails at open fails the run as soon as
its channel has room for the attempt, which is at once, and not when its turn
comes. Deferring the start of an input's source is a real feature and it is not
this checkpoint's; naming the difference is.

**A split feeding a join that waits head-of-line needs a buffer, and nothing
checks that it has one.** This is the first shape in which two junction
contracts can be individually satisfiable and jointly impossible, and it was
found by running it rather than by reading it. A broadcast pulls only when every
live leg has room; a concat reads one input to its end before touching the next;
an interleave waits for the input whose turn it is even when another has
something ready. Wire a broadcast's two legs straight into a concat, or into an
interleave whose segment size is more than one, and the run stops before its
second element: the junction is waiting for a leg the split cannot fill until
the other leg is drained, and nothing drains it. A declared buffer on the legs —
one deep enough for the head-of-line depth the junction's contract implies, which
is the segment size for an interleave and the whole input for a concat —
resolves it, and the interleave case with two-element buffers is a test here,
alive and exactly determined. This is the same class of statement ADR 0005 makes
about cycles: a wait that only the waiter could release is a deadlock by
construction. The difference is that the cycle rule is enforced at validation and
this one is not; a liveness check over junction contracts is not something this
checkpoint has, and the honest position is that the shape is documented rather
than refused.

**An interleave is the one junction with a payload.** How many streams a
junction joins is stated by its edges, so no junction writes an arity down; how
many elements a rotation takes from one of them before moving on is not an edge
at all, so it is written into the document under `local-interleave-parameters`
and validated as a positive integer by the very reader the runtime uses. Zero is
a real count for a take and a skip and is not one here — a rotation that takes
nothing from an input is a junction that never emits.

**Eight inputs, the first two required.** The ceiling mirrors the fan-out's and
is stated for the same reason. "Optional" on an input port is what "ignorable"
is on an output port: the edges of a document say how many streams a given
occurrence joins, and the ports past the second are inputs a graph may leave
unwired. Nothing about junction validation needed a new rule here either — an
input port address carries at most one edge, and the graph compiler already
requires an edge at every port that is not optional.

**What this checkpoint does not do.** There is still no authoring spelling for
any junction: the C# graph builder is M4.2, and the tests here build documents
and binding tables directly. There is no zip and no combine-latest — the two
junctions that hold a partial row are checkpoint 3, and a pump that never holds
one cannot pretend to be them. There is no partition and there are no cycles.
The control plane is proven to work across a fan-in rather than proven in
general: a paused joining run comes to rest and moves again — including a pause
that lands on a junction asleep on its inputs, which is the case the wait
discipline exists for and the one a run arranged so that there is no other way
to be quiet actually tests — a shutdown drains it in the junction's own order, a
cancellation ends it, and the general statement is still checkpoint 5.
And one property of the local planner is worth stating rather than discovering:
where a document's stage reference and the binding table's kind declare the same
ports, the binding is what executes — a `merge` node bound to an interleave is
refused only because it has no segment size to read, and a `broadcast` node
bound to a balance has been executing as a balance since checkpoint 1. The
binding table is the statement of behavior by design; that it can disagree with
the document about *which* junction a node is has not been made a diagnostic.

## M4.1 checkpoint 3 (row-building fan-in) — as implemented

The plan above is what was built; what follows is where it needed a decision
it did not already contain, where a sentence of the design turned out to be
looser than it sounds, and what this checkpoint does not do.

**A second joining pump rather than two more strategies in the first.** The
design offered the choice and this is the answer: what a pump *is* is how
many reads stand between two deliveries, and that is exactly what these two
junctions change. A merge, a concat, and an interleave deliver the element
they read and hold nothing between elements, so one loop with one read and
one delivery is all three of them; a zip delivers one element for every N it
reads and a combine-latest delivers zero or one for every one, and both of
them therefore hold a row *across* passes. A loop that carries only a cursor
cannot do that, and a loop that carried a row and a cursor and a flag saying
which of the two loops it was would be two loops written on top of each
other. What the two shapes genuinely share is shared as code and not as
prose: the wait-any with its cached per-input waits, the room check, the
pause bracket, and the delivery path are the very ones the simple fan-in
uses, so there is one place where a wait is taken on the run token and one
place where an offer applies a boundary's policy.

**The combiner is behavior, and the arity is still the edges.** A junction
that builds a row needs to know how to build it, and which member of a row
each input contributes is a statement about element types, which never
appear in a local document — the same reason an unzip's halves are a
binding. So zip and combine-latest carry a combiner and no payload at all,
and how many inputs it receives is stated by the edges exactly as it is for
every other junction. The combiner's shape is `Func<object?[], object?>`,
pinned at authoring rather than recovered by reflection the way an unzip's
projections are, and the difference is the arity: a projection is a
one-argument function whose type arguments the delegate names, while a
combiner would need one delegate shape per number of inputs, so a graph
joining nine streams would have no shape at all. The array is fresh per row,
copied out of the junction's own slots, because those slots keep changing —
a zip releases them the moment the row is placed and a combine-latest writes
over one of them on every arrival — and an author who kept the array they
were handed would otherwise watch a row they had already been given empty or
change. That is a promise a combiner cannot check from inside itself, so it
is tested by keeping every row and reading them all after the run is over.

**Room first, read second, with the row as the unit of demand.** The rule is
the one every junction here follows and it is sharpest in this shape: a zip
reads one element from every input against one unit of downstream demand. An
input that has already given the pending row its column is not read again
until that row is emitted, which is what makes the elements of one row the
i-th of every input rather than whatever arrived; and because the room is
secured before any of the reads, a junction that has nowhere to deliver
holds a partial row and does not start filling another. Both halves are
counted the way the buffer and fan-in suites count — how far a held source
gets, read after the run is over. With the sink parked, four elements leave
each input and no fifth: one in the row that reached the sink, one in the
row in the junction's output channel, one in the input's own channel, one in
the source's hand, and nothing at all inside the junction. With three inputs
and the slowest one held, four elements leave each fast input and no fifth:
one in the emitted row, one in the column being held, one in the channel,
one in the hand — two columns held at once, which is N−1 for three inputs,
and a fifth element would be the same input pulled twice for one row.

**Eager completion discards the partial row, by name.** A zip completes as
soon as an input it still needs has ended, and the columns it was holding
are cleared where the completion is decided rather than left to fall out of
scope: a row missing a column can never be completed, and a junction that
kept the other columns would be holding elements for a delivery that cannot
happen. Completing is also what releases the inputs that were still live —
the junction closes every channel it reads, which stops the segments feeding
them exactly as a completion arriving from downstream does, and an endless
input on the other leg of a zip whose short leg ran out has its enumerator
released rather than its thread parked forever. Two things about "eager" are
worth stating rather than discovering. A completed input whose element is
*already in the pending row* does not end the row: `zip([1,2],[1])` emits
`(1,1)` and then completes, which is Rx's answer, and it falls out of the
pump reading a column before it ever asks whether that input has more. And
"as soon as" means at the junction's next look at an input it still needs:
when one needed input ends while another needed input is merely silent, the
junction is asleep on both and acts on the end when the silent one answers,
consuming one element it then discards with the row. That element stays
inside the N−1 bound and nothing observable about the run's outcome changes,
so the looseness is recorded here rather than paid for with a wait that
returns on every end and costs the merge a pass; a run parked on such an
input could not settle in any case, because a source asleep in one of this
runtime's own waits is released by shutdown and cancellation and not by a
completion below it.

**Combine-latest is Rx's operator and not Akka's `zipLatest`.** Nothing is
emitted until every input has produced at least once — an arrival before
that updates the junction's state and leaves nothing at all, which is
provable as an input that produces everything it has and ends while the sink
has still received nothing. After that every arrival emits one row carrying
the latest element of every input, and an input that completes freezes its
last element into every later row: one element produced once appears in nine
rows, which is the operator holding N by construction rather than by
counting. The junction completes when *every* input has, which is the whole
of what separates it from a zip standing in the same place — a graph in
which a zip emits one row and completes gives this junction three. And an
input that completes without ever producing means no row can ever be built:
such a run reads the inputs that are live to their end, emits nothing at
all, and completes cleanly rather than failing or stopping early.

**A split feeding a zip needs nothing between them, and that is not luck.**
The head-of-line hazard checkpoint 2 documented has a cousin here and the
cousin is benign, which is worth stating as loudly as the hazard was. A
broadcast pulls only when every live leg has room and then gives the same
element to all of them, so each leg receives exactly one element per element
pulled. A concat wants one leg drained to its end and an interleave with a
segment above one wants several elements from one leg before it touches
another, and the split cannot supply them while the other leg is undrained —
a wait only the waiter could release, resolved by a declared buffer as deep
as the head-of-line depth. A zip wants exactly one element from every leg
per row, which is the same number the split supplies, and it never waits on
one leg while refusing to drain another: the two contracts are one shape
read from opposite ends. So the diamond runs on handoffs of one element with
no buffer anywhere, and its output is an exact sequence rather than the
multiset a merge could report — which is also what finally proves the unzip
row's claim end to end: a row split into halves, each half transformed on
its own, and the halves zipped back together realign positionally with no
skew. A combine-latest below a broadcast is easier still, because it takes
whichever leg has something and no leg ever waits behind another; what it
emits there is genuinely a scheduling question and the test says so rather
than asserting a sequence it could not promise.

**What this checkpoint does not do.** There is still no authoring spelling
for any junction: the C# graph builder is M4.2, and the tests here build
documents and binding tables directly. Nothing checks that a combiner
expects as many elements as the junction has wired inputs, because a
`Func<object?[], object?>` does not say — a combiner built for a different
number is the author's own mismatch and is reported as whatever their code
raises; the generic signatures of the builder are what will make it
unreachable. There is no partition and there are no cycles. The control
plane is proven to work across these two junctions rather than proven in
general: a pause reaches a zip holding a partial row and a combine-latest
that cannot emit yet — the states only these pumps have — a shutdown drains
both, a cancellation ends both, and the general statement is still
checkpoint 5. And the bounds are proven as the greatest number of elements a
run absorbed, not as a measurement of the junction's own memory: N−1 and N
are what the counting on the source side implies given where every other
element in the run must be, which is the same argument every bounded-memory
test in this suite makes.

## M4.1 checkpoint 4 (partition and cycles) — as implemented

The plan above is what was built; what follows is where it needed a decision it
did not already contain, where a sentence of the design turned out to be wrong
rather than merely loose, and what this checkpoint does not do.

**A cycle does not complete from outside, and the design said it did.** This is
the correction, stated first because everything else about a cycle follows from
it. The design's claim was that "completion enters a cycle only when every edge
into the component has completed, at which point the component drains and its
channels close in dependency order". Nothing in this engine does that, and
nothing should. Closing every external input of a loop is not a statement about
the loop: the elements circulating inside it are a live stream, and for the
graph a cycle is usually written for — an iterative computation whose external
input is a seed — that is the whole run rather than its tail. A rule that ended
the loop when its seed ran out would end the computation at the moment it
started working. What is true is smaller and has three cases.

1. **From inside, by a stage on the loop that ends its own stream.** A `Take`,
   a `TakeWhile`, a `First` — anything that completes — standing **on the
   cycle** ends the loop and the run: the completion walks upstream around the
   loop the way it walks up a chain, the junction's inputs close, the segments
   below drain, and the run reports what it accumulated. This is the author's
   exit and the only end a cycle has of its own accord.
2. **From outside, by a stop.** Cancellation abandons a loop as it abandons
   anything else, because every wait a junction takes is taken on the run's
   token. Shutdown needed one new thing and it is the smallest one available:
   **a shutdown cuts every feedback edge.** A feedback edge is where work
   enters a graph a second time, so it is the loop's own source, and "stop
   pulling" said to a source is "stop re-admitting" said to a loop. What was
   queued in that channel is drained through it, what is already circulating
   leaves by the exit the graph has, and the junction that was reading it sees
   its last input end and completes. Nothing a shutdown of an acyclic graph
   would have kept is discarded.
3. **Otherwise, not at all.** A cycle whose elements all die inside it — a
   filter that eventually drops everything, a routing function that eventually
   sends everything out — goes **quiet** rather than completing. Every pump in
   it is then asleep on a channel that will never produce and will never close,
   because the only thing that could close it is the loop itself. The run stays
   alive until it is shut down or cancelled. This engine does not detect a
   quiet loop and deliberately does not guess: the detection is sound only as
   "every segment of the component is idle, every channel inside it is empty,
   and every channel into it is closed and empty", which is a distributed
   termination problem with a racy answer, and an early guess would truncate a
   run silently. A hang that an author can see and stop is a better answer than
   a completion that was not true. The honest promise is therefore: **write the
   loop's exit as a stage on the loop, or stop the run.** This case is pinned
   as an assertion rather than left as a sentence: a loop with a filter that
   eventually drops everything is run to the point where nothing can wake it
   again, and at that point everything it produced has been delivered and the
   run has not ended.

**And the trap that follows from case 1, stated loudly.** A `Take` on the
*exit leg* of the loop's fan-out does **not** stop the loop. When that leg's
downstream completes, the leg leaves the junction's delivery set and the
junction goes on feeding the legs that remain — which for a feedback fan-out is
the loop itself, so the elements circulate forever with nowhere to go. That is
ADR 0005's third shared rule working exactly as written; it is a trap only
because a loop is self-sustaining where a chain is not. The exit has to be on
the cycle.

**Legality is a walk over the graph with the relieving boundaries removed.**
ADR 0005 says a cycle is legal exactly when every cycle passes at least one
boundary that can answer without waiting for its own downstream. "Every cycle"
is what makes the naive reading wrong: a component may contain a dropping
buffer and still contain a cycle that avoids it, and enumerating cycles to tell
them apart is exponential. Deleting the relieving nodes and asking whether any
cycle survives is the same claim answered in one depth-first walk, and the walk
reports the surviving loop's node path because "there is a cycle" is not
something an author can act on. A relieving boundary is a declared `Buffer`
whose overflow policy is not `Backpressure` — dropping, discarding, or failing
are all answers, and a failing run is not a hanging one — and nothing else is,
the implicit handoff between two segments least of all. A backpressuring buffer
of any capacity only postpones the deadlock and is refused like a handoff.

**Two more refusals that a cycle makes reachable for the first time.** A cycle
nothing outside feeds can never hold an element, so a run of it would idle
forever in half its segments; it is refused by name rather than left to the
connectivity message, which would say only that some nodes went unvisited. And
a graph every one of whose branches runs back into a junction has no terminal
at all: without cycles that shape does not exist, because following the edges
of a finite acyclic graph always reaches a node that feeds nothing.

**The rule lives in the planner, not in the graph compiler.** ADR 0005 says
"the graph compiler enforces this as a validation rule". The graph compiler is
catalog-generic and a relieving boundary is local-vocabulary knowledge — which
stage is a buffer, and what its payload's overflow policy says — so the rule
sits where every other shape rule of this runtime sits, beside "two sources
that never meet" and "a node fed by more than one stream that is not a fan-in".
The ADR's substance is kept exactly: the graph is refused before anything
executes, and the diagnostic names the cycle's node path.

**M0's no-self-loop rule is gone, as ADR 0005 ratified.** `GraphEdge.Create`
and `GraphDocument`'s structural invariants refused a self-loop and said in
their own messages that they were doing so only until cycles arrived with a
boundary contract. They have. A self-loop is a cycle of one node and is now
tested by the cycle rule like any other loop — refused with the same sentence,
naming the same path — and the definition plane no longer has an opinion about
loops at all, which is right for a plane that does not know what a boundary
policy is. This is the one change outside `Orleans.Dataflow` in this
checkpoint.

**A back edge always terminates at a fan-in, and the plan rests on it.** The
walk that compiles a document is acyclic by construction: it starts at the
heads, and a junction is built by the last branch that arrives at it. A cycle
breaks that, because the branch carrying a feedback edge begins *below* the
junction it feeds — so the junction would wait for a branch that waits for the
junction. What breaks the deadlock is that the feedback inputs are known before
the walk begins, as the back edges of a depth-first walk rooted at the heads.
A junction is then built when every input **from outside the cycle** has
arrived, with a place kept for each feedback input, and the arrival that
eventually comes round fills the place in the list the segment is already
reading. That works because a back edge's target always has a tree edge into it
as well, so it always has more than one incoming edge, so the rule that a node
fed by more than one stream must be a fan-in has already required it to be one —
and because at least one of its inputs is a tree edge, so it always has an
arrival from outside to be built by. Both facts are checked rather than
reasoned about.

**Partition reads first and waits second, and it is the only pump that does.**
Every other junction in this engine secures room before it takes an element,
because taking one it cannot place is a read-ahead no contract allows. A
partition cannot: the room it needs is room on the leg its element belongs on,
and which leg that is is what the author's function answers from the element
itself. So the order inverts and the bound stays one element for a different
reason — one element is read, routed once, and held until its target can take
it. ADR 0005's table says "pulls upstream when its target has room" and its
text says "runs the author's routing function once per element and then waits
for that element's target specifically"; the text is what an implementation can
mean, and it is what this implements.

**Head-of-line, one element deep, and that is the operator.** While the held
element waits for its own leg, no other leg is offered anything and the input
is not read again, so a leg whose elements are queued upstream starves for
exactly as long. It is proved the way every bound in this suite is proved — how
far a held source gets, read after the run is over — and it is not a defect to
be worked around inside the junction: it is the difference between a partition
and a balance, and an author who wants the other behavior wants the other
junction. A declared buffer on the slow leg buys slack, exactly as one does
under a concat. The routing function runs exactly once per element, on the
segment's own thread, never while the run is paused and never for an element
the junction did not take — the keyed adapter's read-once rule in a second
place and for the same reason, since a function an engine may call again is one
that has to be pure and nothing here can require that of an author.

**Out of range fails the run; a leg that has left does not.** The two look
alike and are not, and getting this wrong is the mistake this checkpoint made
first and then measured. Out of range is ADR 0005's own decision and it is
right for the reason the ADR gives: the answer names nothing at all, there is
no such stream, and discarding the element would hide a defect. The sentence
carries both the answer and the wired arity, because how many legs a junction
has is stated by its edges and by nothing the function can see, so an answer of
three against two legs is otherwise indistinguishable from an off-by-one. A leg
that has **left** is something else: it is a stream that *ended*, and this
engine already answers that everywhere — an element arriving at a channel a
downstream completion closed is abandoned rather than dropped, counted, or
failed on. Making this junction the exception was implemented, and it was wrong
twice over. It contradicts the third shared rule, which says a completed leg
stops feeding rather than stopping the world. And it makes the outcome of an
ordinary run a race: a completion walking upstream closes legs while elements
are still travelling towards them, so the same graph ended successfully or in
failure depending on which arrived first — which a full-suite run duly
demonstrated by failing once and passing eight times. A contract that cannot
say which is not one. So a routed element whose leg has gone is abandoned, the
junction goes on feeding the legs that remain, and it completes upstream when
the last of them leaves. An abandoned element is not counted as a drop, for the
reason the drop counter already gives: nothing discarded it, the stream it was
travelling to had ended. A mode in which any leg leaving ends the run is the
declared-variant escape hatch ADR 0005 describes — the same shape as an
eager-cancel broadcast — rather than a silent change to this one.

**The completion walk terminates on a cyclic producer graph, and the flags are
what make it so.** `Complete` and `Leave` are mutually recursive: a segment
that stops closes every channel it reads, and a closed channel lowers its
producer's count of live outputs until the producer stops in its turn. In a
strongly connected component "its turn" eventually means the segment the walk
started at. The two interlocked flags — this segment has stopped, this edge is
closed — are what bound it, and that was verified by instrumenting the walk and
watching it come back rather than by reading the code: on a loop whose exit leg
has already left, one completion produces the sequence *enter, enter, enter,
re-entry* and stops, with the re-entry landing on the very segment the walk
began at. The depth is bounded by the number of segments, and a test whose only
claim is that such a run ends at all is the honest one, because a missing guard
would not fail an assertion — it would exhaust the stack.

**What this checkpoint does not do.** There is still no authoring spelling for
any junction, splitting or joining: the C# graph builder is M4.2, and the tests
here build documents and binding tables directly. There is no liveness rule for
the acyclic split-join hazard checkpoint 2 documented, and the assessment is
recorded below rather than half-implemented. A `Zip` or a `Concat` standing in
a cycle is legal by the boundary rule and can still starve, because that rule
is about the write side — a pump waiting for room — and a row-building junction
in a loop waits for an *element* that only its own output could produce; the
first row can never be built and the loop never starts. That is the same class
of statement as the split-join hazard and is documented rather than refused.
Quiescence detection for a loop that has gone quiet is not here and is named
above as the thing that would replace the hang. And the control plane is proven
to work across a partition and across a cycle rather than proven in general —
a paused loop comes to rest and moves again, a shutdown ends it, a cancellation
ends it, a failure inside it wins — with one case explicitly deferred: a pause
that lands on a partition **holding a routed element** is not tested, because
filling a leg means its consumer is stuck, and the only ways to keep a consumer
stuck are an author's callback, which blocks quiescence by design, and a
probe's rendezvous, which is checkpoint 5's.

### Assessment: a liveness rule for the acyclic split-join hazard

Checkpoint 2 documented a shape whose junction contracts are individually
satisfiable and jointly impossible: a broadcast feeding an order-dependent
join — a concat, or an interleave whose segment size is above one — with no
buffer on any path between them. The cycle rule refuses its own version of the
same thing at validation, so the question this checkpoint owed was whether the
acyclic version can be refused too. It can be *stated*, and it is not being
shipped, for reasons worth recording rather than repeating later.

The rule would be: for a fan-in `J` and two of its inputs `i` and `j`, if both
are fed transitively by one fan-out whose contract is *every live leg must have
room* (broadcast, unzip), then the path to `j` must provide at least the
head-of-line depth `J`'s contract implies while it waits on `i`. For an
interleave that depth is its declared segment size, which is a number. For a
concat it is **the whole of input `i`**, which is not a number at all and cannot
be one — so for a concat the only satisfiable form of the requirement is the
cycle rule's own predicate, a boundary on `j`'s path that answers without room.
That much is crisp.

What is not crisp is everything between. "The path" is not one path: branches
pass through further junctions, each with its own contract, and the slack a
path provides is the sum of the declared capacities along it only when nothing
on it changes the element count — which a `Take`, a `Where`, and a fan-in all
do. A rule that summed capacities anyway would refuse graphs that run today,
including the very one checkpoint 2 shipped as a live test: an interleave of
segment size two under a broadcast with two-element buffers on the legs, which
is exactly determined and alive. A rule that refused every broadcast-to-concat
pair would refuse the ones a dropping buffer makes legal. And a rule that got
either direction wrong would be worse than the hazard, because a refusal is a
graph an author cannot run at all while a deadlock is a graph an author can
observe, read the documentation for, and fix with a buffer.

The assessment is therefore: **documented, not enforced**, and the distinction
from the cycle rule is real rather than an inconsistency. A cycle's illegality
is a property of the graph's shape alone — a loop of waiting boundaries waits
for itself whatever the elements do — while this hazard's depends on the
element counts a graph actually produces. The first is decidable by looking;
the second is not.

## M4.1 checkpoint 5 (control-plane generalization) — as implemented

The engine's last checkpoint, and the one whose job was to check a claim rather
than to add a pump. What follows is the hole the check found, the states that
had to be reachable before any of it could be asserted, and what the whole M4.1
arc does and does not leave behind.

**The counters needed no change and one wait did, which is why the claim was
checked rather than repeated.** The design's claim — quiescence is every
segment parked or idle with no callback in flight, and the counters never
depended on the plan being linear — is true of `LocalPause` itself: it counts
segments, and a junction is a segment. What the claim rests on is a second
statement nobody had written down, that *every wait a segment can take reports
itself*, and that one was false. A channel sink's write into a full channel
blocked the segment's thread without telling the pause gate anything, so a run
holding an element at a `Sink.ToChannel` whose consumer had stopped reading
could never reach quiescence: `PauseAsync` did not fail, it hung, which is why
no assertion had ever caught it. The wait is exactly the mirror of the one a channel
*source* takes on an empty reader, and that one has reported itself since it
was written — the two halves of one adapter were answering a pause differently.
The fix is the bracket the other waits already have, taken only when the write
does not complete at once so that the ordinary element pays nothing, and the
regression lives with the linear suite because the hole was never junction-
specific. It was found by sweeping every blocking call in the runtime against
the question "does this one say so", which is a different exercise from reading
the pause code and agreeing with it.

**Every junction's own held state comes to rest, and a probe is what made the
states reachable at all.** ADR 0005's rule 5 says a junction parks between
elements and that what it holds is held rather than in flight. Asserting that
needs the junction to actually *be* holding something, which needs a consumer
that has stopped consuming — and the only two ways to stop a consumer are an
author's callback, which blocks quiescence by design and would therefore be
asserting a pause the contract says cannot happen, and a probe's rendezvous,
which holds the element on the run's own thread inside a wait this runtime owns.
That is why checkpoint 4 deferred the partition case and why it is here: the
suite now pauses a partition **holding an element it has already routed**, a
broadcast that cannot pull because one leg is full, a balance with no willing
leg, a concat asleep on the input whose turn it is while another input has
something ready, a merge asleep on every input, a zip holding a column, a
combine-latest remembering both inputs' latest, and a loop with its own stream
circulating. Every one of them reaches quiescence, resumes, and finishes with
the elements it was holding delivered once, unchanged, and in order.

**The double pause is the idiom that makes those claims facts.** A pause asked
for while a segment is still on its way to a wait may be answered by an
ordinary park at that segment's safe point, which proves nothing about the
wait. Pausing, resuming, and pausing again — the M2 suite's own idiom for a
source that parks on nothing at all — leaves the run in a state from which
nothing can move, so the second quiescence is the wait's own. It is what turns
"a partition was probably in its room-wait" into "the routing function has run
three times and the fourth element is still in the junction's input channel".

**The bounds are proven through the pause and not beside it.** Each junction's
ADR 0005 number is asserted as how far a held source got, read while the run is
quiescent and again after it ends: a partition holds one element and starves
the other leg for exactly as long (the leg receives nothing at all while the
pause is in effect, and receives everything afterwards); a broadcast holds
*none* while it waits, because it asks for room before it pulls, so the element
it would have taken is still in its input channel; a merge holds none while
waiting and one while placing, which is four elements absorbed and no fifth
pull; a zip holds N−1 columns, and the column read before the pause is the
column of the row emitted after it; a combine-latest holds N, proven by a row
emitted after the resume from an arrival on one input alone. The two overflow
policies checkpoint 3 recorded as inherited-but-untested below a row junction
are tested here: a dropping boundary below a zip drops rather than pacing the
pump and every row is accounted for as delivered or counted, and a failing one
faults the run with the junction's own offer as the origin.

**Shutdown drains and cancellation abandons, asserted on one graph in one
state.** A diamond — a broadcast, a declared buffer on each leg, a transform on
one of them, and a zip that joins them — is held at its first row and then
either shut down or disposed, so the drain and the abandonment differ by
nothing but the request. Shutdown delivers all six elements the run had
admitted, with nothing dropped; cancellation delivers the row already inside
the author's callback and nothing behind it. A graph mixing kinds — a partition
splitting by parity, a transform per leg, a merge joining them, a declared
buffer below that — drains all eight, which is the accounting ADR 0005 asks
for read across a split and a join at once. What "admitted" means is exact here
rather than estimated, because an emit into a source probe completes when the
run has taken the element.

**A failure on one branch reaches its sibling as cancellation, and the sibling
says so.** ADR 0005's first shared rule is usually read across a fan-in; the
new statement is across a fan-out, where the branch that did not fail has to
learn that the run is over. Proving that it learns it as an abandonment rather
than as an ordinary end takes a branch that can tell the two apart, which is an
asynchronous callback holding the run's own token: the callback records which
of the two it observed, and it observes the cancellation.

**Disposal mid-flight is asserted once per junction kind, and the first claim
is the one that would hang.** Ten graphs — one per junction plus a cycle — are
built over endless sources, held at a sink, and disposed. That `DisposeAsync`
*returns* is the claim: it waits for every segment to have left its loop, so a
pump that could not be woken from its own wait would never let the test finish.
The outcome is then the cancellation that was asked for, every enumerator the
run obtained was released — including the per-lap ones an endless source hands
out, which is where a branching run has more of them than a linear one ever
did — and disposing again changes nothing.

**Resume-then-repause storms neither deadlock nor lose an element.** Two of
them: a diamond stormed once per element for forty elements, whose output is an
exact sequence of rows, and a graph in which a partition, a merge, a broadcast
and a zip all stand at once, stormed sixty times, whose output is an exact
multiset because a merge promises the multiset rather than the interleaving.
Each cycle asks for quiescence with elements genuinely in flight on several
branches, so a counter hole anywhere would hang rather than fail — which is
what the deadline in the suite's `Reaches` helper exists to turn into a report.

**A probe attaches to a branch end, and in a graph that is enough.** The design
sentence said "a probe attaches to an edge", and this checkpoint is where that
had to become true or be withdrawn. It is withdrawn, because a probe is an
ordinary stage of the local vocabulary — a bounded ingress queue at the head of
a branch, a rendezvous terminal at its end — and a stage attaches where a stage
can stand. What changed in the DAG world is not the probe but what a branch end
*is*: one leg of a split, one input of a join, one exit of a loop. So a
`TestSource.Probe` feeds a broadcast, a `TestSink.Probe` measures one leg of a
partition while its sibling starves, and the demand meter reads through a
fan-in exactly as it reads along a chain — `PullsObserved <= emitted + 1` on
every input. A probe that attached to the *middle* of an edge would be a
different thing, a tap, and nothing in this milestone's contracts needs one;
building it to make a sentence true would have been machinery with no claim
behind it. The one thing the testing package needed was nothing at all: the
occurrences are lifted out of the authoring values that spell them, which is
the same back door every junction fixture already goes through.

**The two planes have to be talking about the same node, and now they say so.**
Checkpoint 2 recorded that a `merge` node bound to a different junction kind
was undetected, and this is that gap closed. The check is one comparison per
node — the stage the document names against the stage the binding's kind
declares — and its placement is the whole of its design: it is asked **last**,
after every structural refusal has had its say. Every one of those names
something the runtime actually cannot do (this node is fed by two streams and a
mapping cannot join them; this junction is wired at a port its stage does not
declare; this shape cannot stand where the document puts it), and those
sentences are sharper than "the two planes disagree", so they keep speaking
first for every mismatch whose shapes differ enough to be told apart. What
reaches the new check is the residue nothing structural could ever separate:
two fan-in junctions of the same arity, a `select` bound to a `where`, a
`first` bound to a `last`. Adding it changed no existing diagnostic, which is
what the placement bought, and it is unreachable through the authoring API for
the reason every refusal here is — that surface builds the node from the
binding's own kind, so the two agree by construction.

### What the M4.1 arc proved, and what it left

Five checkpoints turned a line into a graph without changing the execution
model underneath it. What they proved together: channels keyed by edge rather
than by chain position, with fusion unchanged inside the branches and a
junction that never fuses; every junction of ADR 0005 with the memory bound its
table states, measured rather than argued; completion that walks upstream per
edge and stops at a junction with a live leg; terminals counted rather than
singular, with one result slot per ending and one outcome for the run; cycles
legal exactly when a relieving boundary breaks them, refused by a walk that
names the surviving loop's path, and ending from inside or by a stop but never
by the closing of their external inputs; and a control plane — pause, resume,
shutdown, cancellation, failure, probes — that holds across all of it, with the
one wait that did not report itself found and fixed.

What it left, deliberately and by name. There is no authoring spelling for any
junction: every proof here is over a document and a binding table built
directly, which is the durable half of a branching graph, and the C# graph
builder is M4.2. There is no liveness rule for the acyclic split-join hazard —
the assessment above says why a rule that refused it would be worse than the
hazard. There is no quiescence detection for a loop that has gone quiet, and
the honest promise stays "write the loop's exit as a stage on the loop, or stop
the run". The operator waves M4 owes — batching, windowing, timing, rate,
flattening, deduplication, sequence edits, observation — are untouched here,
and so is the clock that the timing group brings with it. Substreams,
group-by, split, prefix-and-tail, and dynamic hubs are M4+ and not started.

### What checkpoint 5 does not prove

The pause contract is proven for every junction *this engine has*, in the held
states those junctions can be in, on graphs of a handful of segments. It is not
a proof that no wait anywhere reports nothing: the sweep that found the channel
sink was over this runtime's own blocking calls, and a wait added later without
the bracket would be exactly as invisible as that one was — the invariant is
maintained by discipline, not by a test that could fail. Nothing here is a
statement about an author's own code either: a callback that blocks, an
enumerable that ignores its token, a channel consumer that stops reading all
still delay a pause and a shutdown by design, and the tests that show a pause
waiting for a callback in flight are showing that rule rather than working
around it.

"No leaked threads" is proven in the only form this suite can prove it: a
disposal returns, and it can only return once every segment has left its loop.
Nothing here counts threads or watches for an unobserved task exception raised
after a run has settled — the continuation that observes every abandoned
callback is a structural argument the M2 suite made, not a fact re-measured
here.

The accounting claims are about elements the run *admitted*, which a source
probe makes exact and an enumerable source does not: a shutdown of a graph fed
by an enumerable delivers what it had, and how much that was depends on how far
the source had run. Nothing here claims a number for that shape.

And the whole of it is the local runtime. The Orleans runtime distributes runs
rather than stages, so a branching document executes inside one local engine
there too — but no test in this checkpoint materializes one through a silo, and
"the control plane holds across a distributed DAG" is a sentence nobody has
earned yet.

## M4.3 wave 1 (time) — as implemented

The clock arrives, and with it every operator that reads one. What follows is
where the clock lives, what a wait on it owes the control plane, what each
operator's contract turned out to be once it had to run in a pull engine, and
what this wave does not do.

### Where the clock lives

**The clock is the host's, resolved at materialization, carried by the run.**
`LocalDataflowHost` takes a `TimeProvider` (default `TimeProvider.System`);
the planner puts it on the plan, the run puts it in the context every source,
terminal, and clock-reading stage already receives, and no stage of this
vocabulary ever names `TimeProvider.System` itself. That is ADR 0005's
sentence made mechanical, and the reason it has to be mechanical is that a
single stage reaching for the system clock would make every deterministic
test of a graph containing it a lie.

**The document never carries a clock**, because a clock is runtime and not
definition: two runs of one graph may be measured by two different clocks and
their fingerprints are identical. What *is* in the document is every number an
author wrote — the delay, the two windows, the timeout's gap, the rate, the
burst, the mode, the tick source's two durations — written as counts of
`TimeSpan.Ticks` under contracts of their own, so two graphs differing only in
a duration are two graphs with two fingerprints, and a run executes the
document's number rather than the binding's.

**The run has one zero.** The clock is read once, when the run is built, and
that reading is what every "since the run started" duration measures from: an
initial delay, both windows, a timeout's first gap, a throttle's first budget,
and a tick source's tick zero. One reading rather than one per stage, because
the alternative is a zero that depends on when a thread happened to be
scheduled — a race an author could observe and a test could not pin. The
clock-reading stages are attached to the run before any segment is launched
for the same reason.

### What a clock wait owes the control plane

Checkpoint 5's rule is the one every new wait was written against: **a wait
that does not report itself is a hole in quiescence.** Every clock wait this
wave adds takes the `Idle`/`Busy` bracket, so a run parked in one comes to
rest under `PauseAsync` without the clock moving at all, and every one of them
is taken on a token, so a stop releases it. Three consequences are contracts
rather than incidents.

- **A stop releases a clock wait and keeps the element.** A shutdown ends the
  wait of an initial delay, a throttle, or a tick source at once: a stop is not
  a stream, so the element in the segment's hand is delivered rather than held
  back for a clock that no longer paces anything. A cancellation raises and
  abandons it. The same split every other wait of this runtime makes.
- **A wait that finishes during a pause parks instead of delivering.** The
  stage returns to the gate before it emits, which is the second look the
  source pump has taken since checkpoint 1. A paused run whose budget arrives
  is still a paused run.
- **Time passes while a run is paused.** A pause holds elements at safe
  points; it does not stop the clock, and pretending otherwise would need
  every timing stage to observe the gate's edges and re-derive its deadlines
  from them. So a run held for longer than a timeout's gap fails, and one held
  past a window's end closes that window. Stated, tested, and documented
  rather than discovered.

**Two stages act when no element arrives at all**, which no per-element method
could ever be asked: a timeout has to fail a stream that has gone silent, and
a take-within has to end one whose window closed while nothing came. Each
holds one timer of the run's clock and acts from it through two hooks the run
already had — `Complete`, the walk a downstream completion takes, and `Fail`,
the record a throwing stage makes, both already safe from any thread. No pump
shape was added and no wait discipline changed; what is new is only that a
timer may be the caller. The timers are released when their segment stops, so
none outlives its run.

Three details of those timers are worth stating because each was a defect
before it was a rule. A timer is created **disarmed** and armed afterwards, so
the stage's own field is assigned before anything can fire — a timer armed in
its constructor may fire first, and with a controlled clock a test can make
that happen by advancing while the run is being launched. A fire is a
**question rather than a verdict**: both stages re-read the elapsed time and
re-arm if the moment has not come, which is what lets a watchdog ignore a
stream that kept its promise and what makes the third rule possible. And the
arm is **clamped to what the clock accepts** — the BCL's timers count
milliseconds in an unsigned 32-bit number, so a due time past about
forty-nine days is refused, and a window or a gap of months is an ordinary
thing for an author to write. Before the clamp, `Timeout(TimeSpan.FromDays(400))`
threw an argument exception out of `MaterializeAsync` from inside a timer
nobody had asked about.

### The operators, and what each one turned out to mean

**`Delay(d, holdback)` shifts a stream; it does not pace one.** It is driven
by the machinery an asynchronous stage is driven by, because that is the shape
of the promise: an element admitted starts its own wait at once, results are
emitted in input order, and a burst that fits the declared holdback comes out
with its gaps intact, later by the delay. A stage holding one element at a time
would have turned the same burst into a stream paced at one element per delay,
which is a throttle and not a delay. The holdback is required — the declared
capacity is how many elements are waiting out their delay at once, with one
more in the handoff in front of them as there is in front of every
asynchronous stage — and the declared overflow policy answers the element that
arrives when both are occupied, exactly as a buffer's would.

Being a window rather than a hold has two consequences this engine states
rather than hides: **a pause waits for the delays in flight** and **a shutdown
drains them**, both exactly as they do for an author's callback in flight, and
both bounded by the delay itself. That is the one clock wait a stop does not
cut short, and it is the async window's rule rather than a decision taken here.

**`InitialDelay(d)` delays the stream and not its elements.** The first
element is held until `d` has passed since the run started and everything
after it passes untouched; a stream whose first element arrives later than that
is not delayed at all, because the wait is for a moment rather than for a
duration.

**`SkipWithin(w)` is the wall-clock `Skip`** and the one clock-reading stage
that never waits: an element arriving inside the window is dropped the moment
it arrives, and the clock stops being read once the window has closed, because
the answer can never change again.

**`TakeWithin(w)` ends the stream at its deadline**, and does so from two
sides that say one thing: the element arriving at or after the deadline is not
emitted, and the timer ends the stream when the window closes with nothing
there to end it on. The second is the operator's reason to exist, and the
honest limit is worth naming: **the stream ends at that stage, and a source
above it that is asleep in one of this runtime's own waits learns at its next
element.** That is the engine's existing rule — a completion below does not
release a source's own wait — so a run over a ticking source ends within one
interval of the deadline rather than at it, and a run over `Source.Never`
does not end until it is stopped. The elements are exactly those that arrived
before the deadline either way.

**`Timeout(d)` fails a stream that goes quiet**, counting from the previous
element and, for the first one, from the moment the run started; the run
faults with `StreamTimeoutException`, which is a `TimeoutException` a caller
can catch by its own name. The timer is a watchdog rather than a deadline: it
is armed once and, when it fires, asks how long the stream has actually been
silent, re-arming for the remainder if an element arrived meanwhile. So the
ordinary element pays one timestamp and no timer call, and a timeout that did
not happen cannot be reported.

**`Throttle(options)` is a token bucket counted in exact integers.** The
budget is held in element-ticks — a tick of elapsed time is worth `Elements`
of them and one cost unit costs `Per.Ticks` of them — so the refill is
continuous rather than stepped: three per second admits an element every third
of a second instead of three at each second's edge. The bucket starts full,
holds `MaximumBurst` (defaulting to `Elements`, written into the document
either way), and charges what the cost function answers when there is one.
`Shaping` waits for budget on the segment's own thread, which backpressures
upstream; `Enforcing` fails the run with `RateLimitExceededException`. Nothing
is ever dropped by either. Two failures belong to both modes: an element
costing more than the whole burst, which no amount of waiting could admit, and
a negative cost, which would give a stream budget back.

**`Valve(controlName, initialMode)` is the one operator of this wave that
reads no clock**, and it is here because it needs the other half of the same
attachment: somewhere to wait that reports itself. It is also the first control
this vocabulary declares in the middle of a chain rather than at one of its
ends, and the M2 control seam needed no line for that — the graph builder
already collected a control from any occurrence, the planner already sorted
controls by the port they are declared on rather than by where the node stands,
and a run already handed every control out as soon as it existed. It is the
simplest control there is, because a valve has no element type: the runtime
object *is* the `IValve` an author receives, so there is no facade to build.

A closed valve holds the element the stage has in its hand and backpressures
everything above it, with no capacity of its own — what accumulates is exactly
the declared buffer above it plus the element the valve holds plus the one in
the source's hand — and nothing is dropped. Closing takes effect at the next
element and never retroactively. Two valves in one chain are two controls and
both have to be open; two runs of one graph have two valves. The state a valve
*starts* in is in the document, because a graph whose valve starts closed
produces nothing until something opens it; what an author does to it afterwards
is a run's own business.

**`Source.Tick(initialDelay, interval)` emits tick numbers, and the numbers
are the contract.** A tick that comes due while the run is busy is *skipped*
rather than queued — a queue of moments that have already passed grows without
bound whenever the consumer is slower than the interval — and tick `n` is due
at `initialDelay + n * interval` after the run started whether or not it was
emitted, so a consumer that missed three receives a number three higher and
can see that it fell behind. Akka's tick source emits a fixed element the
author supplies, which is honest for a source that never skips; here the
skipping is the contract, and a counter that jumped silently would hide the
one thing worth reading. A stream of a constant is one `Select` away. The
source is endless, non-durable, and belongs to its run the way an enumerator
does: two runs of one graph tick independently, and it is bounded by whatever
is written below it or by a stop.

### The test clock

`Orleans.Dataflow.Testing.TestClock` is a `TimeProvider` a test moves by hand:
a monotonic reading, a wall-clock reading, and timers that fire when the test
says so. Everything in the BCL that takes a `TimeProvider` — `Task.Delay`
above all, which every wait here is built on — is built on exactly those, so
implementing the four members is smaller than a package dependency would be
and leaves nothing to discover about what it does. `Advance` moves to each due
moment in turn rather than jumping to the end, so a callback reads the clock at
its own due moment and a timer it arms inside the window fires within the same
advance; `WaitForTimersAsync` is the synchronization a virtual clock needs and
a real one does not, because advancing past a wait the run has not armed yet
would arm it after the moment it was waiting for.

What it is not is a scheduler: the segments of a run are real threads doing
real work, and only their *waiting* is virtual, so a test still synchronizes
with the run through a probe, a slot, or completion.

### A correction to ADR 0005, measured rather than argued

The ADR lists, beside a dropping buffer, "an explicit delay" as a boundary
that makes a cycle legal. For this engine that is not true, and the rule is
not implemented: a delay waits for room below it exactly as a backpressuring
buffer does — its window fills, and then the pump above it waits for a slot
only the pump below could free — so it postpones the deadlock rather than
breaking it. A cycle whose only boundary is a delay is refused like any other
waiting loop, with the delay named in the path. The predicate stays what
checkpoint 4 implemented: a declared buffer whose overflow policy is not
backpressure, and nothing else.

### What this wave does not do

**Nothing here is proven across a junction.** Every graph in this wave's suite
is a chain. A delay on one leg of a broadcast, a timeout on one input of a
zip, and a throttle inside a cycle are all expressible and none is asserted;
the operators are ordinary stages of the vocabulary and compose by
construction, but "compose by construction" is an argument rather than a
measurement.

**One race is closed by discipline rather than by a test.** A timing stage is
attached before any segment starts and detached when its own segment stops, so
a timer of a run that has ended cannot fire; but a run that ends in the same
instant its window closes is a race between the two, and the outcome is
whichever got there first. That is inherent to a deadline — a stream that ends
exactly at its timeout has no true answer — and it is named rather than
papered over.

**The bounds are proven as how far a held source got**, which is the accounting
every bounded-memory test in this suite makes, and not as a measurement of a
stage's own memory. A delay's holdback is asserted as the number of elements a
recording source hands out before the run stops asking — the window plus the
handoff plus the element in the source's hand — read while nothing can move
because the clock is not moving.

**Nothing here says anything about a distributed run.** The clock is the local
host's; a registered stage receives the run's tokens and whatever its own
provider gave it, and the Orleans path materializes with `TimeProvider.System`
because no stage it can execute reads a clock at all. A silo-wide controlled
clock is not a thing this milestone has.
