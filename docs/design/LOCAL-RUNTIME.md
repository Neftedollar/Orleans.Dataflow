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

## M4.3 wave 2 (batching, flattening, deduplication, sequence edits) — as implemented

Wave 1 gave the runtime a clock. This wave gives it two things it had never
had: a stage that **holds elements back** and hands them over when its stream
ends, and a stage that answers one element with **several**. Everything below
follows from those two, plus one consequence of the first that needed the
clock — a group closed by a window rather than by a count.

### Three seams, and why each of them is the smallest one

Until this wave every stage of this vocabulary was a function from one element
to at most one element, applied by one loop that walked a segment's fused
stages in order. Three additions widen that, and each is a widening of the
existing walk rather than a new pump.

**A residue at the end of a stream.** `LocalElementStage.Flush` asks a stage
what it is still holding, and the run asks it once per stage, in flow order, on
the segment's own thread, after the loop that fed it has ended. Each residue is
pushed through the stages *below* the one that gave it — the same walk an
element takes, entered part way down — so a group is an ordinary element to
everything downstream of the batch that built it, including to a `Take` that
may end the stream on it. Nothing else in the vocabulary answers this question,
and nothing else pays for it.

**A sequence instead of an element.** `LocalStageOutcome.EmitMany` hands the
run an enumerator rather than an element, and the run owns it from that moment:
it advances it, pushes each element it yields through the stages below, and
releases it on every path including the ones where a stage below ends the
stream part way through. The run's token and the pause gate are examined
between two inner elements exactly as the source pump examines them between two
pulls — which is what makes an author's endless inner sequence a stream this
runtime paces rather than a loop a run disappears into. The recursion is one
frame per flattening stage in a segment and never one per element.

**A wake with no element behind it.** A batch closed by a clock has to emit
while nothing is arriving, and emitting is the one thing a timer of this
runtime must never do itself: two threads walking one segment's stages is not a
race worth having. So the timer signals — through the very `LocalWakeup` latch
an asynchronous segment already sleeps on beside its input — and the segment
that owns the stage wakes, asks `Due`, and emits. One thread still builds,
closes, and hands over every group, so the stage needs no lock.

The third seam has a price and it is stated rather than hidden: **a
grouped-within is a boundary.** It is opened into a segment of its own with one
bounded handoff in front of it, exactly as an asynchronous stage is, because
only a segment waiting on its own input channel can be woken. Fused into a
source's loop it would sit behind whatever the source was doing, and a window
that closed would wait for the next element to notice it — which is the one
case the operator exists for. `Grouped` and `Sliding` fuse like any other
stage, and the difference is measured rather than described: a run of the first
pulls its source exactly as far as the open group, and a run of the second runs
ahead by its handoff.

**A boundary is not the same thing as a relieving boundary.** A cycle is legal
only when it passes a declared buffer whose overflow policy is not
backpressure, and a grouped-within's handoff waits like every other boundary
this engine has, so a loop relieved only by one is refused exactly as a loop
relieved only by a delay is. That predicate is unchanged and this wave adds
nothing to it.

### What the residue walk is, exactly

The rule is one sentence: **when a segment's loop ends without being cancelled,
every fused stage is asked for its residue in flow order, and each answer
travels through the stages below the one that gave it.** Asking every stage
rather than only the ones below whatever ended the stream is what makes the
answer independent of fusion — and three existing mechanisms are what make it
correct rather than merely convenient.

- **A spent bound refuses.** `Grouped(3).Take(1)` emits one group: the take's
  own arithmetic refuses the residue offered to it afterwards, exactly as it
  refuses any element past its bound. `Take(5).Grouped(3)` emits `[1,2,3]` and
  `[4,5]`, because the take ending the stream *is* the batch's end of stream.
- **A closed boundary refuses.** A segment stopped from below has had its
  output channel completed before it was marked stopped, so a residue offered
  into it is refused and abandoned — the same thing this engine already does
  everywhere for an element arriving at a channel a downstream completion
  closed, and not counted as a drop for the same reason.
- **The walk stops at the first residue that ends the stream.** Both the
  end-of-stream walk and the window walk return as soon as pushing a residue
  answers "the stream is over", so no stage is asked after the stream it feeds
  has ended.

`TakeWhile` was made to latch its rejection as part of this wave, and it is
worth being honest about why: **nothing reaches it twice today.** The third
rule above is what guarantees that, and it is a property of two loops in
`LocalRun` rather than of the stage. Every other stage that ends a stream
already refuses on its own — a spent take by arithmetic, a closed window by
elapsed time — so the latch puts the invariant where a reader looks for it
instead of leaving one stage relying on its caller. A test asserts the
reachable half of the claim; the latch itself is defensive and is recorded as
defensive.

A cancellation asks for no residue at all: what a batch was holding is
abandoned with everything else in flight. A shutdown does ask, because a
shutdown ends the stream as running out would and the elements in the group
were admitted.

### The operators, and what each one turned out to mean

**`Grouped(n)` is the simple one and its only interesting moment is the end.**
A group is emitted the moment it fills, so the stage holds at most `n` elements
and that is the whole of its bound. The last group is the only one that may be
partial, an empty group is never emitted, and a stream whose length is a
multiple of `n` gives exactly the groups it filled.

**`Sliding(n, step)` is three operators wearing one name**, and the relation
between the two numbers is which one an author gets: a step below the size
overlaps windows, a step equal to it partitions the stream, and a step above it
samples one — the elements between two windows are counted past rather than
buffered, so a sampling window costs no more memory than a partitioning one.
The end-of-stream rule is the part worth stating: **the buffer leaves as a final
window only if it holds an element no window has carried.** That single rule
gives both familiar behaviours without a special case for either — a stream
shorter than the window emits everything it had, and a stream that ended in the
middle of an overlap emits nothing new, because everything it still holds has
already been seen.

**`GroupedWithin(n, window)` closes a group on size or on time, whichever comes
first, and the window belongs to the group.** It starts when the group's first
element arrives and it is gone the moment the group is emitted, which makes
"an empty window emits nothing" a consequence rather than a rule: with no group
open there is no window running, so a stream that goes quiet for an hour costs
one disarmed timer and the group after the quiet is timed from its own first
element. The timer follows wave 1's discipline exactly — created disarmed,
armed when a group opens, clamped to what the clock accepts, and a fire is a
question rather than a verdict, so a wake that finds the window still open
re-arms for the remainder.

**The weighted form closes the group before the element that would break its
bound**, so the bound is never exceeded rather than exceeded once per group.
That element starts the next group and the next window is timed from *its*
arrival, which is the only reading under which the two bounds do not
contradict each other. The cost function runs once per element, on the
segment's own thread, before the element joins anything, and two answers fail
the run for the reasons a throttle's do: a negative weight, which would let an
element lighten a group, and a weight above the whole bound, which no group
could ever carry — waiting for one that could would never end.

**`SelectMany` is concat-map and is bounded by construction.** One inner
sequence is read to its end before the next element is asked for, so the order
of the result is a function of the input alone; a function answering an empty
sequence drops its element, which makes filtering a special case of flattening
rather than a second operator; and a function answering `null` fails the run
rather than being read as "nothing", because reading one meaning into the other
hides a mistake that costs elements. Nothing here ever holds a whole inner
sequence, which is the property that makes the operator safe to write over an
author's generator.

**`DeduplicateConsecutive` is the deduplicator that needs no bound**, because
its bound is one element and is a fact about the shape rather than a number an
author chose. It collapses runs and never compares across them.

**`Distinct` gained a policy, and the policy changes what the operator means.**
`Fail` is the default and is what the operator promised when it had no choice:
everything emitted was the first of its key, and a bound sized on a wrong
assumption reports that instead of quietly becoming something weaker.
`EvictOldest` is the deliberate weakening — an element whose key was evicted is
emitted a second time if it arrives again, so the stream is distinct over a
window of the last `MaxTrackedKeys` keys and not over its history. Age is when
a key was *first* remembered rather than when it was last seen, so a repeat
does not refresh a key; the set and a queue of the same keys are kept side by
side, hold exactly the same keys at every moment, and an eviction is therefore
the head of the queue and needs no search.

**The sequence edits add no stage at all.** `Append` is `Concat` under the
name the vocabulary uses and builds a byte-identical document; `Prepend` is the
same junction with its inputs swapped, which is what "before" means when
argument order is identity-bearing; and `DivertTo` is a two-legged partition
with the main line on its first leg — the same shape `AlsoTo` gives a
broadcast, and the reason the receiver stays an expression rather than becoming
one branch of a closed graph. Each therefore inherits its junction's contract
whole, including the parts that cost something: a concat's later input is
running and parked in its own bounded channel while the earlier one plays out,
and a partition holds one element and waits for the leg that element belongs
on, so a slow diverted branch holds the main line up for exactly that long.

### The engine gap: bounded-parallel flattening

**`MergeMap(maxConcurrency)` is not implemented in wave 2, and it is not one
call away.** The matrix row says "concat-map, merge-map with bounded
parallelism" and half of it ships here; this is what the other half would cost,
measured against the code rather than guessed. **It ships in wave 3, and the
section below records how much of this estimate held.**

The asynchronous pump is a window of `Task<object?>`: it admits up to the
declared number of callbacks, frees a slot when a *result is emitted*, and
sleeps on "an element arrived" or "a callback finished". Every one of those
three is wrong for a flattening stage. A merge-map's window holds
*enumerations* rather than tasks; a slot is freed when an enumeration **ends**
rather than when it produces; and the pump has to sleep on "any of N
enumerations has an element", which is a different wait — `Task.WaitAny` over
one pending `MoveNextAsync` per live inner sequence, re-armed per inner
element. Emission order is then arrival order across N live enumerations, which
is a statement no existing pump makes.

That is a new pump shape rather than a variation of one, roughly the size of
the asynchronous pump itself, and it brings its own control-plane surface:
where a pause parks when three inner sequences are mid-flight, what a shutdown
drains, and what disposal must return from. Shipping it beside four batching
operators and a flattening one would have meant shipping it without that
surface proven, so it is named here and left for a wave of its own.

**What an author can write today, and what it costs.** `SelectAsyncUnordered(n,
x => collect(x))` followed by `SelectMany(batch => batch)` runs `n` inner
computations at once and flattens their results — but it materializes each
inner result completely, so it is bounded only by what the author knows about
the inner sizes. That is an honest workaround for small inners and an honest
non-answer for a stream of sequences, which is why it is written here rather
than offered as an overload.

### What this wave does not do

**No graph-valued flatten.** `SelectMany` flattens a `Func<T,IEnumerable<TNext>>`
— a local sub-enumeration on the segment's own thread. A source of sources, in
which each inner stream is a graph with its own stages, boundaries, and
junctions, is a different feature: it needs sub-graphs materialized per element
and torn down per element, which is nothing this engine has a shape for. The
matrix words "concat-map, merge-map" are the operator family and not a promise
of graph-valued flattening.

**No asynchronous inner sequence.** `Func<T,IAsyncEnumerable<TNext>>` is not
offered in this wave. The mechanism is straightforward — the segment's thread
already blocks on this runtime's own waits, so an inner `MoveNextAsync` would
take the same `Idle`/`Busy` bracket every other wait takes — but
"straightforward" is not "tested", and the pause, shutdown, and cancellation
behaviour of a wait inside an inner enumeration is exactly the surface that has
to be proven rather than argued. **Wave 3 offers it, as the shape a merge-map
takes**, and the bracket sentence above turned out to be the one thing this
paragraph got wrong: see below.

**Termination watch and monitor are deferred, with a reason each.**
`WatchTermination` wants to report *how* a stream ended as a value, and ADR
0002's slot model is what makes that awkward rather than obvious: a result slot
resolves at the end of a run and **carries the run's outcome**, so a slot typed
"how it ended" would fault when the run failed instead of resolving to
"failed". The honest shape is therefore a *control* — resolved when the run
starts, carrying a task the author awaits — and the contract that control would
have to state is the one that needs measuring: a failure anywhere cancels the
run's token, so a stage above the failure sees cancellation rather than that
failure, and the run's own `Completion` is where the failure is. That is a
statement worth proving before it is published. `Monitor` is deferred to where
it already belongs: the matrix's "Stage/run monitor snapshots" row targets M5,
`RunHandle.DroppedElements` is already internal and already says a monitor is
what an author will read it through, and inventing a second, weaker snapshot
shape here would fix that design by accident.

**A completion callback needs no operator.** `RunHandle.Completion` is the
run's own task and reports exactly how the run ended — success, the author's
own exception unwrapped, or cancellation — with the run's resources released
and its slots settled before it transitions. A `Sink.OnComplete` beside it
would be a second spelling of one fact.

**The bounds are proven as how far a held source got**, which is the accounting
every bounded-memory test in this suite makes: a count-closed batch's fusion is
asserted as a source pulled exactly as far as its open group, and a timed
batch's boundary as a source that ran ahead of it. Neither is a measurement of
a stage's own memory.

**Nothing here is proven under a fault injected mid-residue.** A stage that
throws while the run is draining a segment's residues faults the run — the
walk runs inside the same `try` an ordinary element does — but a partial
residue walk, in which one stage's group was delivered and the next stage's
was not, is reasoned about rather than measured.

## M4.3 wave 3 (bounded-parallel flattening and the asynchronous folds) — as implemented

Wave 2 named three shapes it could not reach and said why for each: a merge-map
wants a pump nothing here has, an asynchronous scan wants a fold that awaits,
and a result-bearing asynchronous terminal wants a combination of two existing
pieces. This wave builds all three, and the interesting part is that the three
turned out to be two different sizes: **one of them really is a new pump, and
the other two are not stages of a pump at all.**

### The merge-map pump, and how much of wave 2's estimate held

The estimate held almost exactly, which is worth recording because it was made
against the code rather than guessed. `LocalRun.Merge` is the eighth loop shape
and it is what wave 2 described: a window of **enumerations** rather than tasks,
a slot freed when an enumeration **ends** rather than when it produces, and a
sleep over one outstanding `MoveNextAsync` per open inner sequence — a
`Task.WaitAny` over those steps plus, while there is room to admit one, the
arrival of an element on the input. It is a boundary for the same reason an
asynchronous stage is and a stronger version of it: no pass of somebody else's
loop could ever take that wait.

Four things the estimate did not say, and each of them is a decision rather than
a detail — the last of them because getting it wrong was the one defect this
pump actually had.

**The room-wait is not in the wait-set, and that is deliberate.** A merge-map
waits on two different kinds of thing, and only one of them belongs in the
`WaitAny`. The steps of its open sequences are events it has to *choose*
between; downstream room is a wait it simply *takes*, at the moment it has an
element with nowhere to put it, in the very `Offer` every other segment uses —
with the `Idle`/`Busy` bracket that offer has had since checkpoint 2. So an
inner element with no room below parks the pump rather than a thread per inner,
which is the sentence the design asked for, and it costs no machinery at all.
The consequence is stated rather than hidden: while the pump is parked for room,
the other open sequences go on running their own steps, and each of them holds
at most the one element its step produced.

**An outstanding step is counted as a callback in flight, not as an idle
segment.** This is the one place wave 2's forward guess was wrong. It said an
inner `MoveNextAsync` would take "the same `Idle`/`Busy` bracket every other
wait takes"; it must not. `LocalRunContext` states the rule the guess
contradicts — *a wait this runtime owns says so, and a wait inside an author's
delegate says nothing* — and an inner sequence's step is an author's delegate
running. Reporting it idle would let a pause reach quiescence while an author's
iterator was mid-element, which is exactly what quiescence says is not
happening. So the pump does what the asynchronous stage does with a callback:
`LocalPause.Admitted` when a step is armed and `LocalPause.Completed` when it
finishes, with the segment itself reporting idle for the `WaitAny` it sleeps in.
The composite is the honest one — a merge-map is quiescent when every open
sequence's step has answered and the pump is asleep, holding what those steps
produced.

**A failing inner sequence has to reach a pump that is not looking.** The
continuation on each armed step is what makes that true, and the case that
demands it is a real one rather than a hypothetical: the pump can be parked in a
full boundary's offer, or at a sink probe's rendezvous, when one of its other
sequences faults. A failure observed only when the pump next examined its window
would never be observed at all — the pump is waiting for room that is not
coming, and the run would hang rather than fail. So each step is observed the
moment it completes, from whatever thread completed it, by the same
`LocalRun.Observe` an asynchronous callback goes through; recording the failure
cancels the run's token, and cancelling the token is what releases the pump from
the wait it is in. The suite asserts this directly, with a merge-map held at a
probe sink and a sibling sequence failed underneath it.

**A pass cut short by a pause must not fall through to the wait, and reading the
gate twice is not how to know.** This is the one defect the implementation
actually had, and it is worth recording because it is a shape rather than a
typo. Taking an element from a completed step and delivering it are two steps of
one pass, so a pass stopped between them leaves an enumeration *holding* an
element with no step outstanding — and the wait at the bottom of the loop has
nothing to wait on for that enumeration. Guarding with a second read of the
pause gate looks sufficient and is not: a resume landing between the two reads
lets the pass proceed into a wait over an enumeration it had asked nothing of.
The loop therefore carries whether *this pass* got through its enumerations, and
an unfinished one goes back to the top — where it either parks or delivers what
it was holding. The regression is a resume-then-repause storm, once per element
with three sequences in flight, which is the same idiom checkpoint 5 used on the
junctions and for the same reason: a hole in this accounting hangs rather than
fails.

### The two order sentences, and why the pump makes both true

**Emission is unordered across inner sequences, and the order of each inner
sequence is preserved.** Both halves are the loop rather than a rule applied to
it. The elements go out as the pump finds them ready, which across several
sequences is arrival order and nothing else; and an enumeration is never asked
for its next element until the one before it has been delivered, which is why
one inner sequence's own order survives being interleaved with every other's.
The second half is also the whole of the operator's memory bound: an open
sequence holds at most one element, so a merge-map of `n` holds at most `n`
elements plus the one it is placing, and `MaxConcurrency = 1` is a concat-map
that costs a segment.

The bound is proven the way every bound in this suite is proven — as how far a
held source got. Four elements are absorbed by a merge-map of two with every
inner sequence held at its first step: two open sequences, one element in the
handoff channel, and one in the source segment's hand. With a declared buffer of
four in front, seven are absorbed and not four, because a buffer written in
front of a merge-map is its own input channel rather than a second one behind an
implicit handoff — the rule every boundary of this vocabulary follows.

### What a stop does, and the one place this pump differs from the asynchronous one

**A stream ended below releases the open sequences rather than draining them**,
and that is the deliberate difference. An asynchronous stage drains its
callbacks because they are an author's code already running and cancelling them
would report a cancellation nobody asked for; an enumeration is not running of
its own accord, so there is nothing to be polite to. It is released — which is
what disposing an enumeration means — and an endless inner sequence therefore
does not outlive the stream it was feeding. A `Take` below a merge-map ends the
run successfully with every open sequence's `DisposeAsync` awaited to its
return.

**A shutdown is the opposite case and plays the open sequences out to their
natural end.** It reaches the pump as the end of its input, exactly as it
reaches any other downstream segment, so nothing new arrives and everything
already admitted finishes. The honest footnote is that "admits no new element"
is a statement about upstream rather than about the pump: what the boundary in
front of it already held is delivered and *is* admitted, because draining a
boundary's contents is what a shutdown does everywhere in this engine. A
cancellation abandons: the run's token releases every outstanding step, and each
sequence is released.

**Release is the caller's, on every path.** The open sequences live in
`LocalRun.Execute`'s own frame beside the head enumerator, and one call releases
both — which is what makes "an inner enumeration is disposed on every terminal
path" true of the paths the pump never returns from: a failing selector, a
cancelled wait, a stream ended below. Releasing one means awaiting its
outstanding step first (an enumeration whose `MoveNextAsync` is in flight may
not be disposed at all) and then awaiting its `DisposeAsync` rather than
starting it. A release that throws is reported under the rule the head
enumerator already follows: only when nothing else went wrong.

### The asynchronous folds are not stages of a pump, and the sketch that said they were is corrected

Wave 2's audit sketched an asynchronous scan as "the asynchronous stage with a
concurrency of one, ordered, with the state threaded by a wrapper the planner
builds". That would work, and it is the wrong shape. **One fold of such a stage
can never run beside another, because the state the next element folds into is
this fold's answer** — so a window, an admission rule, a slot freed by emission,
and the bounded channel in front of them are all machinery with nothing to do.
Worse, the sequentiality would be a *consequence* of the asynchronous pump's
admission rule rather than a property of the shape: correctness resting on a
feature that exists for a different reason.

So `ScanAsync` is a fused stage that waits, and `Sink.AggregateAsync` is a
terminal that waits, and both go through one method — `LocalRunContext.Await`.
It blocks the segment's own dedicated thread, which is what that thread is for,
and then parks against the pause gate, which is the second look every other wait
here takes. Two consequences are worth stating.

**Nothing is reported to the pause gate while a fold runs, and that is the rule
rather than an omission.** The segment's own thread is inside the author's
callback, so it is neither parked nor idle and the counters already report the
run as moving; a pause therefore waits for a fold in flight exactly as it waits
for a slow synchronous stage, which is the contract the asynchronous stages
already state. The park *after* the fold is what keeps a paused run from moving:
a fold that finished during a pause leaves its new state in the stage's hand at a
safe point instead of emitting it.

**An asynchronous fold costs no boundary, and that is measurable rather than
claimed.** With a fold held at its first element, the source has produced
exactly the element being folded — a fold built on the asynchronous stage's
machinery would have a handoff channel in front of it and the source would have
run one further. `Scan` fuses and so does `ScanAsync`; the difference between
them is the awaiting and nothing else.

The rest of each operator is its synchronous twin's contract, kept: one state
out per element in, the seed not emitted, an empty stream emitting nothing, the
state allocated per run, the folder receiving the run's own token, a failure
mid-fold faulting the run with the author's own exception, and — for the
terminal — a slot that resolves through the ordinary machinery when the run
ends, faults with the run's failure, cancels with its cancellation, and resolves
what was folded so far under a shutdown. `ForEachAsync` declares a bound because
its callbacks are independent and declares no result because it accumulates
nothing; `AggregateAsync` declares neither a bound nor independence, and
declares a result. That is the whole of the difference between them.

### What this wave does not do

**A merge-map with a genuinely quiet inner sequence does not reach quiescence.**
An outstanding step is an author's code in flight, so a pause waits for it — the
same rule under which a callback that never completes holds a pause forever.
That is a real consequence for a merge-map of long-lived streams and it is the
rule rather than a bug, but it is the reason a merge-map is not the operator to
reach for when the inner sequences are subscriptions that go quiet for hours.

**A fold that ignores its token delays a stop until it returns.** This is the
one behaviour where the asynchronous folds differ from `SelectAsync`, which
abandons its in-flight callbacks on cancellation and lets a continuation observe
them. A fold is awaited to its outcome instead, for the reason the asynchronous
cursor is: abandoning it would leave the state neither the old one nor the new
one, and a failure it was about to report would be lost. The slow-callback rule
therefore binds harder here, and it is stated rather than discovered.

**A pump whose window is full learns about a stream ended below only at its next
emission.** When it is not admitting, the input channel's closing is not in its
wait-set, so a merge-map whose sequences have all gone quiet does not notice a
`TakeWithin` above it firing. This is the asynchronous pump's shape rather than
this one's — that pump waits only on its callback latch when its window is full
— and neither is made worse by the other. Cancellation is in every wait and is
unaffected.

**Nothing here is a statement about a graph-valued flatten.** `MergeMap`
flattens `n` local sub-enumerations on one segment's thread. A source of
sources, in which each inner stream is a graph with its own stages, boundaries
and junctions, is still a different feature this engine has no shape for, and
the matrix words "concat-map, merge-map" are still the operator family rather
than a promise of one.

**The bounds are how far a held source got, and not a measurement of memory.**
Four elements absorbed by a merge-map of two is an accounting of admitted
elements, exactly as every bounded-memory claim in this suite is; nothing here
weighs a heap.

**And the whole of it is the local runtime.** No merge-map and no asynchronous
fold has been materialized through a silo, and every graph carrying one is
`nondeployable` because it is a lambda-bound local stage.

## M4.4 (bounded group-by) — as implemented

The first substream operator, and the one the capability matrix marks P1. The
row's demand is a single clause — *maximum active keys, eviction, cancellation,
and idle cleanup are bounded* — so bounds are not a quality of this operator but
the whole of what it is, and every decision below is downstream of that.

The shape is: **the substream flow is declared once and instantiated per key.**
`source.GroupBy(options, keySelector, groupFlow)` takes an ordinary
`Flow<T,TOut>` and runs one instance of it per key, merging what the instances
emit into one stream. There is no `Source<Source<T>>`, no sub-graph
materialized per element, and no new pump: a keyed stage is a **fused element
stage** like a batch or a scan, and the per-key chains are instances of the very
`LocalElementStage` shapes a top-level chain fuses.

### The seams stretched, and one of them widened by a word

Wave 2 left three seams — a residue at the end of a stream, a sequence instead
of an element, and a wake with no element behind it. This operator needed the
first two and needed the first one to say slightly more.

**Emission is merged through `EmitMany`, and for the ordinary element it is not
even that.** Pushing one element through one key's chain produces zero, one, or
several emissions; the stage collects them into a list of its own and answers
`Drop`, `Emit`, or `EmitMany` according to how many there were. The three-way
answer is not an optimization dressed as a contract: the ordinary element of an
ordinary group flow produces exactly one emission, and a sequence of one would
cost an allocation and a walk of the run's flattening path to say what `Emit`
says.

**`Flush` answers an outcome rather than a flag.** A batch holds one residue and
a keyed stage holds one *per active key*, so the end of a stream is where
several of them have to leave at once. Rather than a second flush seam, `Flush`
now returns the element vocabulary's own `LocalStageOutcome` — `Drop`, `Emit`,
or `EmitMany` — and the run walks a many-residue answer through the very
`Expand` it walks a flattening stage's sequence through, with the token and the
pause gate examined between two of them exactly as before. `Due` stays a flag,
because the one shape that answers it emits exactly one group.

**Nothing re-enters the run, and that is why this is a stage rather than a
pump.** The worry worth naming — a per-key chain wanting to emit *during*
another key's flush, or a residue walk re-entering itself — does not arise,
because a substream never talks to the run at all. Its emissions go into the
stage's own list and the run reads that list after the method has returned. The
list is reused across elements, which is safe for one reason and it is worth
writing down: the run's walk only ever pushes elements through the stages
*below* this one, so nothing downstream can call back into it while its sequence
is being read. Two keyed stages fused in one segment are two lists, and the
suite runs that graph on purpose.

### One key's chain is the run's own walk, read one level down

Pushing an element through a key's stages is `LocalRun.Advance` with one
difference, and the difference is the whole of what a substream is: **a stage
that ends the stream ends that key's stream and not the run's.** Everything else
is the same shape — an emitting stage passes its element on, a dropping one
stops the walk, a stage that emits and completes does both — and when a key's
chain does end, that key is drained by `LocalRun.Drain` read one level down:
every stage asked in flow order, each residue pushed through the stages below
the one that gave it, and the walk stopping at the first residue that ends the
stream. A spent `Take` inside a group flow refuses the residue offered to it for
the same arithmetic reason a top-level one does.

**A key whose substream ended keeps its place, and every later element of that
key is dropped.** Remembering that a key has ended is what keeps it ended, and
what that memory costs is one of the declared places — a run whose keys all end
early still fails at the key past the bound, which the suite asserts, because
"active" counts the keys this stage is answering for rather than the ones still
producing.

### Bounds are the contract

`GroupByOptions.MaxActiveKeys` is required and has no unbounded spelling, for
the reason `DistinctOptions.MaxTrackedKeys` has none: one substream per key a
stream ever carried is unbounded memory, and a default would be a leak nobody
wrote down. What the key past the bound costs is
`ActiveKeyOverflowPolicy`, and the two values are two different operators.

- **`Fail`** faults the run with `TrackedKeyOverflowException` naming the bound
  *and the key*. The key is in the sentence and a deduplicating stage's is not,
  because a stage that holds a substream per key fails on the shape of the data
  and the key that broke the bound is usually the whole diagnosis — a null, an
  identifier meant to be coarse, a timestamp used as a key.
- **`EvictIdle`** flushes the key that has waited longest for an element and
  then forgets it completely. **Eviction is a flush-and-forget**: the evicted
  substream's residues walk downstream at that moment — the wave-2 residue
  discipline applied per key — and an element of that key arriving later starts
  a *fresh* substream from its own seed. **One key can therefore appear more
  than once downstream, with a scan restarting from zero and a batch from an
  empty group. That is what bounded means here**, and it is asserted rather than
  footnoted.

**Idleness is when a key last had an element**, which is the only reading under
which this policy differs from a deduplicating stage's `EvictOldest`. An element
of a key marks it active whether or not its substream still accepts elements, so
an ended key whose elements keep arriving is not idle — and an ended key that
*does* go idle is evicted without being flushed a second time, after which its
next element opens a substream again.

A third enumeration rather than a reading of the second, because the two
evictions have two prices: forgetting a set member costs one element emitted
twice, and forgetting a substream costs whatever that substream was holding.

### The end of the stream, and what a stop does

**Every key still open is flushed, in the order its substream opened.** Arrival
order rather than idleness, because it is the order that does not depend on the
policy: a run under `Fail` has no idleness order at all, and a reader comparing
two runs of one graph should not have to know which policy was declared to know
what order the tail comes out in. Under eviction "the order its substream
opened" and "the order its key first arrived" part company for a key that was
evicted and came back, which is the honest wording of the same rule.

Shutdown, cancellation, and pause need nothing new and get nothing new. A
shutdown ends the stream as running out does, so every key's residues are handed
over; a cancellation abandons what every key was holding, exactly as it abandons
a batch's open group; and a pause parks between two elements with every key's
state intact, which the suite asserts with the double-pause idiom and a scan per
key whose sums carry on across the hold rather than restarting. The one new
state a pause can land in is **the middle of the end-of-stream flush** — several
residues on their way out, one delivered and the rest still in the stage's hand
— and it is asserted there too, because that is where the widened seam meets the
control plane. It comes to rest, resumes, and delivers the rest unchanged; a
spent bound below it cuts the same walk short instead, which is the run's
existing rule read over an answer carrying several residues.

**A keyed stage is not a boundary.** It fuses, so a run of one pulls its source
exactly as far as the element in its hand — measured as how far a held source
got, which is the accounting every bounded-memory claim in this suite makes.

### The group flow holds element stages only, and that is v1's honesty

A group flow is fused per key, so it holds the shapes that are a function of an
element and their own state: `Select`, `Where`, `Scan`, `Take`, `Skip`,
`TakeWhile`, `TakeThrough`, `SkipWhile`, `Distinct`, `DeduplicateConsecutive`,
`Grouped`, and `Sliding`. An asynchronous stage, a merge-map, and a buffer each
want a segment and a channel of their own; a junction wants several; a
clock-reading stage wants a run to attach to and, for two of them, a timer that
can complete or fail the run. One instance per key of any of those is not
something a fused stage can hold, and the refusal names every offending stage
and its position rather than the first one — a group flow is written as one
expression, and an author fixing them one per compile is an author running the
same call four times.

Two more are refused for this operator's own reasons rather than for their
machinery's, and both are stated as v1's honesty:

- **A flattening stage.** What a keyed stage hands the run is one sequence per
  element, read after the stage has returned, so a `SelectMany` inside a group
  flow would have its inner sequence **materialized** rather than streamed —
  bounded by what the author knows about the inner sizes rather than by the
  boundary below. That is exactly the promise this operator exists to make, so
  the shape is refused instead of being quietly weakened. Every admitted shape
  answers at most one element per element, which is what makes the emissions of
  one element bounded by the length of the chain, and the emissions of the end
  of the stream bounded by the declared bound times that length.
- **A nested `GroupBy`.** A second bound and a second key table per key of the
  first is a real feature with a real contract to state, and it is not this one.

### The document states the group flow

A keyed stage is the first shape of this vocabulary whose payload carries other
stages. It has to: leaving the flow out would make two graphs that observably
differ look identical — grouping through a `Take(2)` and through a `Grouped(3)`
would be one document and one fingerprint — and this vocabulary's rule is that
what changes a graph observably belongs in the payload. So the contract
`local-group-by-parameters@v1` carries `maxActiveKeys`, `overflowPolicy`, and
`group` — an array of one entry per stage, each naming its own stage reference
and carrying its own payload, validated by the very reader that stage uses when
it stands on its own. What the stages *do* is not there, exactly as it is
nowhere else in a local document.

What that payload is *not* is a nested document: there are no identities, no
ports, and no edges, because a group flow is a chain fused per key and its order
is the array's order. And because both planes now describe the flow, the planner
checks that they are describing the same one — a group flow of a different
length is a document and a binding built from two different graphs, and a
different shape at the same position is one graph whose halves were edited
apart. Both are reported by name, and both are unreachable through the authoring
API, which writes the payload from the very descriptors it binds.

One refactoring came with that. The thirteen fused element shapes were thirteen
arms of the planner's switch and are now one factory — `Fusible` — read by both
callers: a chain of a document builds each of them once, and a keyed stage
builds one of each per key. Everything that costs something (reading the
payload, wrapping the author's delegate, the reflection inside that wrapping)
happens once when the plan is built; what a factory does per key is construct an
object over values it is already holding. A shape that answers the factory and
stands where it cannot falls through to the switch, which reports the position
exactly as it did when the arms were there.

### What this wave does not do

**There is no `Source<Source<T>>` and this is not one.** The substream is a flow
declared at authoring time, not a stream an author receives and consumes; split,
prefix-and-tail, and dynamic hubs are the other substream rows and are
untouched. A group flow that could itself contain a junction or an asynchronous
stage is the feature that needs sub-graphs materialized per key, and nothing in
this engine has a shape for that.

**One composition of each kind is proven, and the general statement is not.**
Two keyed branches joined by a merge keep their own tables, and an element a
timed batch produced from a timer's wake is an ordinary element to a keyed stage
below it — those two are measured rather than argued, because "composes by
construction" is an argument. Every other topology is not: a keyed stage on one
leg of a broadcast, one inside a cycle, one under a partition, and a group flow
that would like a window of its own are all unasserted, and the last of them is
refused outright.

**The order of an eviction's residues against the arriving element's own
emissions is implemented and not observable.** The eviction happens first, so
its residues are collected first; but with one flow instantiated per key, a
substream whose first element emits is a substream that holds no residue, so no
graph can be written in which both happen for one element. The order is recorded
as the implementation's and not as a tested claim.

**The bounds are proven as how far a held source got and as what a run
delivered**, which is the accounting every bounded-memory claim in this suite
makes; nothing here weighs a heap or counts the substream table's memory.

**The reuse of one emission list is argued rather than measured.** The claim
that nothing downstream can call back into a keyed stage is a property of the
run's walk — it only ever enters the stages below — and a test could not fail if
it stopped being true; what the suite does assert is the case that would break
first, two keyed stages fused in one segment with the upper one's residues
travelling through the lower one's table.

**And the whole of it is the local runtime.** No keyed stage has been
materialized through a silo, and every graph carrying one is `nondeployable`
because it is a lambda-bound local stage.

## M5.1 (the injection seam and local supervision scopes) — as implemented

The first phase of M5, and the one ADR 0007 said had to come first: the tests
for a policy cannot be written against luck, so the failure-injection seam
lands before anything it is used to prove.

The shape is: **supervision is a scope, and a scope is a stage.** A
`local/supervised@v1` node carries a policy and the chain it answers for, in
its payload, exactly as a keyed stage carries its group flow. No `StageNode`
schema change, no per-node annotation, no Abstractions change — and the
consequence a reader should hold onto is that a supervision policy is a fact of
the document and of the fingerprint taken over it, which is what makes it a
policy a cluster could one day honor rather than a runtime flag.

### The injection seam is a stage, not a hook

`local/fault-point@v1` is an ordinary element stage of the local vocabulary
whose payload is its **arming** — `mode` (`never`, `once`, `always`) and
`firstFailure`, a one-based arrival — and whose binding is what it throws. It
sits in the core vocabulary for the reason `local/sink-probe@v1` does: the
vocabulary is one closed set and a document has to be able to name what it is
running. Every spelling an author can reach it through lives in the Testing
package (`TestFlow.FaultPoint<T>`), and so does the public arming vocabulary
(`FaultPointMode`), so the shipping package publishes no words for injecting
faults.

**The arming is declared rather than only armed at run time, and that is not a
convenience.** A run starts as soon as it is materialized, so a test that could
arm only through a resolved control would be racing the very elements it wanted
to fail. The declared arming makes "fail the second element" a fact of the graph;
the control (`IFaultPoint`) is for re-arming a run whose elements a test is
already pacing through a source probe, and for reading `ElementsSeen` and
`FaultsThrown`. Re-arming counts from the *next* arrival where the declared
arming counts from the first of the run, which is the reading a test wants in
both places.

**A fault point's counter is not stage state.** The `LocalFaultPoint` is built
once when the plan is built and every instance of the stage shares it, so a
scope that restarts its stages does not turn "fail the second arrival" into
"fail the second arrival since the last restart". A retry's re-offer is an
arrival of its own, which is what makes "the scope really did retry" a number
rather than an inference.

**It is the one occurrence in this vocabulary whose control slot is optional.**
A fault point inside a scope is not a node — the stages of an inner chain have
no identity — so there is nothing for a slot to name; the authoring surface
refuses the control-bearing spelling there by name rather than declaring a slot
nothing could resolve. The cost is stated rather than hidden: a fault point
inside a scope cannot be re-armed or read at run time, and its declared arming
is the whole of what it does.

### Four forms, one stage, one method

`Resume`, `RestartStage`, `Retry`, and `Recover` are one attached stage whose
`Apply` is a retry loop every other form leaves on its first pass. The walk
through the scope's chain is `LocalRun.Advance` read one level down — the
keyed stage's substream walk with one instance instead of one per key — and the
emissions go into a list the run reads after the method has returned, so
nothing re-enters the run and this is a stage rather than a pump.

- **`Resume`** drops the failing element and keeps everything the chain was
  holding. A scan inside the scope goes on counting; a half-filled batch stays
  open.
- **`RestartStage`** drops the element and rebuilds every stage of the chain
  from the very factories a fresh run builds them from, which is the group-by's
  per-key instantiation machinery reused rather than reinvented. A restarted
  scope is indistinguishable from one that has just started.
- **`Retry`** re-offers the element **to the scope's first stage**, up to
  `MaxAttempts`, waiting the declared ladder's rung on the run's clock between
  attempts, and applies `OnExhaustion` — `Fail` (the default), `Resume`, or
  `RestartStage` — to an element that used them all. Re-offering to the first
  stage is the declared semantics and the reason to keep a retrying scope small:
  a stateful stage inside one sees the element once per attempt, which the suite
  asserts by value rather than footnoting.
- **`Recover`** emits a declared fallback and ends the scope's stream
  *successfully*: everything above the scope stops, everything below it drains,
  the result slots resolve, and the run reports success. Recovering with an
  **alternate source** is a different capability with a boundary of its own and
  is deliberately not a knob here.

The pair of tests that carries the milestone is `Resume` against
`RestartStage`: the same graph over the same elements with the same injected
failure, one enumeration member apart, producing `[1, 4, 8]` and `[1, 3, 7]`.
A test that counted elements would pass for both.

### The backoff ladder, and no jitter in v1

The ladder is an explicit `TimeSpan` array in the payload
(`backoffTicks`, in ticks, like every other duration this vocabulary carries),
not a base and a factor: a ladder is what a document can state exactly, so a
reader sees the waits the run will take and no reader has to reproduce an
arithmetic nobody wrote down. **The last rung repeats**, so a ladder shorter
than the attempt count reads as "and then this long every time"; an empty ladder
means every re-offer happens at once. A rung of **zero is admitted**, which is
the one place this vocabulary's duration rule bends — "try again now" is the
ordinary shape of a first rung, where a delay of no time describes an operator
that should have been left out.

**There is no jitter in v1, and the reason is stated rather than deferred
silently.** ADR 0007 says the ladder is "jittered by the runtime"; jitter
answers a question a per-element retry inside one run of one process does not
ask — it spreads a *fleet's* restarts, and there is no fleet here — and adding a
random source would make the one thing this phase has to prove, that the waits
are exactly what the document says, a statistical claim instead of an asserted
one. The payload's shape admits jitter later without a document change, and the
restart-section row is where the herd is real. Recorded as a deferral.

The waits go through the checkpoint-5 wait discipline unchanged: reported idle
for their duration, released by both stops, and followed by a park. So a pause
during a backoff takes effect at once and holds the re-offer however far the
clock is then advanced; a shutdown releases the wait and the re-offer happens
without the rest of the rung being paid, so the element in hand is delivered;
and a cancellation is raised and abandons it. All three are asserted on a clock
the test moves by hand.

### What a scope does not catch, and where the engine drew each line

Four lines, and each is a different kind of claim.

- **A failure outside every scope fails the run**, unchanged since M2. Proved as
  a *contrast*: one graph with the fault point inside the scope's chain and one
  with it a stage earlier, identical in everything else, ending contained and
  failed respectively. The same holds below a scope and on a junction leg beside
  one.
- **A cancellation is not a failure and no form weakens it.** The scope catches
  `OperationCanceledException` only to rethrow it. A scope that caught
  cancellation would turn a stop into a stream that would not stop.
- **A failure of the machinery rather than of an author's stage is a refusal at
  materialization.** A payload this runtime cannot read, a chain holding a shape
  a scope cannot execute, and two planes describing different chains are all
  `InvalidOperationException` before the run has an element to supervise. That
  is the engine's line: everything a scope can answer for happens *inside*
  `Apply`, after the plan was accepted.
- **A failure raised while a stream is ending is not supervised.** The residue
  walk — the scope's own, and the run's — has no failing element to drop,
  nothing to re-offer, and no fallback question to ask, so it travels to the run
  like any unsupervised failure. This is v1's honesty rather than an oversight,
  and the test that says so injects the failure exactly there, in a batch's
  projection of the partial group it hands over.

### The chain holds element stages only, and that is v1's honesty

`RunsInsideAScope` is the group flow's list plus the fault point, and the two
differences are the whole of what a scope is against what a keyed stage is: a
scope owns **one** instance of its chain rather than one per key, so a fault
point's arrival counter means what a test wrote down.

Three shapes are refused for reasons of this operator's own:

- **A flattening stage**, and this is the sharpest one. What a scope hands the
  run for a `SelectMany` is a sequence the run reads *after* the scope's own
  method has returned, so a failure raised while it was enumerated would happen
  **outside the scope it appears to be inside** — supervision that silently did
  not apply, which is worse than a refusal.
- **A nested scope.** A policy inside a policy has a contract of its own to
  state — which answer wins, what a restart of the outer one does to the inner
  one's state — and it is not this one.
- **A `GroupBy`.** A key table whose reset is a scope's business is a second
  feature, and it is not this one. The composition that is *not* refused is the
  useful one: a keyed stage beside a scope rather than inside it.

**A scope is refused inside a group flow**, and by the clause a group flow has
always had rather than by a new one: a scope reads the run's clock, so one
instance per key of it is not something a fused stage can hold. A fault point is
refused there too, because one counter per key is not what "fail the second
element" means to the test that wrote it.

### The document states the policy and the chain

`local-supervision-parameters@v1` carries `form`, `scope` — one entry per stage
with that stage's own reference and payload, `LocalInnerChain`'s array, shared
with the keyed stage — and, **only for the retrying form**, `maxAttempts`,
`backoffTicks`, and `onExhaustion`. A fixed shape would have been easier to read
and would have been a lie: an attempt count on a scope that resumes is a number
nothing reads, and a reader finding one would have to guess whether the graph
was generated wrong or the engine was ignoring it. So the admitted member list
is a function of the form, the authoring guard refuses a retry-only member on
the other three, and the unknown-member report refuses a hand-written document
that carries one.

What is **not** in the payload is the fallback a recovering scope emits. It is a
value of an element type no local document names, so it travels in the binding
table exactly as `Source.Single`'s element does; ADR 0007's other half of that
split — a canonical constant, deployable by construction — belongs to the
registered vocabulary, where element contracts are real.

**No form names an exception type.** A policy filtering by type would need CLR
names in a document, which the definition plane forbids, or a declared failure
taxonomy, which is real design work owed its own evidence. V1 supervises every
failure inside the scope alike; the taxonomy is a recorded deferral.

One refactoring came with all this. `LocalGroupByParameters` no longer owns the
inner-chain encoding: it is `LocalInnerChain`, read by both the keyed stage and
the scope, with a `Words` value carrying what each owner calls its chain and
which shapes it admits. The refusals stay in each owner's own vocabulary — a
group flow "runs fused per key", a scope "owns the execution of its chain
element by element" — spoken by the reader the runtime itself uses, so a
hand-written document and an authored one are refused in the same words.

### Counting is what makes a dropped element observable

Two counters beside `DroppedElements`, both internal for the reason that one is:
what an author will read them through is a monitor, and publishing a bare
counter now would fix that shape by accident.

- **`SupervisedFailures`** — every failure a scope intercepted, which for the
  retrying form means once per *failed attempt*. It answers "how much did this
  run swallow", and an attempt that failed was swallowed.
- **`PoisonElements`** — ADR 0007's poison element counted as such: an element
  that used every attempt it was given, whatever the exhaustion answer then did
  with it. It moves for the failing answer too, so a run that failed after
  exhausting its retries is distinguishable from one that failed on its first
  element.

Two numbers rather than one, because they answer different questions and one
number could not answer both.

### What this phase does not do

**Nothing durable.** No checkpoints, no cursors, no commit marks, no resume, and
no storage contract — those are M5's next phases, and nothing here writes
anything anywhere.

**Nothing distributed.** Every graph carrying a scope or a fault point is
`nondeployable`, because it is a lambda-bound local stage; no scope has been
materialized through a silo and no crash test exists yet. The seam ADR 0007
describes as "named seams a test arms to throw *or to kill the host*" is here in
its throwing half only.

**No restart-section form.** Restarting a source, flow, or sink *section* with a
budget is the coarser-grained row and is untouched; so is
`WatchTermination`, which ADR 0007 returns to the control-slot machinery in a
later phase.

**No exception-type filter**, as above, and no per-scope observability: the two
counters are per run, so a graph with three scopes reports one number for all of
them.

**One composition of each kind is proven and the general statement is not.** A
scope on a junction leg, two scopes in one chain, a scope below a keyed stage,
and a fault point inside a scope driving all of it are measured; a scope inside
a cycle, under a partition, or in front of a merge-map are unasserted.

**The "several residues as the scope's own stream ends" path is implemented and
not observed.** The element vocabulary has an emit-and-complete and no
emit-many-and-complete, and the scope asks for the completion through the
attachment instead — the walk a window's timer already takes. No chain the scope
admits has been shown to produce that case, and the branch is recorded as
defensive rather than tested.

## M5.2 (the checkpoint model, the storage contract, and local resume) — as implemented

The second phase of M5, and the one ADR 0007 said would follow the injection
seam: a **checkpoint** as a value, a **store** with the coordinator's fencing,
three **seams** that put something in a checkpoint, and a **resume** that reads
one back. Nothing here is distributed — the local runtime proves the *model*,
and a run outliving the process that was running it is M5.3's with a cluster
under it.

### A checkpoint is a canonical value with five parts

`LocalCheckpointDocument` writes and reads one document, and the reader refuses
what it does not declare, exactly as every stage payload of this vocabulary
does. Its five members are ADR 0007's five parts and all five are always
present:

```json
{"cursors":{…},"fingerprint":"sha256:…","marks":{…},"revision":3,"states":{…}}
```

Three of them are tables keyed by **node identifier**, because a node identifier
is the one name a document and a checkpoint of it agree on. A shape that varied
with what a run happened to have would make an absent `marks` ambiguous between
"no sink marks" and "written by a version that had none", so a run with nothing
of a kind writes an empty object rather than leaving the member out. The golden
test is the load-bearing one: two captures of one run state produce
byte-identical documents whatever order the plan enumerated its seams in, which
is what makes "the document changed" mean "the run moved".

**Every value inside it is a canonical value, and that is the seam's requirement
rather than the document's convenience.** A cursor's position, a scope's state,
and a sink's mark are produced by an adapter, a scope, and a sink respectively,
and each hands over a `CanonicalJsonValue` — no object, no CLR type name, no
serializer's opinion. That is the wire discipline unchanged, and it is what lets
one process write a checkpoint another reads. A seam that cannot serialize into
the canonical plane declares nothing and contributes nothing.

### The storage contract is the coordinator store's, generalized

`ICheckpointStore` (in `Orleans.Dataflow.Hosting`, beside the factory seam) is
`ReadAsync` / `WriteAsync` / `ClearAsync` over one document per `(GraphId,
RunId)` pair, ETag-guarded. ADR 0007 writes the key as `(GraphId, RunId,
"checkpoint")`; the third component is the interface here rather than an
argument, because this interface holds checkpoints and nothing else.

**A locally authored graph has no identity of its own**, so every one of them is
`anonymous` and the run name is what actually separates two checkpoints in a
store. That is the same statement `LocalVocabulary.AnonymousGraph` has always
made about result slots, read over durability: a checkpoint of a *different*
local graph under the same run name is caught by the fingerprint rather than by
the key, and a deployment that wants keys to separate graphs names its graphs —
which is what a `PipelineDefinition` is for.

**`ClearAsync` exists and the engine never calls it.** Forgetting a finished
run's checkpoint is an operational decision — how long a completed run's
position is worth keeping is a deployment's question and not a runtime's — so
the contract carries the verb and the runtime leaves it alone.

The refusal is the contract and not an implementation detail:
`CheckpointConflictException` carries both ETags, and **a writer whose write is
refused stops rather than retries**. Retrying with the fresh ETag would
overwrite the truth a fresh attempt is building with a snapshot of a run that
owns nothing — which is the corruption the ETag exists to prevent. The run
fails with that exception, unwrapped, on `RunHandle.Completion`.

`InMemoryCheckpointStore` ships in the Testing package, which is where ADR 0007
put it; a durable one is the deployment's, exactly as the coordinator's is. It
is a **store and not a mock** for the reason `SurvivingCoordinatorStore` is one:
the property the model rests on is optimistic concurrency, so an implementation
that accepted every write would let a test prove nothing. Its `Supersede` is the
coordinator store's own, read over a checkpoint — the only honest way to produce
a real conflict against a live run.

### Timing is declared, and the element bound holds the run where it says

`DurableRunOptions` carries the store, the run identity, and up to two bounds:
an `Interval` on the run's own clock and an `EveryElements` count. **A run that
declares neither never touches the store**, which only the store can say and
which the suite asserts against it.

The two bounds are asked in different places, and the difference is honest
rather than incidental.

- An **interval** is the capture loop's own wait, on the run's `TimeProvider`,
  so a controlled clock moves it. It records whatever position the run had
  reached, which is the answer a timed capture can give.
- An **element bound** is reached on a source segment's own thread, and the hold
  is requested *there*, before that segment takes another step. Between "the
  bound was reached" and "a loop beside the run woke up" a fast source would
  deliver an unbounded number of further elements; requesting the hold from
  inside the segment is what makes the stored cursor **exactly** the element the
  bound named. A source of six elements at a bound of three stores cursor six,
  as a number rather than a range.

Elements are counted as **admitted** — every element a source of the run hands
to the graph, summed across sources. Not committed at a sink, which is what the
marks say and is a different number for every graph that filters; and not per
source, which would make a two-source graph's cadence depend on which source was
faster.

### A capture is hold, snapshot, resume — and the cost is stated

All three are machinery that already existed. The hold is `LocalPause`, reached
exactly as `RunHandle.PauseAsync` reaches it; the snapshot is three reads over
seams that are quiescent by construction while the hold lasts; the resume is
`LocalPause.Release`. ADR 0007 asked for the pause machinery to be reused rather
than reinvented, and `LocalCheckpointer` is the whole of that reuse.

**The cost is that a capture holds the run for its duration**, the store write
included, and nothing overlaps. That is deliberately the simple answer:
something cleverer is only worth building once the simple one has been measured,
and `RunHandle.CheckpointHold` is the measurement — the sum of every hold on the
run's own clock, beside `RunHandle.Checkpoints`. Both are internal for the reason
`DroppedElements` is: what an author will read them through is a monitor.

**The pause gate grew a second holder, and that was a real defect rather than a
tidy-up.** An author pauses through the handle and a capture holds the run to
snapshot it, and a durable run that is also paused by hand has both at once. A
single gate meant the capture's release opened it for both — silently resuming a
run its author had stopped. `LocalHold` is two flags and not a count: a count
would have broken the other half of the contract, which is that pausing twice
and resuming once leaves the run moving. The regression test fails against the
single-gate behaviour and passes against this one.

### The cursor seam: a source that knows where it is

`LocalSourceCursor` is the runtime seam, and it has three moving parts and no
more: open at where the checkpoint said, advance when an element has been
delivered, and answer where you are. `from-enumerable` declares one —
`LocalIndexCursor`, whose position is `{"index":n}` — and it is the proof
vehicle rather than a general promise.

**Advancing is the run's call and not the sequence's.** A sequence learns its
element was wanted only when the next one is asked for, and the moment between
those two — element delivered, next not yet asked for — is exactly where a pause
lands; a cursor that counted pulls would be one behind at every capture. The run
therefore advances the cursor when an element has travelled through the segment
it entered, which is a fact only the pump knows, and the stored position is
exact rather than approximately right in a safe direction.

**What this particular cursor requires of the author is stated rather than
assumed.** Reopening re-enumerates the very sequence the author handed over and
skips that many elements, so a sequence that enumerates differently the second
time resumes into different elements, and one shorter than the stored position
fails the resume by name. A source over a list has every business declaring this
cursor; one over an iterator that reads a socket has none.

**Every other local source declares nothing and resumes from now.** The
per-source table is in [ADAPTERS.md](../ADAPTERS.md) rather than generalized
here.

### The durable-state seam: a scope, and it is not a supervision form

`local/durable@v1` owns a declared chain and can hand that chain's whole state
to a checkpoint and take it back. It is a **scope** for the reason supervision
is one — what survives a resume is a decision about a *region*, it has to be
visible in the document for a cluster to honor it, and a region is a stage in
this vocabulary — and it is the one shape of the vocabulary that requires a
capability token of its own, `durable-state`, which has existed as a word since
M0 and earns its keep here.

**It is deliberately not a form of the supervision scope**, and the decision is
worth its sentence. The two answer different questions — what a failing element
costs, and what a dead process costs — and folding them together would force
every author who wants durable state to declare a failure policy and every
author who wants a retry to decide about durability. Worse, the one place they
overlap is a contradiction: `RestartStage` resets every state in its scope and
`durable-state` keeps every state across a resume, so a scope that was both
would have a contract with a hole in it. Kept apart, each says one thing
exactly, and the composition an author actually wants — a durable scope inside a
supervised section — stays a composition, which is what a scope being a stage is
for.

**The chain admits the shortest of the three inner-chain lists**, and the reason
is the one thing this scope promises. A stage inside one has to hand its state
over *as a canonical value*: a `select` and a `where` hold nothing, a `take` and
a `skip` hold a count, a fault point's arrival counter belongs to the run rather
than to the stage (M5.1's own statement about restarts, read over a resume), and
a `scan` holds a value of a type no document names. Everything else — a
`distinct`, a `grouped`, a `sliding`, the two prefix operators — is refused **by
name**, at authoring and by the payload reader in the same words, because
admitting one would produce a resume that silently reset state the scope had
promised to keep.

### The exportable-state facet, and the one refusal a document cannot make

`LocalElementStage` grew three optional members: `ExportsState`, `ExportState`,
and `RestoreState`. It is a property of the **built stage** rather than of its
shape, because for one shape the answer depends on the binding: a `scan` exports
only when its author bound a state codec, and a codec is a pair of delegates, so
no document can state it.

The codec is the author's and it has to be: a state is a value of a type no
document names, so only the author can say what it looks like written down. The
spelling is a `Scan` overload taking `export` and `restore`, and the consequence
is stated where it bites — **two graphs whose scans differ only in carrying one
have the same fingerprint**, so "this scan exports state" is refused when the
plan is built rather than when the document is validated. That is the same line
M5.1 drew for every disagreement between a scope's two planes: a machinery
failure fails materialization, before the run has an element.

The plan therefore builds the scope's chain and asks each instance, which it
would have done anyway — a durable scope owns one instance of its chain, not one
per key.

### The commit-mark seam: after the side effect, never before it

`local/marking-sink@v1` runs a callback and then advances a count, and the order
is the whole contract. A callback that throws leaves the mark where it was, so
the number always describes work that finished; a mark that moved first would
promise a commit that had not happened, and a resume's duplicate window would
become a loss window. The stage lives in the core vocabulary for the reason
`local/sink-probe@v1` and `local/fault-point@v1` do — a document has to be able
to name what it is running — and the only spelling lives in the Testing package
(`TestSink.Marking<T>`), because a real committing sink is an adapter's and this
one exists to prove the seam.

**The mark counts committed deliveries and is not a source position.** The two
agree for a graph that neither drops nor multiplies elements between a source
and its sink, and part company across a resume, because a replayed element is a
second delivery of one element. It is **restored** across a resume, so a run
that has committed eleven elements over two attempts says eleven rather than
starting over.

### Resume, and the arithmetic that makes at-least-once a number

`LocalDataflowHost.MaterializeDurableAsync` starts a durable run;
`MaterializeFromCheckpointAsync` continues one. Resume is the **same `RunId`**
continuing: the checkpoint is read with its ETag, and the resumed attempt
presents that ETag at its own first capture, so a stale attempt still writing
loses to it exactly as a superseded coordinator does.

Four refusals, all by name and all before the run's first element: the store
holds nothing for that run; the stored document is not one this runtime can
read; it was taken of a **different fingerprint or revision** (v1's
same-revision rule, with cross-revision migration a recorded deferral); or it
names a node this graph has no such seam for. A seam the plan has that the
checkpoint does *not* name is the opposite case and is not a refusal — it is a
source that had delivered nothing or a scope that had not been reached, and each
starts from its beginning as a fresh run would.

**What the replay costs is measured rather than bounded.** In the fused proof —
twelve elements, a capture every three, an injected failure at the ninth — the
checkpoint stores cursor six and mark six, the crashed attempt had committed
eight, and the resumed attempt commits `[7…12]`. The duplicate window is exactly
the two elements between the stored cursor and the mark at the crash, asserted
by value, and the union of the two attempts is the whole stream with nothing
missing.

**The durable state does not duplicate, and the reason is that a checkpoint is
one moment.** The scope's state and the cursor are captured at the same safe
point, so a replayed element is added to the scope's state exactly once: the
running total in the resume proof ends at the true sum of the whole stream even
though the sink saw two elements twice. At-least-once is the sink's window and
not the scope's.

**And there is a loss window, measured rather than glossed.** A graph that holds
elements between a cursor and its mark at capture time loses them: a `grouped(5)`
outside a durable scope, captured at cursor eight with one group committed,
resumes at eight and the three elements the batch was holding are gone —
`8 − 5 = 3`, a number the checkpoint itself hands over because it carries both
cursors and marks rather than one of them. That is v1's honest boundary rather
than a defect: the batch is not durable, so it reset, and the cursor had counted
what it was holding. A graph that must not lose them puts the batch inside a
durable scope, or puts the marking sink where the elements actually land.

**Nothing anywhere claims exactly-once.**

### What this phase does not do

**Nothing distributed.** No silo, no process death, no host-killing half of the
injection seam. The "crash" here is an injected failure that kills the attempt,
and what survives it is an in-memory store in the same process. The row for
durable resume after process or silo failure therefore advances to *this half
implemented*, and its other half is M5.3's. **M5.3 shipped it** — a store behind
a silo, activation-driven resume, and a crash suite over real silo kills — and it
needed no change to anything in this section, which is the strongest thing that
can be said about a model: see the M5.3 section of
[ORLEANS-RUNTIME.md](ORLEANS-RUNTIME.md).

**No cursor but one.** `from-enumerable` declares an index cursor and every
other local source declares nothing. An Orleans stream sequence token is the
cursor the model was designed for; **it arrived in M5.3**, through one public
overload of the provider seam — `DataflowStageRuntime.Source(open, cursor)` over
a `DataflowSourceCursor` the adapter's opener closes over — which is the only
touch that phase made to this package, because a cursor declared by a registered
source has nowhere else to be declared.

**No checkpoint on a clean end.** A run that completes has an outcome and does
not write one, which is deliberate and is what makes "the last stored capture is
what a resume replays from" true without a special case for the last one.

**No overlap, no incremental snapshot, no copy-aside.** A capture holds the run,
including for the store write. The cost is measured so that a cleverer answer
can be argued for with a number.

**No cross-revision migration** and no compatibility rules: a resume against a
different fingerprint or revision is refused by name, and migrating a checkpoint
across a changed document is M5's later phase or a recorded deferral.

**No per-run monitor.** `Checkpoints` and `CheckpointHold` are internal beside
the other counters, for the reason those are.

**One composition of each kind is proven and the general statement is not.** A
durable scope beside a supervision scope, a durable scope over a chain of five
admitted shapes, a marking sink behind a batch, and a capture during an author's
pause are measured; a durable scope on a junction leg, inside a cycle, or in
front of a merge-map are unasserted.
