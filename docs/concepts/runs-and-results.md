# Runs and results

*How does a running pipeline end, and how do you get a value out of it?*

Building a graph starts nothing. This page is about the other half: turning a
graph into something that executes, getting values back from it, and — the part
that repays careful reading — the four different ways it can stop.

## Materializing

One call turns a graph into a running thing:

```csharp
await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(graph, cancellationToken);
```

[Materializing](../reference/glossary.md#materialize) validates the document
against the host's [catalog](../reference/glossary.md#catalog), builds an
execution plan, starts the engine, and hands you a
[run handle](../reference/glossary.md#run-handle). It is the only call in the
library that starts anything; everything before it was value construction.

**A run is not the graph, and two runs share nothing.** Materialize the same
graph twice and you get two independent [runs](../reference/glossary.md#run):
fresh source enumeration, fresh stage state, fresh fold seeds, no shared mutable
anything.

```csharp
await using RunHandle first  = await host.MaterializeAsync(counting);
await using RunHandle second = await host.MaterializeAsync(counting);
```

```text
first 5, second 5
same handle: False
```

The one thing that is *not* freshened is state you captured inside your own
lambdas. A counter declared outside the graph and incremented inside a `Select`
is yours to keep fresh; the runtime cannot see it and does not pretend to.

The handle is `IAsyncDisposable`. `await using` is the ordinary shape, and
disposing it stops the run and waits for it.

## Result slots, and why they exist

A pipeline that produces a value does not hand you its sink. It declares a
**named, typed** [result slot](../reference/glossary.md#result-slot) when you
close the graph, and you read it from the run handle:

```csharp
RunnableGraph graph = Source.From(orderEvents)
    .Where(order => order.IsValid)
    .Select(OrderDocument.FromEvent)
    .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> processed);

await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(graph, cancellationToken);

long processedOrders = await run.GetValueAsync(processed, cancellationToken);
```

> From the `first-pipeline` scenario,
> [`samples/Orleans.Dataflow.Samples/CSharp/FirstPipeline.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/FirstPipeline.cs).
> The F# frontend hands the slot back in a tuple instead of an `out` parameter,
> and builds the same document.

The indirection is doing three jobs, and each one is a thing the alternative
cannot do.

**A graph is a value that outlives the call that built it.** If the result were
"the sink object you passed in", then reading a result would mean holding a
reference to a live object — which works exactly until the graph is stored,
compared, sent to a silo, or resumed in another process. A name survives all of
that; an object reference survives none of it.

**A pipeline may produce more than one value.** A [branching](branching.md)
graph ends in several sinks and each can declare its own slot, so one run answers
several questions. The type of the graph does not change: `RunnableGraph` is not
generic over its results. A generic parameter would shorten exactly one program
and collapse into tuple threading the moment a graph has two results.

**The type is yours, not the document's.** The document stores a result
*contract* reference and no CLR type. `ResultSlot<long>` is how your process
carries the type across the gap: closing the graph produces it, so the type comes
from the sink you actually used rather than from an assertion you made.

Two rules follow that surprise people once each:

- **A slot resolves only against a run of the graph that declared it.** Passing a
  slot from one graph to a run of another is refused with a message naming which
  identity disagreed — see [Graphs and identity](graphs-and-identity.md#the-fingerprint-identifies-shape-not-behavior).
- **Result slots resolve at the end of the run, but control slots resolve at its
  start.** A fold's value cannot exist until the stream has ended. An ingress
  queue must exist immediately, because producers push into a run that is already
  running. So a control's task is already complete when the handle is handed over,
  and how the run ends never changes it: a run that dies at start still hands out
  its queue, and every later offer to it answers with the refusal that says the
  run has ended.

`GetValueAsync` is callable before, during, and after the run, and asking twice
gives the same answer twice. The token it takes cancels **your wait**, never the
run — a later call still resolves.

## The four ways a run stops

This is the section to read twice.

| How it stops | `Completion` | Result slots | In-flight work | `WatchTermination` | Snapshot status |
|---|---|---|---|---|---|
| The source ran out | succeeds | resolve with final values | n/a — nothing is left | resolves `Completed` | `Completed` |
| You shut it down | succeeds | resolve with the state accumulated so far | **drained** | resolves `Completed` | `Completed` |
| You cancelled it | cancels | **cancel, resolving nothing** | **abandoned** | cancels | `Canceled` |
| Something threw | faults with that exception | fault with the same exception | abandoned | resolves `Failed` | `Failed` |

In every case the source's enumerator is disposed, on every path.

### It ran out

The ordinary ending. The source has no more elements, everything downstream
drains, folds resolve, and `Completion` succeeds.

### You shut it down — the graceful stop

`ShutdownAsync()` means **"stop producing, and let what is already in flight
finish."** The run stops pulling from the source and then completes exactly as if
the source had ended: a buffer's contents are delivered, in-flight asynchronous
callbacks are awaited, an aggregate resolves its slot with the state it has
accumulated, and `Completion` reports **success**.

### You cancelled it — the abrupt stop

Cancelling means **"stop now."** The materialization token firing, or
`DisposeAsync()`, cancels the run: buffered and in-flight work is abandoned,
slots resolve nothing and cancel with the run, and `Completion` reports
cancellation.

### The two, side by side

Same graph, same infinite source, stopped at the same point by the two different
verbs:

```text
shutdown      Completion: completed  slot: 3148                snapshot: Completed
cancellation  Completion: cancelled  slot: cancelled, no value snapshot: Canceled
```

That is the whole distinction and it is the most commonly mis-read thing in the
library, so here it is in one line: **shutdown is "we are done", cancellation is
"abandon this".** A shutdown gives you your fold. A cancellation gives you
nothing, on purpose, because the value would be a partial answer nobody asked
for.

Three practical notes:

- **A stop is observed between elements.** A source that blocks inside a pull
  delays either stop until that pull returns. The runtime does not interrupt your
  code, ever.
- **Neither `ShutdownAsync` nor `DisposeAsync` ever throws** — not for the
  cancellation disposal itself caused, and not for a failure the run had already
  suffered. A teardown that replaced your own exception under `await using` would
  hide the thing worth reading. How the run ended stays on `Completion` and on
  the result tasks.
- **Both are idempotent.** Asking twice, or asking after the run ended, waits for
  the same outcome again.

### Something threw

A failure wins over everything. The exception you threw reaches you **unwrapped**
— awaiting `Completion` rethrows that very instance, not something wrapping it —
and every result slot faults with the same exception. No element is observed
after a failing one.

```text
Completion threw InvalidOperationException: element 3 is bad
slot threw InvalidOperationException: element 3 is bad
```

If nothing in the graph declared a policy for that failure, that is the end of
the run. [Failure and supervision](failure-and-supervision.md) is about
declaring one.

## Reading a run rather than inheriting it

`Completion` takes the run's outcome **on**: awaiting it makes the run's failure
your own. That is the right shape for code that treats a failed pipeline as its
own failure. It is the wrong shape for a supervisor, a log line, or a metric,
which want to *react* to an ending rather than catch it.

So there is a second surface. `WatchTermination` is a task that **resolves** with
the [ending](../reference/glossary.md#ending) as a value:

```csharp
RunEnding ending = await run.WatchTermination;
```

```text
WatchTermination resolved: failed with System.InvalidOperationException: element 3 is bad
```

A failed run's watch completes *successfully*, carrying the failure's type name
and message as facts to read. Note what is not there: **there are only two
endings**, `Completed` and `Failed`. A cancelled run has no ending, because
cancelling abandons a run rather than finishing one — so the watch of a cancelled
run cancels rather than resolving a third kind. That is not pedantry. On a
cluster, "this run is over" is a claim that retires a durable name, and a
deactivation cancels the run it was hosting; if cancellation counted as an
ending, every silo recycle would permanently retire the run it was carrying,
which is the exact thing durability exists to prevent.

The watch never disagrees with `Completion`: both resolve, both report the same
failure, or both cancel.

The failure travels on the watch as a type name and a message rather than as the
exception object, because that is the shape a clustered host can also fill — an
exception chain is only as serializable as its least prepared link. The instance
itself is still on `Completion`, unwrapped.

## The snapshot

`Snapshot()` is one reading of where a run is and what its counters have reached:

```text
Failed: dropped 0, supervised 0, poison 0, checkpoints 0, held 00:00:00
```

Five counters, always printed whether or not any has moved, so that two readings
are comparable and a healthy run's line is not ambiguous:

| Counter | Means |
|---|---|
| dropped | Elements discarded by declared overflow policies. |
| supervised | Failures a [supervision scope](../reference/glossary.md#supervision-scope) intercepted — one per failed attempt. |
| poison | Elements that used every attempt they were given. |
| checkpoints | Checkpoints written; zero forever for a run without durable options. |
| held | Total time [checkpoints](../reference/glossary.md#checkpoint) held the run. |

The status has **four** values — `Running`, `Completed`, `Failed`, `Canceled` —
where an ending has two, and the difference is deliberate: a snapshot answers
"where is it", so a live run says `Running` and a cancelled run still has a place
it stopped.

Two things to know about the reading. It is safe to call at any point in a run's
life, from any thread, and it never throws; a run that has ended reports its
final counters forever. And it is **a reading, not a consistent cut**: the
counters are read one after another while the run may be moving, so an element
supervised between two reads lands in one counter and not yet in another. Each
individual counter is exact.

A graph with three supervision scopes reports one `supervised` number for all of
them. Per-scope resolution does not exist.

## Pausing is neither stopping nor ending

`PauseAsync()` stops a run without ending it, and `ResumeAsync()` continues it
from exactly where it stopped. A paused run has no outcome, no resolved result,
and nothing to release; every segment parks at the same point between elements at
which it would observe a stop, and the returned task completes once all of them
are there and no callback is still running.

What "nothing is in flight" means here is worth stating, because the strict
reading would deadlock: an element already produced and waiting — in a buffer, in
a segment's hand at a full boundary, in an asynchronous stage's window, or at a
sink nobody has asked — is **held**, not in flight, because nothing will move it.
Demanding that every such element be handed over first would be a promise no run
could keep: a source waiting for room in a full buffer is waiting for the very
segment a pause has parked.

**Stopping always wins over pausing.** A shutdown, a cancellation, a disposal or
a failure during a pause ends the run; the parked segments observe it at their
park points, and a pause can never delay any of them. A paused run's controls
keep working, and `GetValueAsync` simply keeps waiting, because a paused run has
not ended and has no result yet.

Pausing is also the machinery a durable run's [checkpoints](durability.md) use to
reach a quiet moment, which is why a checkpoint's cost is measured in the same
units.

## On a cluster

The same vocabulary, with the differences the network forces:

```csharp
await using OrleansRunHandle run = await cluster.MaterializeAsync(pipeline, cancellationToken);

RunEnding ending = await run.WatchTermination;
long tally = await run.GetValueAsync(accepted, cancellationToken);
RunSnapshot snapshot = await run.SnapshotAsync(cancellationToken);
```

> From the `cluster` scenario,
> [`samples/Orleans.Dataflow.Samples/CSharp/Cluster.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/Cluster.cs).

Three differences, all of them honest reporting rather than changed meaning:

- **`SnapshotAsync` is a call, not a property read.** The counters are on the
  silo.
- **A remote failure arrives as a type name and a message.** Your exception type
  does not survive the hop; `Completion` faults with an exception carrying that
  pair, and `WatchTermination` resolves with the same pair as a value.
- **A run whose host was recycled has no ending at all.** An ordinary run that
  died mid-flight never reached a terminal state and never will, so the watch
  **faults** rather than resolving — there is nothing to report. That is the
  difference between "it ended badly" and "it is gone", and [the cluster
  model](cluster-model.md) is where that distinction is explained.

## Where to go next

- [Failure and supervision](failure-and-supervision.md) — what you can declare in
  advance so a throw is not the end.
- [Durability](durability.md) — how a run survives the death of its process.
- [Branching](branching.md) — several sinks, several slots, one run.
- [Run handles](../reference/run-handles.md) — every member of both handles.
- [Testing and observability](../guides/testing-and-observability.md) — driving a
  run deterministically, and watching one in production.
