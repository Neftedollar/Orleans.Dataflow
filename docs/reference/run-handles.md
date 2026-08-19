# Run handles

A [run handle](glossary.md#run-handle) is the control surface of one run: how it
ended, what it produced, and how to stop it. There are two, and they are
deliberately not polymorphic — `RunHandle` for a run in your process,
`OrleansRunHandle` for a run in a cluster.

```csharp
await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(graph);
await using OrleansRunHandle clusterRun = await orleansHost.MaterializeAsync(pipeline);
```

A handle is the *run*, not the graph. Materializing one graph twice gives you two
handles over two independent runs, and a handle answers only for its own.

Every member of either handle is safe to call from any thread, at any point in
the run's life, concurrently with any other member. Two callers awaiting one
result observe one outcome.

**Examples on this page** were compiled against the library in a scratch project
written for this page; the cluster snippet is the sequence
`samples/Orleans.Dataflow.Samples/CSharp/Cluster.cs` performs, compiled against
`OrleansDataflowHost` here.

---

## The two handles at a glance

| | `RunHandle` | `OrleansRunHandle` |
|---|---|---|
| Namespace | `Orleans.Dataflow` | `Orleans.Dataflow.Hosting` |
| Produced by | `LocalDataflowHost.MaterializeAsync` and its durable forms | `OrleansDataflowHost.MaterializeAsync` and its durable forms |
| Takes a | `RunnableGraph` | `PipelineDefinition` |
| `Completion` | ✅ faults with the very exception a stage threw | ✅ faults with `PipelineRunFailedException` carrying the type name and message |
| `WatchTermination` | ✅ | ✅ |
| `GetValueAsync(slot, ct)` | ✅ | ✅ |
| `ShutdownAsync()` | ✅ returns when the run has stopped | ✅ returns when the request has been delivered |
| `DisposeAsync()` | ✅ cancels and waits | ✅ cancels and waits |
| Reading counters | `Snapshot()`, synchronous | `SnapshotAsync(ct)`, one grain call |
| `PauseAsync` / `ResumeAsync` / `IsPaused` | ✅ | ❌ — local only |
| `RunId` | ❌ — the local host names runs itself | ✅ |
| `Epoch` | ❌ | ✅ |
| `Ticket` | ❌ | ✅ |

The shape of that table is a decision, not an omission. The two handles share
verbs where the semantics are the same and diverge where the capabilities do. A
shared base type would either flatten them to the smallest set — losing pause
and the synchronous snapshot — or throw `NotSupportedException` from the rest,
and both are worse than two honest types.

---

## `RunHandle` — a run in this process

Nine members.

| Member | What it does |
|---|---|
| `Task Completion` | The run's outcome as a task. It transitions exactly once: to completed when the source ended or a shutdown was asked for, to faulted with the exception a stage or the source threw, to cancelled when the run was cancelled. The exception is *unwrapped*: awaiting rethrows that very instance. The run's resources are released and its result slots settled before this transitions. |
| `Task<RunEnding> WatchTermination` | The run's outcome as a *value*. Resolves with `RunEnding.Completed`, resolves with a `Failed` ending carrying the failure's type name and message, or cancels — because a cancelled run has no [ending](glossary.md#ending). It settles immediately before `Completion` does, so a caller that has awaited `Completion` reads a settled ending here. Reading this property starts nothing. |
| `RunSnapshot Snapshot()` | One reading of the run's observable state. Callable at any point, from any thread, and never throwing; a run that has ended reports its final counters forever. |
| `Task<TResult> GetValueAsync<TResult>(slot, ct = default)` | Resolves one result the graph declares. Callable before, during, and after the run; asking twice gives the same answer twice. |
| `ValueTask ShutdownAsync()` | Stops the run gracefully and waits for it to stop. The run stops pulling, the element in flight is finished, an aggregate resolves its slot with what it accumulated, and `Completion` reports success. |
| `ValueTask DisposeAsync()` | Cancels the run and waits for it to stop. Never throws — not for the cancellation it caused, not for a failure the run had already suffered. |
| `bool IsPaused` | Whether the run is currently being held at its park points. |
| `Task PauseAsync(ct = default)` | Asks the run to stop between elements and waits until it has. |
| `Task ResumeAsync()` | Releases a paused run and waits until it is moving again. |

### Shutdown, cancellation, and pause are three different things

- **Shutdown drains.** "Stop producing, and let what is already in flight
  finish." An aggregate resolves its slot with the state it has accumulated and
  the completion reports success.
- **Cancellation abandons.** Nothing is drained, slots do not resolve, and the
  completion reports cancellation. It is spelled by the token you passed to
  `MaterializeAsync`, or by `DisposeAsync`.
- **Pause is neither.** A paused run has no outcome, no resolved result, and
  nothing to release. Both stops win over a pause: a run asked to shut down or
  cancelled while paused observes that at its park points and ends.

### What a pause guarantees

`PauseAsync` completes once every segment is at its next safe point and no
asynchronous callback is still running: **no author code of the run is executing,
and nothing will move an element until it is resumed.** An element that was
already produced and is waiting — in a buffer, in an asynchronous stage's window,
or at a sink nobody has asked for it — is *held* rather than in flight. Demanding
that every such element be handed over first would be a promise no run could
keep: a source waiting for room in a full buffer is waiting for the very segment
a pause has parked.

The token cancels the *wait* and not the request. A caller who stops waiting has
still asked for a pause; resuming is what withdraws it. `ResumeAsync` continues
every segment from exactly where it parked: a pause loses no element and repeats
none.

`IsPaused` is observational and best-effort by construction — it answers for a
moment that may already have passed. Nothing may be built on it that a race could
break. The way to know a pause has taken effect is to await `PauseAsync`, which
is a fact rather than a reading.

A pause holds elements, not the clock. A `Timeout` whose declared gap elapses
during a pause fails when its timer fires.

```csharp
await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(graph);

RunSnapshot snapshot = run.Snapshot();
Console.WriteLine($"{snapshot.Status}, dropped {snapshot.DroppedElements}, checkpoints {snapshot.Checkpoints}");

await run.PauseAsync();
Console.WriteLine(run.IsPaused);
await run.ResumeAsync();

await run.ShutdownAsync();

RunEnding ending = await run.WatchTermination;
Console.WriteLine(ending.Kind is RunEndingKind.Failed ? ending.FailureType : "completed");

Console.WriteLine(await run.GetValueAsync(seen));
await run.Completion;
```

```fsharp
let host = LocalDataflowHost()
use! run = host.MaterializeAsync graph

let snapshot = run.Snapshot()
printfn "%A dropped %d" snapshot.Status snapshot.DroppedElements

do! run.PauseAsync()
do! run.ResumeAsync()
do! run.ShutdownAsync()

let! ending = run.WatchTermination
let! count = run |> Run.value seen CancellationToken.None

do! run.Completion
```

`RunHandle` is a C# type that F# uses directly; the one F# spelling of its own is
`Run.value slot cancellationToken run`, which is `GetValueAsync` with the
arguments in pipeline order. Everything else is a method call. The handle is
`IAsyncDisposable`, so `use!` is F#'s `await using`: it binds the handle and
disposes it at the end of the scope, on the exception path as well as the
ordinary one.

---

## `OrleansRunHandle` — a run in a cluster

Nine members. The same vocabulary on purpose: a run completes, a run can be shut
down gracefully or cancelled, results are read by slot. What the network changes
is not the meaning of any of those but how faithfully they can be reported, and
both losses are stated rather than papered over.

| Member | What it does |
|---|---|
| `PipelineRunTicket Ticket` | What the coordinator issued when the run started. It does not move — see the table below. |
| `string RunId` | The identity of the run, which is `Ticket.RunId`. For a durable run this is the name *you* chose. |
| `long Epoch` | The ownership epoch every control call this handle makes carries. |
| `Task Completion` | The run's outcome. Faults with `PipelineRunFailedException` describing what the run threw, and with `PipelineRunLostException` when the activation hosting it was recycled with nothing to continue from. Polling starts when this property is first read — a run nobody is watching still runs. |
| `Task<RunEnding> WatchTermination` | The run's ending as a value, with the same rules as the local one. Faults with `PipelineRunLostException` when no ending will ever come, and with `PipelineFencingException` when an ordinary run's identity turns out to be somebody else's claim. Shares one poll loop with `Completion`. |
| `Task<RunSnapshot> SnapshotAsync(ct = default)` | One reading, one grain call. It neither starts nor joins the poll loop, so a monitor sampling on its own schedule costs exactly the calls it makes. |
| `Task<TResult> GetValueAsync<TResult>(slot, ct = default)` | Resolves one result the pipeline declares. The slot is validated locally before a call is made, and again by the run grain against the document it is actually running. |
| `ValueTask ShutdownAsync()` | Asks the run to stop gracefully. It returns when the *request has been delivered*; that the drain has finished is what `Completion` reports. Awaiting the drain inside a grain call would park an activation for as long as the graph takes. |
| `ValueTask DisposeAsync()` | Cancels the run and waits for it to reach a terminal state — not merely for the request to be sent, because that is what `await using` means to the caller and what the local handle has always done. Never throws. |

### `PipelineRunTicket`

Five members, all `string` but the epoch, and all recorded when the run started.

| Member | What it is |
|---|---|
| `RunId` | the run's identity |
| `GraphId` | the pipeline's identity |
| `Epoch` | the ownership epoch the coordinator issued for *this attempt* |
| `GraphFingerprint` | the fingerprint of the document the silo actually ran |
| `CatalogFingerprint` | the fingerprint of the stage catalog it validated against |

For a durable run whose hosting silo has since died, `Ticket.Epoch` is the one
*that* attempt held; what the handle is currently claiming under is
[`Epoch`](#the-epoch-and-why-a-durable-handle-follows-it). The two fingerprints
are what a caller compares when it wants to know that the cluster ran the
document it thought it sent, and against the vocabulary it thought was deployed.

### What the network costs you, precisely

- **A failure arrives as text.** The local runtime rethrows the very exception
  instance the author's code threw. Across a hop that is not possible in general:
  carrying the object would require every failure type in every pipeline to be
  Orleans-serializable, so a run whose stage threw an unprepared exception would
  fail to report that it had failed. The type name and the message travel
  instead, on `PipelineRunFailedException`. What is lost is the stack, the
  instance, and catching by the original type.
- **Completion is observed within one poll interval**, not at the moment it
  happens. The interval is
  [`OrleansDataflowClientOptions.PollInterval`](options.md#orleansdataflowclientoptions).
  A client watching many runs makes one call per run per interval.
- **A result must survive Orleans serialization.** The result type needs
  `[GenerateSerializer]` with `[Id]` on every member, or a registered serializer,
  and a type that does not fails when a result is first sent rather than when the
  pipeline was written. It also meets the silo's own
  [result-size cap](errors.md#resulttoolargeexception).

### Why there is no pause here

The engine a grain hosts *does* pause — checkpoint capture uses that very
machinery — so the gap is not imposed by the network. What is missing is the
design a remote pause owes: an epoch-fenced pause and resume protocol, a decided
answer to what a pause means across an activation death, and an `IsPaused`
reading that is honest about polling. Shipping a lossy version was rejected in
favour of not shipping one.

### The epoch, and why a durable handle follows it

Ownership of a run is claimed once, by the epoch the coordinator assigned when it
started the run, and every later control call restates that claim. A call
carrying any other epoch is refused with
[`PipelineFencingException`](errors.md#pipelinefencingexception), loudly, because
a stale owner silently succeeding is exactly the split brain the epoch exists to
prevent.

**A durable handle follows the run rather than the attempt.** A resume is the
same run continuing and claims a fresh epoch, so a handle holding the previous
number is out of date rather than wrong: it learns the current one from the
fencing refusal that names it and carries on. That is safe precisely because a
durable run is *named* — the identity the handle addresses is the author's own,
one run answers to it, and following its ownership forward cannot reach anybody
else's work. **An ordinary handle never does this and must not**: its run has no
later attempt, so a fencing refusal there means somebody else's claim, and
adopting it would be taking over a run this handle never started.

**A durable run that has written a checkpoint never reports itself lost.** The
poll is what continues it: addressing the run activates its grain, the activation
finds the stored position, claims a fresh epoch, and is executing by the time the
poll is answered. A durable run that died before its first capture is a different
case and reports the loss like any other, because there is no position to
continue from.

```csharp
await using OrleansRunHandle run = await host.MaterializeAsync(pipeline);

Console.WriteLine($"{run.RunId} at epoch {run.Epoch}");

RunSnapshot snapshot = await run.SnapshotAsync();
Console.WriteLine(snapshot.Status);

await run.ShutdownAsync();
await run.Completion;
```

---

## What disposing does

Both handles treat disposal as the abrupt stop. `DisposeAsync` cancels the run
exactly as the materialization token would, so `Completion` and every result slot
end cancelled unless the run had already reached a terminal state of its own.
Both wait for the run to stop before returning, and **neither ever throws** — not
for the cancellation it caused itself, and not for a failure the run had already
suffered. A teardown that replaced the caller's own exception with the run's
would hide the thing worth reading; how the run ended stays on `Completion`.

Disposing twice, or disposing a run that already ended, waits for the same
outcome again.

The cluster handle adds two cases where there is nothing to wait for and it does
not wait: an attempt whose activation was recycled, and an identity some other
claim owns. Polling either would be asking about work that is not this handle's —
and for a durable run, addressing it is what brings it back.

If you want a run to *finish* rather than to stop, call `ShutdownAsync` and await
`Completion` before the handle goes out of scope.

---

## `RunSnapshot`

One reading of a run's observable state. Six members, and it is a record, so two
readings with the same numbers are equal.

| Member | Type | What it counts |
|---|---|---|
| `Status` | `RunSnapshotStatus` | `Running`, `Completed`, `Failed`, or `Canceled` |
| `DroppedElements` | `long` | elements the run's buffers have discarded, across every boundary |
| `SupervisedFailures` | `long` | failures the run's supervision scopes have contained |
| `PoisonElements` | `long` | elements that used every attempt they were given |
| `Checkpoints` | `long` | checkpoints written; zero for a run with no declared checkpoint timing |
| `TotalCheckpointHold` | `TimeSpan` | the sum of every hold a checkpoint took, measured on the run's clock |

**A snapshot is a reading and not a consistent cut.** The counters are read one
after another while the run may be moving, so an element supervised between two
of the reads lands in one counter and not yet in another. Each individual counter
is exact.

The counters on a cluster snapshot describe *the attempt that answered*. A
durable run's ending observed while its activation still lived reports that
attempt's final counters; the same ending re-read after the activation is gone
comes from the coordinator's register, which records outcomes and not
diagnostics, so the counters there read zero. The continuous record is the
metrics pipeline's — see [monitoring](../operations/monitoring.md).

## `RunEnding`

The run's outcome as a value rather than as a task outcome.

| Member | Type | What it is |
|---|---|---|
| `Kind` | `RunEndingKind` | `Completed` or `Failed` |
| `FailureType` | `string?` | the CLR type name of what was thrown, or `null` |
| `FailureMessage` | `string?` | its message, or `null` |
| `RunEnding.Completed` | static | the completed ending |
| `RunEnding.Failed(failureType, failureMessage)` | static | a failed ending |

**There are only two endings.** A cancelled run has no ending, because cancelling
abandons a run rather than finishing it — which is why `WatchTermination` cancels
for a cancelled run rather than resolving. The watch therefore never disagrees
with `Completion`: both resolve, both report the same failure, or both cancel.

---

## Result slots and control slots

A `ResultSlot<TResult>` is a named, typed place where a value appears. You get
one while authoring and read it from the handle.

| Member | What it is |
|---|---|
| `Id` | the slot's identifier, as it appears in the document |
| `Graph` | the fingerprint of the document that declared it |
| `IsDefault` | whether this is the default value, which names no result |
| `Equals(other)`, `operator ==`, `operator !=` | value equality; a slot is a struct |

A graph declares two kinds of slot, and they resolve at different moments.

**A result** — a fold's state, a first or last element, a collected list — exists
only once the stream has ended, so its task completes when the run does and
carries the run's outcome: it faults when the run fails and cancels when the run
cancels.

**A control** — an ingress queue, a valve — exists as soon as the run does,
because producers push into a run that is already running. Its task is already
complete when the handle is handed over, and how the run ends never changes it. A
run that fails immediately still resolves its controls, and the queue behind one
answers every later offer with the refusal that says the run has ended. Controls
are declared on the stage that produces them and recovered by name with
`RunnableGraph.Control<TControl>(name)`.

Two control types ship:

| Type | Members | What it does |
|---|---|---|
| `IIngressQueue<T>` | `OfferAsync(element, ct)`, `Complete()`, `Fail(exception)` | The producer side of `Source.Queue`. An offer answers `QueueOfferOutcome.Accepted`, `Dropped`, `Closed`, or `Failed`; acceptance is admission into the queue and never downstream completion. |
| `IValve` | `Open()`, `Close()`, `IsOpen` | The gate `Valve` places. Closing holds the stream where it stands; opening releases it. |

```csharp
RunnableGraph graph = Source.Queue<int>(new BufferOptions { Capacity = 8 }, "ingress")
    .Valve("gate")
    .To(s => s.Count(), "seen", out ResultSlot<long> seen);

ResultSlot<IIngressQueue<int>> ingress = graph.Control<IIngressQueue<int>>("ingress");
ResultSlot<IValve> gate = graph.Control<IValve>("gate");

await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(graph);

IIngressQueue<int> queue = await run.GetValueAsync(ingress);
IValve valve = await run.GetValueAsync(gate);

QueueOfferOutcome outcome = await queue.OfferAsync(1);
valve.Close();
valve.Open();
queue.Complete();

Console.WriteLine(await run.GetValueAsync(seen));
await run.Completion;
```

### Which slot resolves against which run

A slot is accepted only when it was declared by the thing this is a run of, and
the two worlds are checked separately:

- **A `RunnableGraph`'s slot binds to the built instance.** Two lambda graphs of
  one shape share a fingerprint whatever their delegates compute, because a
  document records no delegate — so the fingerprint is checked first and the
  instance identity after it.
- **A `PipelineDefinition`'s slot binds by fingerprint and lineage alone**, with
  no instance identity, because registered behavior makes content identity
  meaningful. Recover one with `PipelineDefinition.ResultSlot<TResult>(name,
  contract)`.

Presenting the wrong one throws `ArgumentException`, and the message says which
of the two identities disagreed.

---

## Related

- [Runs and results](../concepts/runs-and-results.md) — why the vocabulary is
  shaped this way.
- [Errors](errors.md) — every exception either handle can produce.
- [Hosting](hosting.md) — the calls that hand you a handle.
- [Monitoring](../operations/monitoring.md) — what to do with a snapshot in
  production.
