# Testing and observability

Two halves of one problem: seeing what a pipeline does. In a test you want to see
it *exactly*, which means no sleeping and no timing luck. In production you want
to see it *continuously*, which means instruments rather than assertions.

Everything on this page ships in the box —
`Orleans.Dataflow.Testing` for the first half, and one meter and one activity
source for the second.

---

# Testing

## Probes: the test cannot outrun the run

A probe is a demand-aware source or sink. An emit completes when the run has
**taken** the element, not when something accepted it into a buffer, so the test
and the run advance in lockstep and nothing needs a `Task.Delay`.

```csharp
RunnableGraph graph = TestSource.Probe<int>("emitted").To(TestSink.Probe<int>("received"));

await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(graph);

ISourceProbe<int> source = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"));
ISinkProbe<int> sink = await run.GetValueAsync(graph.Control<ISinkProbe<int>>("received"));

await source.EmitAsync(1);

// The element is at the sink and nobody has asked for it, so the run cannot take another.
Task second = source.EmitAsync(2).AsTask();

Console.WriteLine($"probes/second-emit-outstanding  {!second.IsCompleted}");
Console.WriteLine($"probes/pulls-after-one-emit     {source.PullsObserved}");
Console.WriteLine($"probes/received                 {await sink.ReceiveAsync()}");

await second;

Console.WriteLine($"probes/received                 {await sink.ReceiveAsync()}");

source.Complete();

await sink.ExpectCompletedAsync();
await run.Completion;

Console.WriteLine($"probes/pulls-total              {source.PullsObserved}");
```

```text
probes/second-emit-outstanding  True
probes/pulls-after-one-emit     1
probes/received                 1
probes/received                 2
probes/pulls-total              3
```

A probe is a **control**: declared on the graph under a name, built fresh per
materialization, resolved from the run handle with `graph.Control<T>("name")`.
Two runs of one graph never share one.

The assertion worth writing is the last line. **`PullsObserved <= emitted + 1`
holds for every graph and every buffer size**, because a runtime that prefetched
— that pulled a second element before it had done anything with the first —
would exceed it. Two elements emitted, three pulls: the run was always asking for
exactly one more than it had been given, which is a credit of one and not a
prefetch. What changes with the buffers an author declared is how many elements
the run accepts before it stops asking, and that is the other half of the same
measurement.

Three more things a probe gives you:

- **A sink probe *is* the demand.** The run delivers nothing to it that has not
  been asked for, so a test that receives nothing watches the graph fill exactly
  the capacity its author declared and then stop. That is how a bounded-memory
  claim becomes an assertion rather than a hope.
- **Nothing hangs on a run that ended.** Every wait is answered when the run
  ends, with a `ProbeTerminatedException` naming the outcome — because a test
  that hangs reports nothing at all.
- **`ExpectFailedAsync` returns the exception rather than throwing it**, so the
  test decides what to assert about it without going through a `catch` to get
  the value it asked for.

## Controllable time: testing a delay without sleeping

`TestClock` is a `TimeProvider`, and a host built over one measures every
duration in every run by it.

```csharp
TestClock clock = new();
LocalDataflowHost host = new(clock);

DateTimeOffset start = clock.GetUtcNow();
List<DateTimeOffset> observed = [];

RunnableGraph graph = Source.From([1, 2, 3])
    .Delay(TimeSpan.FromSeconds(1), new BufferOptions { Capacity = 4 })
    .To(s => s.ForEach(_ => observed.Add(clock.GetUtcNow())));

await using RunHandle run = await host.MaterializeAsync(graph);

// Wait for the run to arm its timers before advancing.
await clock.WaitForTimersAsync(3);

clock.Advance(TimeSpan.FromSeconds(1) - TimeSpan.FromTicks(1));

Console.WriteLine($"clock/nothing-one-tick-early    {observed.Count == 0}");

clock.Advance(TimeSpan.FromTicks(1));

await run.Completion;

Console.WriteLine($"clock/all-at-exactly-one-second {observed.TrueForAll(at => at == start + TimeSpan.FromSeconds(1))}");
```

```text
clock/nothing-one-tick-early    True
clock/all-at-exactly-one-second True
```

**`WaitForTimersAsync` before `Advance` is the whole idiom**, and it is the one
thing a virtual clock makes the test responsible for. Advancing before the run
has reached its wait arms that wait *after* the moment it was waiting for, and
the run then sits there until the test advances again — a flake that reads as a
hang. Waiting for the timers to exist first turns that into an ordinary
ordering.

The clock's resolution is one tick, which is what lets a test claim that a
boundary is *exactly* where it says it is: advance to one tick short of the
deadline, assert nothing happened, advance the last tick, assert it did.

`clock.PendingTimers` tells you how many armed timers a run is holding, if you
need to assert about the shape rather than the timing.

## Making a stage fail where you said

`TestFlow.FaultPoint<T>` is a pass-through that throws on the arrival you name.

```csharp
List<int> delivered = [];

RunnableGraph graph = Source.From([1, 2, 3])
    .Supervised(
        new SupervisionOptions { Form = SupervisionForm.Retry, MaxAttempts = 3 },
        TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2))
    .To(s => s.ForEach(delivered.Add));

await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(graph);

await run.Completion;

RunSnapshot snapshot = run.Snapshot();

Console.WriteLine($"fault/delivered                 {string.Join(' ', delivered)}");
Console.WriteLine($"fault/supervised-failures       {snapshot.SupervisedFailures}");
Console.WriteLine($"fault/poison-elements           {snapshot.PoisonElements}");
```

```text
fault/delivered                 1 2 3
fault/supervised-failures       1
fault/poison-elements           0
```

Three modes: `Never`, `Once`, `Always`. The default throws a
`FaultInjectedException` carrying the arrival number; an overload takes a factory
when you want your own exception type, and it travels unwrapped exactly as an
author's own would.

**A control cannot be declared inside a supervision scope.** A scope's stages are
not nodes of the document, so nothing could resolve a control declared on one —
and the authoring call refuses it by name rather than failing at run time. Inside
a scope, use the declared-arming spelling above; outside one, the
`FaultPoint<T>(controlName, mode, firstFailure)` overload gives you an
`IFaultPoint` to re-arm mid-run and to read `ElementsSeen` and `FaultsThrown`
from.

## Testing a durable sink without an adapter

`TestSink.Marking<T>` is a callback sink that declares a
[commit mark](../reference/glossary.md#mark) — the seam a durable sink uses,
available where no real adapter is.

```csharp
InMemoryCheckpointStore store = new();
List<int> committed = [];

RunnableGraph graph = Source.From([1, 2, 3, 4, 5, 6])
    .To(TestSink.Marking<int>("mark", committed.Add));

await using RunHandle run = await new LocalDataflowHost().MaterializeDurableAsync(
    graph,
    new DurableRunOptions { Store = store, RunId = RunId.Create("marking"), EveryElements = 3 });

IMarkingSink sink = await run.GetValueAsync(graph.Control<IMarkingSink>("mark"));

await run.Completion;

Console.WriteLine($"mark/committed                  {string.Join(' ', committed)}");
Console.WriteLine($"mark/mark                       {sink.Mark}");
Console.WriteLine($"mark/checkpoints                {run.Snapshot().Checkpoints}");
```

```text
mark/committed                  1 2 3 4 5 6
mark/mark                       6
mark/checkpoints                2
```

`InMemoryCheckpointStore` models the store contract exactly — atomic per
document, compare-and-swap on the ETag, destructive clear — which makes it the
right thing to test *against*. What it cannot hold for you is atomicity under a
process death, because it lives in the test process and cannot be torn by a silo
dying. That duty belongs to your real store, and
[Checkpoint stores](../operations/checkpoint-stores.md) is where it is stated.

## Testing a custom stage

Two things, in this order.

**Run the conformance kit.** `ProviderConformance` is nine structural checks over
your catalog and your factory. Drive it as a theory with
`ProviderConformance.Checks` as the data, so a failure reads as the sentence that
stopped being true. The full worked example is in
[Writing a custom stage](custom-stages.md#proving-it-with-the-conformance-kit).

**Then run the stage.** Register it on a `LocalDataflowHost` — the same
`AddCatalog` and `AddFactory` calls a silo makes — and author an ordinary graph
over it. No cluster is involved, so the test is fast and the failure messages are
your own. `SalesVocabulary` and `SalesStageFactory` below are the worked pair from
[Writing a custom stage](custom-stages.md) — a catalog that names three stages and
the factory that builds them; substitute your own:

```csharp
LocalDataflowHost host = new(builder => builder
    .AddCatalog(SalesVocabulary.Catalog())
    .AddFactory(SalesVocabulary.Provider, new SalesStageFactory()));
```

Reach for a cluster only for what a cluster decides: placement, failover, silos
that disagree about their catalogs, and the shipped Orleans adapters — all of
which need `Microsoft.Orleans.TestingHost` rather than this package.

## What none of this tests

Worth naming, because the tooling is good enough to be mistaken for complete.

- **Semantics of an adapter.** No test can check that an acknowledgement boundary
  is where its documentation says it is, that a cursor reopens on the right side
  of its position, or that a mark advances after its effect.
- **Atomicity under a process death.** See above.
- **What your store does when it is slow.** A store that answers instantly in a
  test says nothing about a store that answers in two seconds under load. Write a
  store double that refuses and one that hangs, and assert on both — the
  difference between them is the single most important operational fact about
  durability.

---

# Observability

## Turning it on

Two lines, and **the names are the contract** — a subscriber names the meter and
the source, never a type:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics.AddMeter("Orleans.Dataflow"))
    .WithTracing(tracing => tracing.AddSource("Orleans.Dataflow"));
```

A silo hosting runs emits engine metrics and run spans automatically; a client
emits the materialize span.

## What comes out

Collected from a real run — six elements through a buffer of two with
`DropOldest`, durable at a cadence of two:

```text
metric  orleans.dataflow.checkpoint.hold.duration 0 {dataflow.graph=<fingerprint>}
metric  orleans.dataflow.checkpoint.hold.duration 0 {dataflow.graph=<fingerprint>}
metric  orleans.dataflow.checkpoint.hold.duration 0.004 {dataflow.graph=<fingerprint>}
metric  orleans.dataflow.checkpoints.written 3 {dataflow.graph=<fingerprint>}
metric  orleans.dataflow.elements.dropped 1 {dataflow.graph=<fingerprint>}
metric  orleans.dataflow.elements.poison 0 {dataflow.graph=<fingerprint>}
metric  orleans.dataflow.failures.supervised 0 {dataflow.graph=<fingerprint>}
metric  orleans.dataflow.runs.ended 1 {dataflow.graph=<fingerprint>, dataflow.run.outcome=completed}
metric  orleans.dataflow.runs.started 1 {dataflow.graph=<fingerprint>, dataflow.run.resumed=False}
span    dataflow.run {dataflow.graph=<fingerprint>, dataflow.run.resumed=False, dataflow.run.outcome=completed}
```

Three instrument-per-capture samples in the histogram, one row per cumulative
counter, and one span covering the run's whole life. Every instrument carries
`dataflow.graph`. The full table of names, what each counts, and the cardinality
bound is in [Monitoring](../operations/monitoring.md).

Two facts about how it is emitted, because they change what you can conclude:

- **The cumulative counters are the runs' own counters, read rather than
  duplicated.** On each collection the library sums every live run's counters
  with the totals runs left behind when they settled. So they keep counting a
  graph's totals after its runs settle, and rates read across run boundaries.
- **Nothing is on the element hot path.** A stage pays nothing for metrics nobody
  is collecting, and the same nothing when they are. The only eager emissions are
  one event per run start, one per run end, and one histogram sample per
  checkpoint hold — all cold paths.

And one that changes what you can rely on: **telemetry never fails a run.** Every
emission swallows, because a listener that throws from a measurement callback is
a broken observer and a run that died of being observed would be a worse defect
than any lost sample.

## Reading one run

The run handle has three surfaces, and choosing between them is choosing what you
want a failure to do to *you*.

| Surface | What it gives you | Use it when |
|---|---|---|
| `Completion` | The outcome as a task outcome — awaiting it makes the run's failure your own. | Your code should stop when the run does. |
| `WatchTermination` | The [ending](../reference/glossary.md#ending) as a *value*: `Completed` or `Failed(type, message)`. | You are a monitor, a log, or a metric reacting to endings rather than inheriting them. |
| `Snapshot()` / `SnapshotAsync()` | Status plus five counters. | You are sampling a run that is still going. |

The snapshot's five counters are dropped elements, supervised failures, poison
elements, checkpoints written, and total checkpoint hold. **Not a consistent cut**
— each number is exact on its own, and they are read at slightly different
moments.

Over the wire the counters describe **the answering attempt**. A durable run's
ending re-read after its activation died comes from the coordinator's register,
which records outcomes and not diagnostics, so those counters read zero. History
belongs to the meter, which is the point of having one.

## Next

- [Monitoring](../operations/monitoring.md) — every instrument name, the cardinality bound, and what to alert on.
- [Handling failure](handling-failure.md) — the policies the supervised and poison counters are counting.
- [Writing a custom stage](custom-stages.md) — the conformance kit in full.
- [Run handles](../reference/run-handles.md) — every member of both handles.
