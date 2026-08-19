# Durable runs

You have a pipeline that takes long enough to matter, and you do not want a
process restart to send it back to the beginning.

Give the run a name, give it a cadence, and give it somewhere to write. It then
survives the death of the process running it: a later materialization under the
same name continues where the last stored position says it got to.

## The whole program

```csharp
using System.Collections.Concurrent;
using System.Globalization;
using Orleans.Dataflow;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

public sealed record Order(int Sequence, string Id);

// Where the checkpoints go. Fifty lines, and the whole of what a deployment implements.
public sealed class DictionaryCheckpointStore : ICheckpointStore
{
    private readonly ConcurrentDictionary<(GraphId Graph, RunId Run), StoredCheckpoint> _checkpoints = new();
    private long _revisions;

    public ValueTask<StoredCheckpoint?> ReadAsync(
        GraphId graph,
        RunId run,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return ValueTask.FromResult<StoredCheckpoint?>(
            _checkpoints.TryGetValue((graph, run), out StoredCheckpoint stored) ? stored : null);
    }

    public ValueTask<string> WriteAsync(
        GraphId graph,
        RunId run,
        CanonicalJsonValue checkpoint,
        string? expectedETag,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        string next = Interlocked.Increment(ref _revisions).ToString(CultureInfo.InvariantCulture);
        StoredCheckpoint written = new() { Document = checkpoint, ETag = next };

        lock (_checkpoints)
        {
            Fence(graph, run, expectedETag);

            _checkpoints[(graph, run)] = written;
        }

        return ValueTask.FromResult(next);
    }

    public ValueTask ClearAsync(
        GraphId graph,
        RunId run,
        string? expectedETag,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_checkpoints)
        {
            Fence(graph, run, expectedETag);

            _ = _checkpoints.TryRemove((graph, run), out _);
        }

        return ValueTask.CompletedTask;
    }

    // The duty that carries the weight: a writer whose ETag is not the stored one has lost the run.
    private void Fence(GraphId graph, RunId run, string? expectedETag)
    {
        string? held = _checkpoints.TryGetValue((graph, run), out StoredCheckpoint stored) ? stored.ETag : null;

        if (!string.Equals(held, expectedETag, StringComparison.Ordinal))
        {
            throw CheckpointConflictException.Superseded(graph, run, expectedETag, held);
        }
    }
}

public static class DurableGuide
{
    public static async Task RunAsync()
    {
        Order[] orders = [.. Enumerable.Range(0, 12).Select(n => new Order(n, $"order-{n:000}"))];

        ICheckpointStore store = new DictionaryCheckpointStore();

        DurableRunOptions Durability() => new()
        {
            Store = store,
            RunId = RunId.Create("orders-of-the-day"),
            EveryElements = 3,
        };

        List<string> first = [];
        List<string> second = [];

        RunnableGraph Build(int failAt, List<string> seen) =>
            Source.From(orders)
                .Select(order => order.Sequence == failAt
                    ? throw new InvalidOperationException($"the host died while handling {order.Id}")
                    : order)
                .To(s => s.ForEach(order => seen.Add(order.Id)));

        RunnableGraph crashing = Build(8, first);
        RunnableGraph continuing = Build(-1, second);

        RunSnapshot afterCrash;

        await using (RunHandle attempt = await new LocalDataflowHost()
            .MaterializeDurableAsync(crashing, Durability()))
        {
            try
            {
                await attempt.Completion;
            }
            catch (InvalidOperationException)
            {
                // The crash.
            }

            afterCrash = attempt.Snapshot();
        }

        // A second host, standing in for a second process: the same document, the same name, the same store.
        await using (RunHandle continued = await new LocalDataflowHost()
            .MaterializeFromCheckpointAsync(continuing, Durability()))
        {
            await continued.Completion;
        }

        Console.WriteLine($"same document          {crashing.Fingerprint == continuing.Fingerprint}");
        Console.WriteLine($"first attempt          {string.Join(' ', first)}");
        Console.WriteLine($"checkpoints written    {afterCrash.Checkpoints}");
        Console.WriteLine($"second attempt         {string.Join(' ', second)}");
        Console.WriteLine($"replayed               {string.Join(' ', first.Intersect(second, StringComparer.Ordinal))}");
    }
}
```

What it prints:

```text
same document          True
first attempt          order-000 order-001 order-002 order-003 order-004 order-005 order-006 order-007
checkpoints written    2
second attempt         order-006 order-007 order-008 order-009 order-010 order-011
replayed               order-006 order-007
```

Read the last line first. Two orders were delivered twice, and that is the
guarantee rather than a defect: delivery between stored positions is
[at-least-once](../reference/glossary.md#at-least-once), and the two orders are
exactly the ones the first attempt delivered after its last checkpoint.

The same scenario, authored twice and checked against itself, is in
[`samples/Orleans.Dataflow.Samples/CSharp/Durable.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/Durable.cs)
and its F# twin
[`Durable.fs`](../../samples/Orleans.Dataflow.Samples.FSharp/Durable.fs). The
store above is
[`SampleCheckpointStore.cs`](../../samples/Orleans.Dataflow.Samples/SampleCheckpointStore.cs)
with its documentation trimmed.

## Naming a run

An ordinary run is named by the host, freshly, once per materialization: two
`MaterializeAsync` calls are two runs that share nothing. A
[durable run](../reference/glossary.md#durable-run) is named by *you*, through
`DurableRunOptions.RunId` locally and `DurablePipelineOptions.RunId` on a silo,
and **the name is the unit of continuation**.

That is the whole of what makes a second materialization continue rather than
start a second run. On a silo, `MaterializeDurableAsync` under a name the cluster
already holds hands you a handle to the run that exists — or, if the silo hosting
it died, continues it from its last stored position. There is no second call to
learn, and no flag: continuing is what the name means.

Locally the two halves are spelled separately, because a local host has no
cluster to ask. `MaterializeDurableAsync` starts fresh and
`MaterializeFromCheckpointAsync` reads the store first and continues. Starting
fresh over a name that already holds a position is not silently destructive: the
fresh run believes the store holds nothing, presents no
[ETag](../reference/glossary.md#etag) at its first capture, and the store refuses
it with `CheckpointConflictException`.

Choose names you can enumerate and give back. A name per tenant per day is fine;
a name per request is a register that grows without bound, and a coordinator caps
it — see [retiring a run identity](../operations/runbooks.md#retiring-a-run-identity).

## Choosing a cadence

`Interval`, `EveryElements`, or both. A run that declares neither **never writes
to the store at all** — which is the honest reading of durable options with no
timing in them, and is asserted rather than assumed.

- `EveryElements` counts elements **admitted** — every element any source of the
  run hands to the graph, summed across sources. Not elements committed at a
  sink, which is a different number for every graph that filters or batches.
- `Interval` means "at most this long between two *timed* captures". A capture
  the element bound made due does not postpone the next timed one.

Taking a [checkpoint](../reference/glossary.md#checkpoint) **holds the run for
its duration**: pause, snapshot, write, release, with nothing overlapping. So the
cadence is a trade you make with numbers. The cost is visible in two places —
`RunSnapshot.TotalCheckpointHold` for one run, and the
`orleans.dataflow.checkpoint.hold.duration` histogram across all of them (see
[Monitoring](../operations/monitoring.md)).

## The arithmetic of the replay window

The [replay window](../reference/glossary.md#replay-window) is the elements
between the last stored position and the moment the process died — the ones a
resumed run delivers a second time.

With a count cadence of *N*, a capture is requested on the source's own thread
the instant the *N*th element since the last position is admitted, and that
source parks there. **With one source the stored position is therefore an exact
multiple of *N*, and the window is at most *N* − 1.**

Crashing the program above at each of its twelve elements in turn measures it:

```text
crashAt= 1  stored= 0  delivered= 0  window= 0
crashAt= 2  stored= 0  delivered= 1  window= 1
crashAt= 3  stored= 0  delivered= 2  window= 2
crashAt= 4  stored= 3  delivered= 3  window= 0
crashAt= 5  stored= 3  delivered= 4  window= 1
crashAt= 6  stored= 3  delivered= 5  window= 2
crashAt= 7  stored= 6  delivered= 6  window= 0
crashAt= 8  stored= 6  delivered= 7  window= 1
crashAt= 9  stored= 6  delivered= 8  window= 2
crashAt=10  stored= 9  delivered= 9  window= 0
crashAt=11  stored= 9  delivered=10  window= 1
crashAt=12  stored= 9  delivered=11  window= 2
```

The `crashAt=9` row is the program above: stored six, delivered eight, window
two — `order-006` and `order-007`.

**With more than one source the multiple is not exact**, and this is worth
knowing before you compute a budget from the cadence alone. `EveryElements`
counts elements admitted *summed across sources*, and each source parks at its
own park point, so a second source can admit one more before it notices. Three
runs of a two-source merge at a cadence of three store these positions:

```text
positions written = 4, 6, 9, 12, 15, 18
positions written = 3, 6, 9, 12, 16, 18
positions written = 3, 7, 10, 12, 15, 18
```

Four, seven, sixteen. So with *M* sources, size the window as *N* + *M* − 1
rather than *N* − 1, and treat the exact number as a bound rather than a
schedule.

Two things widen that number, and both are stated per adapter rather than
promised in general:

- **A source's own cursor granularity.** A stream source's window includes the
  element its cursor names, because a subscription opened at a sequence token
  receives that element again — so it is one element wider than an index cursor's.
- **A sink's mark lag.** The terminating grain call's mark counts replies that
  have been observed, which can lag the truth by up to its `maxInFlight`; at a
  bound of one it is exact.

Every adapter's row in the [adapter reference](../reference/adapters.md) states
its own window. Nothing narrows the window to zero, and nothing claims to.

**The one direction that is not a duplicate.** Where a graph holds elements
between a cursor and its sink at capture time — inside a declared buffer, inside
a junction — those elements have been counted by the cursor and have not been
committed. On resume they are **not** replayed; they are gone. The checkpoint
carries both numbers, so the gap is a measurement rather than a surprise, but it
is the reason a buffer between a durable source and a sink is a decision and not
a free optimisation.

## What resume actually does

A resume rebuilds the graph from the very factories a fresh run builds it from,
and then hands three things back:

| Restored | Reset |
|---|---|
| A source that declared a [cursor](../reference/glossary.md#cursor) reopens at its stored position. | A `Scan` outside a durable scope returns to its seed. |
| A stage inside a durable scope takes back the state it exported. | A grouping stage abandons the window it was filling. |
| A sink that declared a [mark](../reference/glossary.md#mark) takes back its count. | A `Distinct` forgets the keys it had seen. |

Everything not in the left column resets, and that is worth reading as a rule
rather than a list: **state survives a resume only where something declared that
it should**. If a running total matters across a restart, put it in a durable
scope; if a source has no cursor it resumes from now, whatever the checkpoint
says about the rest of the graph.

## A run whose document changed

One document per name. A name that holds a different document is refused —
`PipelineResumeRefusedException`, naming both
[fingerprints](../reference/glossary.md#fingerprint) — because a checkpoint of
another graph describes nodes that are not these nodes, and restoring a cursor
into it would be restoring a position into a source that never counted it.

Two [revisions](../reference/glossary.md#revision) of one pipeline can run side
by side quite happily under two names. What you cannot do is move one name onto a
new document and keep its position, and the refusal says so rather than guessing.
When you mean the other thing — this name now runs that document, from the
beginning — that is
[replacing a durable run](../operations/runbooks.md#replacing-a-durable-run), and
it clears the stored position on purpose.

## Making a sink idempotent

At-least-once is the floor. When repeating an effect is not acceptable, the sink
has to carry its own answer, and there are three shapes that work:

**Deduplicate on a key the element already has.** The cheapest option when
elements carry a natural identity: write with `INSERT … ON CONFLICT DO NOTHING`,
a conditional `PUT`, or an upsert keyed by the order id. The replay then writes
the same row twice and the second write is a no-op. Nothing in this library is
involved, which is the point.

**Make the effect itself repeatable.** Setting a value is idempotent; adding to
one is not. A sink that assigns `status = 'shipped'` may be replayed freely; one
that increments a counter may not. Where you control the schema this is usually
the cheapest change.

**Declare a commit mark and let the resume skip.** A sink written as a custom
stage can declare a `DataflowSinkMark`, which enters the checkpoint and is handed
back before the resumed run's first element — so the sink itself can skip the
work it knows it already committed. The rule the engine cannot check and your
sink must honour: **the mark advances after the effect, never before it.** A mark
that led its effect would turn a duplicate window into a loss window. A mark that
lags costs a wider replay and loses nothing, which is the direction to lean when
the two moments cannot be separated exactly. See
[Writing a custom stage](custom-stages.md#cursors-and-marks-for-durability).

What does not work is hoping the window is small. It is never zero, and a
deployment that has not decided which of the three shapes it is using has decided
on the first one by accident.

## On a silo

The shape is the same and three things differ.

Every silo that may host the run calls `UseCheckpointStore(...)`, over the same
store — a cluster whose silos disagree accepts a declaration on one host and
cannot honour it on another. The refusal a run then gets names exactly that.

`MaterializeDurableAsync` on `OrleansDataflowHost` is both halves: it declares
the run with the coordinator and starts it, and an activation that comes up after
a silo died takes the second half of that path on its own. So a resume needs no
protocol of its own and no call from you.

And a resumed activation lands wherever Orleans places it. The checkpoint travels
through the store rather than through the silo, so placement is a performance
choice and never a correctness one.

## Next

- [Surviving a crash](../start/surviving-a-crash.md) — the same idea as a tutorial, if this page was the wrong altitude.
- [Durability](../concepts/durability.md) — why it is shaped this way.
- [Checkpoint stores](../operations/checkpoint-stores.md) — the store contract in full, and how to implement one over a real document store.
- [Runbooks](../operations/runbooks.md) — replacing a run, retiring a name, recovering from a store outage.
