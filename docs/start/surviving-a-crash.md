# Surviving a crash

After this page you have a run that keeps its place across the death of the
process running it: a first host dies part-way through, a second host is handed
the same document and the same name, and it carries on from where the first one
got to. You will also see the
[replay window](../reference/glossary.md#replay-window) — the elements delivered
twice — and know exactly why it is the size it is. About twenty minutes.

## Before you start

- [ ] You have finished [Your first pipeline](first-pipeline.md).
- [ ] A project referencing `Orleans.Dataflow`. No silo is needed: durability is
      a property of a run, not of a cluster, and it works in your own process
      too.

## Step 1 — the three things a durable run needs

An ordinary run is anonymous: the library makes up a name, and when the process
ends the run is gone. A [durable run](../reference/glossary.md#durable-run)
needs three things you supply.

```csharp
using Orleans.Dataflow;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;

ICheckpointStore store = new MemoryCheckpointStore();

DurableRunOptions Durability() => new()
{
    Store = store,
    RunId = RunId.Create("readings-of-the-day"),
    EveryElements = 3,
};
```

- **A [checkpoint store](../reference/glossary.md#checkpoint-store)** — somewhere
  the progress goes. You write this; Step 2 is the whole implementation.
- **A [run identity](../reference/glossary.md#run-identity)** — the name the two
  attempts share. This is what makes them one run rather than two. A local graph
  is anonymous, so without a name there would be nothing for a store to key a
  [checkpoint](../reference/glossary.md#checkpoint) by.
- **A cadence** — how often to write one down. `EveryElements = 3` here; there is
  also `Interval`, and you may declare both, in which case whichever comes first
  triggers a capture.

The options are a function rather than a variable because each materialization
gets its own — same store, same name, fresh options.

## Step 2 — write the store

This is the whole interface, and this is a complete implementation of it. Fifty
lines, and nothing in it is clever.

It is a type, so in a top-level-statements file it goes at the **bottom**, after
every statement on this page. Put it here, where you are reading it, and the
compiler says `error CS8803: Top-level statements must precede namespace and type
declarations.`

```csharp
internal sealed class MemoryCheckpointStore : ICheckpointStore
{
    private readonly Lock _padlock = new();
    private readonly Dictionary<(GraphId, RunId), StoredCheckpoint> _held = [];
    private long _revisions;

    public ValueTask<StoredCheckpoint?> ReadAsync(
        GraphId graph,
        RunId run,
        CancellationToken cancellationToken = default)
    {
        lock (_padlock)
        {
            return ValueTask.FromResult<StoredCheckpoint?>(
                _held.TryGetValue((graph, run), out StoredCheckpoint stored) ? stored : null);
        }
    }

    public ValueTask<string> WriteAsync(
        GraphId graph,
        RunId run,
        Orleans.Dataflow.Serialization.CanonicalJsonValue checkpoint,
        string? expectedETag,
        CancellationToken cancellationToken = default)
    {
        lock (_padlock)
        {
            Fence(graph, run, expectedETag);

            string next = (++_revisions).ToString(System.Globalization.CultureInfo.InvariantCulture);

            _held[(graph, run)] = new StoredCheckpoint { Document = checkpoint, ETag = next };

            return ValueTask.FromResult(next);
        }
    }

    public ValueTask ClearAsync(
        GraphId graph,
        RunId run,
        string? expectedETag,
        CancellationToken cancellationToken = default)
    {
        lock (_padlock)
        {
            Fence(graph, run, expectedETag);

            _held.Remove((graph, run));
        }

        return ValueTask.CompletedTask;
    }

    private void Fence(GraphId graph, RunId run, string? expectedETag)
    {
        string? held = _held.TryGetValue((graph, run), out StoredCheckpoint stored) ? stored.ETag : null;

        if (!string.Equals(held, expectedETag, StringComparison.Ordinal))
        {
            throw CheckpointConflictException.Superseded(graph, run, expectedETag, held);
        }
    }
}
```

Three methods, and three duties behind them.

**A write is atomic per document.** One checkpoint is one value replaced under
one lock, so no reader ever sees half of one. A store built out of several rows
would have to work for this; a document store gets it free.

**A write is a compare-and-swap on the [ETag](../reference/glossary.md#etag).**
This is the duty that carries the weight, and it is the reason `Fence` is called
*inside* the lock that then does the write — a check that is not atomic with the
thing it guards guards nothing. A writer presents the version it last saw; if
the stored version has moved on, the write is refused and that writer has lost
the run to somebody else. Get this wrong and it is not a performance problem:
two attempts of one run interleave their snapshots into a document describing
neither, and a resume restores a position no attempt was ever at.

**A clear is destructive.** Here that means the entry is gone. If your store
keeps versions, soft deletes, or backups, it has more to say about what a clear
means, and it should say it.

The counter, rather than a hash of the content, is deliberate: two identical
checkpoints written in sequence are still two writes, and a version a reader
cannot tell apart is not fencing anything.

What this store is *not* is durable — the process ending takes every checkpoint
with it, which is exactly why the demonstration below stands up a second *host*
rather than a second process. Point the same three methods at a document
database or a blob store with an ETag and everything above them is unchanged.

## Step 3 — build a graph that dies

```csharp
int[] readings = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

List<int> firstHost = [];
List<int> secondHost = [];

RunnableGraph Build(int dieAt, List<int> seen) =>
    Source.From(readings)
        .Select(reading => reading == dieAt
            ? throw new InvalidOperationException($"The host died on reading {reading}.")
            : reading)
        .To(s => s.ForEach(seen.Add));
```

One builder, two graphs. The `dieAt` parameter is a closure over a number, not
part of the document, so both graphs are the same pipeline — which is the
condition for continuing one as the other.

## Step 4 — run it until it dies

```csharp
await using (RunHandle attempt =
    await new LocalDataflowHost().MaterializeDurableAsync(Build(9, firstHost), Durability()))
{
    try
    {
        await attempt.Completion;
    }
    catch (InvalidOperationException death)
    {
        Console.WriteLine($"first host:  {death.Message}");
    }

    Console.WriteLine($"first host:  delivered {string.Join(' ', firstHost)}");
    Console.WriteLine($"first host:  {attempt.Snapshot().Checkpoints} checkpoints written");
}
```

`MaterializeDurableAsync` starts a run under the declared name and begins
writing its position to the store on the declared cadence.

## Step 5 — continue it on a second host

```csharp
await using (RunHandle continued =
    await new LocalDataflowHost().MaterializeFromCheckpointAsync(Build(-1, secondHost), Durability()))
{
    await continued.Completion;

    Console.WriteLine($"second host: delivered {string.Join(' ', secondHost)}");
    Console.WriteLine($"second host: {continued.Snapshot().Status}");
}

Console.WriteLine($"replayed:    {string.Join(' ', firstHost.Intersect(secondHost))}");
```

A second `LocalDataflowHost`, standing in for a second process. It is handed the
same document, the same run identity and the same store — and nothing else
passes between them.

```console
dotnet run
```

```
first host:  The host died on reading 9.
first host:  delivered 1 2 3 4 5 6 7 8
first host:  2 checkpoints written
second host: delivered 7 8 9 10 11 12
second host: Completed
replayed:    7 8
```

## Step 6 — read the replay window

Look at the two delivered lists. The first host got through eight readings. The
second host started at seven. Readings 7 and 8 were delivered **twice**.

That is the replay window, and it is arithmetic rather than bad luck:

- The cadence is every three elements, so checkpoints were written after 3 and
  after 6. That is the `2 checkpoints written`.
- The host died on reading 9, having already delivered 7 and 8 — which is *after*
  the last stored position of 6.
- A resume can only start from a position that was written down. The last one
  written down was 6, so the second host reopened there and re-delivered 7 and 8
  before reaching new work.

**This is at-least-once delivery, and it is the contract rather than a defect.**
The window is the elements between the last stored checkpoint and the moment the
process died. You size it by choosing the cadence: checkpoint after every element
and the window is at most one, at the cost of a store write per element and a
brief hold of the run each time. Checkpoint every thousand and the window is up
to a thousand. It is never zero — a checkpoint and the work after it cannot be
the same instant.

So the design question is never "how do I avoid replay". It is **"what does my
sink do when it sees an element twice?"** Two workable answers:

- Make the sink idempotent — key the write by something in the element, so
  writing it twice is writing it once.
- Pick a cadence whose window your downstream can absorb, and say so where
  somebody will read it.

## When it does not work

| What you see | What it means |
|---|---|
| `The checkpoint store holds nothing for the run '…' of the graph '…', so there is no run to continue.` | The store held nothing when you resumed. Either the first attempt died before its first capture, or the two attempts were given different `RunId`s, or a fresh store instance was handed to the second. The message says the rest: a run that crashed before its first capture is resumed by being started fresh. |
| `The checkpoint stored for the run '…' was taken of the graph sha256:… and this is a run of sha256:…` | The two attempts are not the same pipeline. A resume continues the very graph the checkpoint describes; there is no migration across a changed document. Compare `Fingerprint` on both graphs — an extra operator is enough to change it. |
| `CheckpointConflictException` on a write | Another attempt of the same run has written since yours last read. Yours has been superseded and should stop; that is the fence doing its job. |
| The run finishes with `0 checkpoints written` and the store is empty | No cadence. `DurableRunOptions` with neither `EveryElements` nor `Interval` runs perfectly well and captures nothing — so there is nothing to continue from. |

## What you learned

- A durable run needs a store you write, a name you choose, and a cadence you
  declare.
- The store contract is three methods, and the compare-and-swap on the ETag is
  the one that matters.
- Continuing a run means the same document, the same name and the same store —
  nothing else passes between the two processes.
- The replay window is the elements between the last checkpoint and the death,
  it is bounded by the cadence you chose, and it is never zero.
- At-least-once is the guarantee; idempotent sinks are how you live with it.

## Where to go now

You have finished the tutorial. From here:

- [Bounding memory](../guides/bounding-memory.md) — what happens when the source
  is faster than the sink.
- [Handling failure](../guides/handling-failure.md) — retries, fallbacks, and the
  counters that show what happened.
- [Durable runs](../guides/durable-runs.md) — naming a run and choosing a
  cadence, as a task rather than as a tutorial.
- [Checkpoint stores](../operations/checkpoint-stores.md) — the store contract in
  full, and how to implement one over a real database.

## Where to look next

The repository's durable sample runs this scenario in both languages and compares
them:
[`samples/Orleans.Dataflow.Samples/CSharp/Durable.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/Durable.cs),
[`samples/Orleans.Dataflow.Samples.FSharp/Durable.fs`](../../samples/Orleans.Dataflow.Samples.FSharp/Durable.fs),
and its own store in
[`samples/Orleans.Dataflow.Samples/SampleCheckpointStore.cs`](../../samples/Orleans.Dataflow.Samples/SampleCheckpointStore.cs).
Run it with `dotnet run --project samples/Orleans.Dataflow.Samples -- --only durable`.
