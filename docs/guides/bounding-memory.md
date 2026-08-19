# Bounding memory

**The problem.** Your source produces faster than your sink consumes, and you
need to know exactly what the pipeline does with the difference — how much it
holds, and which elements it loses if it loses any.

The short answer is that by default it holds *one element* and loses nothing,
because nothing in a pipeline produces until something downstream asks. You only
need the rest of this page when you deliberately relax that.

## The whole program

This runs the same shape six times: nine readings, a sink that stops dead on the
first one it is given, and a source that runs ahead as far as it is allowed. The
only thing that changes is the [buffer](../reference/glossary.md#buffer) between
them.

```csharp
using Orleans.Dataflow;

const int Capacity = 3;

int[] readings = [1, 2, 3, 4, 5, 6, 7, 8, 9];
LocalDataflowHost host = new();

// ---- 1. no buffer: strict pull, and the source barely moves ---------------

{
    Gate sink = new();
    PacedFeed feed = new(readings, sink);

    RunnableGraph graph = Source.From(feed.Elements)
        .To(s => s.ForEach(_ => sink.Wait()));

    await using RunHandle run = await host.MaterializeAsync(graph);

    await sink.Reached;

    Console.WriteLine($"no buffer     source asked for {feed.Pulls} of {readings.Length} while the sink was parked");

    sink.Open();

    await run.Completion;
}

// ---- 2. a buffer, default policy: nothing is lost, the source stalls ------

{
    Gate sink = new();
    PacedFeed feed = new(readings, sink) { AnnounceAt = Capacity + 2 };
    List<int> seen = [];

    RunnableGraph graph = Source.From(feed.Elements)
        .Buffer(new BufferOptions { Capacity = Capacity })
        .To(s => s.ForEach(reading => { seen.Add(reading); sink.Wait(); }));

    await using RunHandle run = await host.MaterializeAsync(graph);

    await feed.Announced;

    Console.WriteLine($"Backpressure  source asked for {feed.Pulls} of {readings.Length} while the sink was parked");

    sink.Open();

    await run.Completion;

    Console.WriteLine($"Backpressure  kept {string.Join(' ', seen)} — dropped {run.Snapshot().DroppedElements}");
}

// ---- 3. the three policies that drop -------------------------------------

foreach (OverflowPolicy policy in
    (OverflowPolicy[])[OverflowPolicy.DropOldest, OverflowPolicy.DropNewest, OverflowPolicy.DropBuffer])
{
    Gate sink = new();
    PacedFeed feed = new(readings, sink);
    List<int> seen = [];

    RunnableGraph graph = Source.From(feed.Elements)
        .Buffer(new BufferOptions { Capacity = Capacity, OverflowPolicy = policy })
        .To(s => s.ForEach(reading => { seen.Add(reading); sink.Wait(); }));

    await using RunHandle run = await host.MaterializeAsync(graph);

    await feed.Finished;

    sink.Open();

    await run.Completion;

    Console.WriteLine($"{policy,-13} kept {string.Join(' ', seen)} — dropped {run.Snapshot().DroppedElements}");
}

// ---- 4. the policy that refuses ------------------------------------------

{
    Gate sink = new();
    PacedFeed feed = new(readings, sink);
    List<int> seen = [];

    RunnableGraph graph = Source.From(feed.Elements)
        .Buffer(new BufferOptions { Capacity = Capacity, OverflowPolicy = OverflowPolicy.Fail })
        .To(s => s.ForEach(reading => { seen.Add(reading); sink.Wait(); }));

    await using RunHandle run = await host.MaterializeAsync(graph);

    await feed.Finished;

    sink.Open();

    try
    {
        await run.Completion;
    }
    catch (BufferOverflowException overflow)
    {
        Console.WriteLine($"Fail          kept {string.Join(' ', seen)} — dropped {run.Snapshot().DroppedElements}");
        Console.WriteLine($"Fail          {overflow.Message}");
    }
}

/// <summary>A place the sink stops until the program lets it through.</summary>
internal sealed class Gate
{
    private readonly TaskCompletionSource _opened = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Completes the first time anything waits here.</summary>
    public Task Reached => _reached.Task;

    public void Wait()
    {
        _reached.TrySetResult();
        _opened.Task.GetAwaiter().GetResult();
    }

    public void Open() => _opened.TrySetResult();
}

/// <summary>A source that hands over one element, waits for the sink, then runs ahead of it.</summary>
internal sealed class PacedFeed(IReadOnlyList<int> elements, Gate sink)
{
    private readonly TaskCompletionSource _announced = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _finished = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _pulls;

    /// <summary>How many elements to hand over before announcing.</summary>
    public int AnnounceAt { get; init; } = int.MaxValue;

    /// <summary>How many elements the run has asked for so far.</summary>
    public int Pulls => Volatile.Read(ref _pulls);

    /// <summary>Completes once <see cref="AnnounceAt"/> elements have been asked for.</summary>
    public Task Announced => _announced.Task;

    /// <summary>Completes when the run stops reading, whether it ran out or gave up.</summary>
    public Task Finished => _finished.Task;

    public IEnumerable<int> Elements
    {
        get
        {
            try
            {
                foreach (int element in elements)
                {
                    if (Interlocked.Increment(ref _pulls) >= AnnounceAt)
                    {
                        _announced.TrySetResult();
                    }

                    yield return element;

                    // The sink must provably hold the first element before the source runs ahead,
                    // or the kept sets below would be a race rather than a policy.
                    if (Pulls == 1)
                    {
                        sink.Reached.GetAwaiter().GetResult();
                    }
                }
            }
            finally
            {
                _finished.TrySetResult();
            }
        }
    }
}
```

```console
dotnet run
```

```
no buffer     source asked for 1 of 9 while the sink was parked
Backpressure  source asked for 5 of 9 while the sink was parked
Backpressure  kept 1 2 3 4 5 6 7 8 9 — dropped 0
DropOldest    kept 1 7 8 9 — dropped 5
DropNewest    kept 1 2 3 4 — dropped 5
DropBuffer    kept 1 8 9 — dropped 6
Fail          kept 1 — dropped 0
Fail          A buffer of capacity 3 was full when an element was offered to it, and its overflow policy is 'Fail'. Raise the capacity, slow the source, or choose a policy that drops.
```

## The default: nothing grows, because nothing produces until asked

Read the first line again. With no buffer, the source had been asked for **one**
element while the sink stood still holding it. Not nine, not five — one.

That is [strict pull](../reference/glossary.md#pull). The
[terminal](../reference/glossary.md#terminal) asks the stage before it, which
asks the stage before that, back to the source, and the source produces one
element in answer to one request. A slow sink slows the source automatically,
and that is all [backpressure](../reference/glossary.md#backpressure) is: not a
mechanism you switch on, but the consequence of a pipeline never producing
anything nobody asked for.

So the memory a pipeline holds is not a function of how long the stream is. Ten
readings or ten million, this pipeline holds one at a time. **You do not need a
buffer to bound memory. You need one to allow memory.**

## The case for a buffer

Strict pull couples the two ends: the source cannot get on with producing while
the sink is working, because there is only one element in flight and the sink has
it. If producing and consuming are both slow and independent — reading a file
while writing to a database — that halves your throughput for no reason.

A buffer is a [boundary](../reference/glossary.md#boundary): a declared place
where more than one element may be in flight. The second line of output shows
what that bought: the source got five elements ahead instead of one. Three in
the buffer, one in the sink's hands, one the source is holding out and cannot
hand over. That fifth is where the source stopped — because the boundary is
declared, and a declared bound is a bound.

There are exactly three kinds of boundary in the library and every one of them is
something you asked for in the graph: a buffer, an asynchronous stage with its
declared [parallelism](../reference/glossary.md#parallelism), and a
[junction](../reference/glossary.md#junction) holding an element until every leg
has taken it. Nothing else accumulates. That is why the memory a run costs can
be read off its document rather than measured.

## The five overflow policies

A buffer's [overflow policy](../reference/glossary.md#overflow-policy) decides
what happens when it is full. Here is what each one did to the same nine
readings, with the same capacity of three, with the sink holding reading 1:

| Policy | Kept | Dropped | What it did |
|---|---|---|---|
| `Backpressure` *(default)* | `1 2 3 4 5 6 7 8 9` | 0 | Made the source wait. Nothing is lost, ever. |
| `DropOldest` | `1 7 8 9` | 5 | Threw away the oldest held element to make room, so the newest three survive. |
| `DropNewest` | `1 2 3 4` | 5 | Threw away each arriving element, so the *first* three survive. |
| `DropBuffer` | `1 8 9` | 6 | Threw away the entire buffer on each overflow, so almost nothing survives. |
| `Fail` | `1` | 0 | Faulted the run on the first overflow. |

Read those kept sets carefully, because the difference between `DropOldest` and
`DropNewest` is the difference between "I want the latest state" and "I want the
start of the incident".

- **`DropOldest`** is for streams where recency is the point: a gauge, a position,
  a heartbeat. An old reading is worthless once a new one exists.
- **`DropNewest`** is for streams where the first few tell the story: the opening
  of a burst of errors, a sample rather than a summary.
- **`DropBuffer`** is for streams that are only meaningful in complete batches —
  discard the half-batch rather than emit a torn one. Notice it dropped *six*
  where the others dropped five; throwing away three to make room for one costs
  more than throwing away one.
- **`Fail`** is for pipelines where falling behind is a bug and you want to hear
  about it now. Note what it kept: only reading 1. The elements the buffer was
  holding are behind the failure and are never delivered, so `Fail` is not "drop
  the overflow and carry on" — it is a stop.
- **`Backpressure`** is the default, deliberately. Four of the five policies lose
  elements, and losing elements should be something you said out loud.

## The trade-offs

**Capacity is required and has no unbounded spelling.** `BufferOptions.Capacity`
has no default, because an unbounded default is a memory leak that compiles.

**A buffer of one is still a boundary.** It is not an optimization the runtime
elides — it genuinely cuts the chain into two independently running halves. If
what you want is decoupling rather than slack, one is a legitimate capacity.

**A dropping buffer relaxes strict pull, permanently.** Once elements can be
discarded, the source is no longer slowed by the sink, so the source will run at
full speed for as long as it has anything to produce. That is usually the point,
but it means the drop count is a rate, not an accident.

**The drops are counted, and you can read them.** `run.Snapshot()` carries
`DroppedElements` — every number in the table above came from it — along with
supervised failures, poison elements and checkpoints. Publish it; a pipeline
dropping elements quietly is worse than one that never bought a buffer.

**The bound is in the document.** Two policies mean two graphs and two
[fingerprints](../reference/glossary.md#fingerprint), because a declared bound is
part of what the pipeline *is* rather than part of how it happened to run.

## The failure modes

| Symptom | Cause |
|---|---|
| `BufferOverflowException: A buffer of capacity 3 was full when an element was offered to it, and its overflow policy is 'Fail'.` | Exactly what it says. Raise the capacity, slow the source, or choose a policy that drops. |
| Memory grows without bound | Not a buffer. Look for a sink collecting into a list, an `Aggregate` accumulating a growing state, or a [group-by](../reference/glossary.md#group-by) with a large `MaxActiveKeys`. See [Windows and keys](windows-and-keys.md). |
| A cancelled run loses what the buffer held | Correct. [Cancellation](../reference/glossary.md#cancellation) abandons; [shutdown](../reference/glossary.md#shutdown) drains. If you want what is in flight delivered, call `ShutdownAsync` rather than cancelling the token. |
| Drops appear where you did not put a buffer | Some adapters and the ingress queue carry their own `BufferOptions`. Check every options record in the graph, not just the ones spelled `.Buffer(...)`. |

## Where to look next

- [Doing asynchronous work](async-work.md) — the other boundary you declare, and
  the other bound you get to choose.
- [Branching and joining](branching-and-joining.md) — the third boundary, and why
  a slow leg holds up its siblings.
- The repository's backpressure sample runs this in both languages:
  [`samples/Orleans.Dataflow.Samples/CSharp/Backpressure.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/Backpressure.cs).
  Run it with `dotnet run --project samples/Orleans.Dataflow.Samples -- --only backpressure`.
