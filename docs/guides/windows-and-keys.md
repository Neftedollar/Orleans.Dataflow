# Windows and keys

**The problem.** You need elements handled in batches rather than one at a time —
by count, by elapsed time, or grouped by some key — and you need to know exactly
how much memory that costs before you deploy it.

The answer for every operator on this page is the same: a
[window](../reference/glossary.md#window) is a bound. What a grouping stage holds
is one window, and what a keyed stage holds is one substream per live key, up to
a number you declare. Nothing here can grow without you having written down how
far.

## The whole program

```csharp
using Orleans.Dataflow;

Reading[] readings =
[
    new("north", 21), new("south", 17), new("east", 24), new("north", 12),
    new("south", 26), new("east", 19), new("north", 18), new("south", 23),
    new("east", 15), new("north", 27),
];

LocalDataflowHost host = new();
CollectOptions upToThirtyTwo = new() { MaxElements = 32 };

// ---- 1. by count --------------------------------------------------------

RunnableGraph byCount = Source.From(readings)
    .Grouped(4)
    .To(Sink.Collect<IReadOnlyList<Reading>>(upToThirtyTwo), "windows", out ResultSlot<IReadOnlyList<IReadOnlyList<Reading>>> byCountSlot);

await using (RunHandle run = await host.MaterializeAsync(byCount))
{
    IReadOnlyList<IReadOnlyList<Reading>> windows = await run.GetValueAsync(byCountSlot);

    Console.WriteLine($"Grouped(4)              {windows.Count} windows, sizes {string.Join(' ', windows.Select(w => w.Count))}");

    await run.Completion;
}

// ---- 2. by count or by time, whichever comes first ---------------------

TimeSpan window = TimeSpan.FromMilliseconds(200);

RunnableGraph byTime = Source.FromAsyncEnumerable(Trickle(readings, after: 3, gap: TimeSpan.FromSeconds(2)))
    .GroupedWithin(4, window)
    .To(Sink.Collect<IReadOnlyList<Reading>>(upToThirtyTwo), "windows", out ResultSlot<IReadOnlyList<IReadOnlyList<Reading>>> byTimeSlot);

await using (RunHandle run = await host.MaterializeAsync(byTime))
{
    IReadOnlyList<IReadOnlyList<Reading>> windows = await run.GetValueAsync(byTimeSlot);

    Console.WriteLine($"GroupedWithin(4, 200ms) {windows.Count} windows, sizes {string.Join(' ', windows.Select(w => w.Count))}");

    await run.Completion;
}

// ---- 3. overlapping windows -------------------------------------------

RunnableGraph sliding = Source.From(readings)
    .Sliding(3, 2)
    .To(Sink.Collect<IReadOnlyList<Reading>>(upToThirtyTwo), "windows", out ResultSlot<IReadOnlyList<IReadOnlyList<Reading>>> slidingSlot);

await using (RunHandle run = await host.MaterializeAsync(sliding))
{
    IReadOnlyList<IReadOnlyList<Reading>> windows = await run.GetValueAsync(slidingSlot);

    Console.WriteLine($"Sliding(3, 2)           {windows.Count} windows, sizes {string.Join(' ', windows.Select(w => w.Count))}");

    await run.Completion;
}

// ---- 4. by key, with a bound on how many keys may be live at once -----

RunnableGraph bounded = Source.From(readings)
    .GroupBy(
        new GroupByOptions { MaxActiveKeys = 2 },
        reading => reading.Station,
        Flow.For<Reading>().Select(reading => $"{reading.Station}:{reading.Celsius}"))
    .To(Sink.Ignore<string>());

await using (RunHandle run = await host.MaterializeAsync(bounded))
{
    try
    {
        await run.Completion;

        Console.WriteLine("GroupBy(2 keys)         the bound was never reached");
    }
    catch (TrackedKeyOverflowException refusal)
    {
        Console.WriteLine($"GroupBy(2 keys)         {refusal.Message}");
    }
}

// ---- 5. the same graph, with the other answer to the bound ------------

RunnableGraph evicting = Source.From(readings)
    .GroupBy(
        new GroupByOptions { MaxActiveKeys = 2, OverflowPolicy = ActiveKeyOverflowPolicy.EvictIdle },
        reading => reading.Station,
        Flow.For<Reading>().Select(reading => $"{reading.Station}:{reading.Celsius}"))
    .To(Sink.Collect<string>(upToThirtyTwo), "seen", out ResultSlot<IReadOnlyList<string>> evictedSlot);

await using (RunHandle run = await host.MaterializeAsync(evicting))
{
    IReadOnlyList<string> seen = await run.GetValueAsync(evictedSlot);

    Console.WriteLine($"GroupBy(EvictIdle)      {seen.Count} of {readings.Length} readings reached the sink");

    await run.Completion;
}

static async IAsyncEnumerable<Reading> Trickle(Reading[] readings, int after, TimeSpan gap)
{
    for (int index = 0; index < readings.Length; index++)
    {
        if (index == after)
        {
            await Task.Delay(gap);
        }

        yield return readings[index];
    }
}

internal sealed record Reading(string Station, int Celsius);
```

```console
dotnet run
```

```
Grouped(4)              3 windows, sizes 4 4 2
GroupedWithin(4, 200ms) 3 windows, sizes 3 4 3
Sliding(3, 2)           5 windows, sizes 3 3 3 3 2
GroupBy(2 keys)         A keyed stage holding a substream for at most 2 keys at once was handed an element whose key 'east' would have been one more. Raise MaxActiveKeys, group over a coarser key, or declare ActiveKeyOverflowPolicy.EvictIdle; the stage does not evict by default, because an evicted key's substream ends where it stood and the same key can then appear downstream a second time.
GroupBy(EvictIdle)      10 of 10 readings reached the sink
```

## Batching by count

`Grouped(4)` turns a `Source<Reading>` into a `Source<IReadOnlyList<Reading>>`.
Ten readings became three windows of 4, 4 and 2 — the last window is short
because the stream ended, and a short final window is emitted rather than
discarded.

The memory cost is one window: at most four readings, whatever the stream's
length. That is the guarantee, and it is the reason the count is required rather
than defaulted.

**Grouping is also the fix for a batching API.** If your downstream takes a
hundred rows per call, `Grouped(100)` in front of the sink turns "one call per
element" into "one call per hundred", and the pipeline still cannot hold more
than a hundred.

## Batching by count *or* time

`Grouped` alone has a problem: if the feed goes quiet with two readings in hand,
those two sit there until a third arrives. For a feed that comes in bursts, that
is unbounded *latency* even though the memory is fine.

`GroupedWithin(4, window)` closes a window when **either** four readings have
arrived **or** the window has elapsed since the group opened, whichever comes
first. Read the output: `3 4 3`. The source above hands over three readings, then
pauses for two seconds, then hands over the rest. The pause is longer than the
200-millisecond window, so the group of three closed on time; then four closed on
count; then the tail of three closed when the stream ended.

There is a weighted form too, `GroupedWithin(maxElements, maxWeight, window,
cost)`, for when the bound you care about is bytes or rows rather than elements —
you supply a `Func<T, int>` and the window also closes when the accumulated
weight is reached.

## Overlapping windows

`Sliding(3, 2)` emits a window of 3 and then advances by 2, so consecutive
windows share an element. Ten readings gave five windows: sizes `3 3 3 3 2`. Use
it for moving averages and any "compare this to the last few" calculation. The
memory cost is one window's worth, same as before.

## Grouping by key

[Group-by](../reference/glossary.md#group-by) splits one stream into substreams —
one per key — and runs the same flow on each:

```csharp
.GroupBy(
    new GroupByOptions { MaxActiveKeys = 2 },
    reading => reading.Station,          // the key
    Flow.For<Reading>().Select(...))     // the flow each substream runs
```

The result is a single `Source` again: the substreams' outputs are merged back
into one stream.

**Every live substream costs memory, so the number of live keys is declared.**
That is `MaxActiveKeys`, and it has no unbounded spelling for the same reason a
buffer's capacity does not: a keyed operator that quietly grows one substream per
key is a memory leak that compiles, and the number of distinct keys in a stream
is not something the runtime can guess.

## What a refusal looks like

The feed above has three stations and the graph declared two. Here is exactly
what happens:

```
A keyed stage holding a substream for at most 2 keys at once was handed an
element whose key 'east' would have been one more. Raise MaxActiveKeys, group
over a coarser key, or declare ActiveKeyOverflowPolicy.EvictIdle; the stage does
not evict by default, because an evicted key's substream ends where it stood and
the same key can then appear downstream a second time.
```

The run fails with `TrackedKeyOverflowException`. Note what the message contains:
the bound you declared, the key that exceeded it, and the three things you can do
about it. This is a designed outcome, not a crash — a keyed operator that grew
until the process died is the thing this library will not do.

Your four choices when it happens:

1. **Raise `MaxActiveKeys`.** Right when you underestimated and you know the real
   ceiling. Wrong when the key is unbounded by nature — a customer id, a session,
   a trace — because then there is no number that is high enough.
2. **Group over a coarser key.** Region instead of station, hour instead of
   timestamp, tenant instead of user. This is usually the right answer for keys
   that are unbounded by nature, and it is a modelling decision rather than a
   configuration one.
3. **Declare `ActiveKeyOverflowPolicy.EvictIdle`.** The least recently used key's
   substream is ended to make room. The last line of output shows the same graph
   with this policy: all ten readings still reached the sink. But read the
   message's warning — an evicted key's substream **ends where it stood**, so if
   that key comes back it starts a *new* substream, and anything downstream that
   assumed one substream per key for the life of the run now sees two. That is
   fine for a stateless per-key flow like the one above, and wrong for a per-key
   running total.
4. **Window before you key.** `Grouped` or `GroupedWithin` in front of the
   `GroupBy` turns many elements into few, which does not reduce the number of
   *keys* — but pairing it with a coarser key often makes the bound reachable.

## The trade-offs

**A window is latency you chose.** `Grouped(100)` means an element can wait for
ninety-nine more. `GroupedWithin` is how you put a ceiling on that, and the
window length is the ceiling.

**A collecting sink is not a window.** `Sink.Collect` accumulates the *whole*
stream, which is why `CollectOptions.MaxElements` is required — and it throws
`CollectOverflowException` rather than growing. It is a fine thing to use in a
test or on a stream you know is short, and the wrong thing to reach for on a
stream that runs all day. The windowing operators are the ones with a bound per
window.

**Eviction changes semantics, not just memory.** See choice 3 above. It is the
only option on this page that can make a downstream stage see something it did
not before.

**The bounds are in the document.** Window sizes, window durations and
`MaxActiveKeys` are all numbers in the [graph
document](../reference/glossary.md#graph-document), so two different bounds are
two different [fingerprints](../reference/glossary.md#fingerprint), and the
memory a run costs can be read off the pipeline rather than measured.

## The failure modes

| Symptom | Cause |
|---|---|
| `TrackedKeyOverflowException: A keyed stage holding a substream for at most N keys …` | More distinct keys were live than you declared. The four choices are above. |
| `CollectOverflowException: A collecting sink bounded at 4 elements was handed one more. Raise MaxElements, or bound the stream with Take; the sink does not truncate, because a shortened list is a wrong result that looks like a right one.` | A `Sink.Collect` hit its `MaxElements`. That sink holds the whole stream; you probably wanted a window or a fold. |
| A group never closes | `Grouped` with a feed that went quiet. Use `GroupedWithin` and give it a window. |
| Windows are smaller than you asked for | `GroupedWithin`, and the window is closing on *time* before the count fills. Lengthen the window or accept the smaller batches. |
| Memory grows although every bound is declared | Look at what is *inside* the per-key flow. A `Scan` accumulating a growing state costs per key, and `MaxActiveKeys` bounds the number of substreams, not the size of each one's state. |

## Where to look next

- [Bounding memory](bounding-memory.md) — the buffer, the other declared bound.
- [Branching and joining](branching-and-joining.md) — splitting by predicate
  rather than by key, where the legs are fixed and named.
- The repository's windowing sample runs this in both languages:
  [`samples/Orleans.Dataflow.Samples/CSharp/Windowing.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/Windowing.cs).
  Run it with `dotnet run --project samples/Orleans.Dataflow.Samples -- --only windowing`.
