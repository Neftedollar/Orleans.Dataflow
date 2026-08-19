# Branching and joining

**The problem.** One stream has to reach several places — or several streams have
to become one — and the authoring syntax for that is the part of this library
people get stuck on.

The syntax is stuck-on-able for one reason, so here it is up front. A straight
pipeline reads left to right: source, operators, sink. A branching one cannot,
because a [junction](../reference/glossary.md#junction) has more than one leg and
you can only write one thing at a time. So you build the legs **first**, as
values, and then hand them to the junction call. Every branching example below
is that shape and nothing else.

## The whole program

```csharp
using Orleans.Dataflow;

Reading[] readings =
[
    new("north", 21), new("south", 17), new("east", 24),
    new("north", 12), new("south", 26), new("east", 19),
];

string[] stations = ["north", "south", "east"];
LocalDataflowHost host = new();
CollectOptions upToSixteen = new() { MaxElements = 16 };

// ---- 1. broadcast: every element reaches every branch --------------------

Branch<Reading> warm = Flow.For<Reading>()
    .Where(reading => reading.Celsius >= 20)
    .To(Sink.Count<Reading>(), "warm", out ResultSlot<long> warmSlot);

Branch<Reading> northern = Flow.For<Reading>()
    .Where(reading => reading.Station == "north")
    .To(Sink.Count<Reading>(), "northern", out ResultSlot<long> northernSlot);

RunnableGraph broadcast = Source.From(readings).BroadcastTo(warm, northern);

await using (RunHandle run = await host.MaterializeAsync(broadcast))
{
    Console.WriteLine($"broadcast   warm {await run.GetValueAsync(warmSlot)}, northern {await run.GetValueAsync(northernSlot)}");

    await run.Completion;
}

// ---- 2. partition: each element reaches exactly one branch --------------

Branch<Reading>[] perStation = new Branch<Reading>[stations.Length];
ResultSlot<IReadOnlyList<Reading>>[] stationSlots = new ResultSlot<IReadOnlyList<Reading>>[stations.Length];

for (int index = 0; index < stations.Length; index++)
{
    perStation[index] = Flow.For<Reading>()
        .To(Sink.Collect<Reading>(upToSixteen), stations[index], out stationSlots[index]);
}

RunnableGraph partitioned = Source.From(readings)
    .PartitionTo(reading => Array.IndexOf(stations, reading.Station), perStation);

await using (RunHandle run = await host.MaterializeAsync(partitioned))
{
    for (int index = 0; index < stations.Length; index++)
    {
        IReadOnlyList<Reading> collected = await run.GetValueAsync(stationSlots[index]);

        Console.WriteLine(
            $"partition   {stations[index],-6} {string.Join(' ', collected.Select(reading => reading.Celsius))}");
    }

    await run.Completion;
}

// ---- 3. fan-in: two streams become one ---------------------------------

RunnableGraph zipped = Source.From(stations)
    .Zip(Source.From((int[])[21, 17, 24]), (station, celsius) => new Reading(station, celsius))
    .To(Sink.Collect<Reading>(upToSixteen), "rows", out ResultSlot<IReadOnlyList<Reading>> rows);

await using (RunHandle run = await host.MaterializeAsync(zipped))
{
    IReadOnlyList<Reading> collected = await run.GetValueAsync(rows);

    Console.WriteLine($"zip         {string.Join(' ', collected.Select(reading => $"{reading.Station}:{reading.Celsius}"))}");

    await run.Completion;
}

RunnableGraph concatenated = Source.From((int[])[1, 2, 3])
    .Concat(Source.From((int[])[8, 9]))
    .To(Sink.Collect<int>(upToSixteen), "all", out ResultSlot<IReadOnlyList<int>> all);

await using (RunHandle run = await host.MaterializeAsync(concatenated))
{
    Console.WriteLine($"concat      {string.Join(' ', await run.GetValueAsync(all))}");

    await run.Completion;
}

// ---- 4. balance: whichever leg is ready --------------------------------

Branch<Reading> workerOne = Flow.For<Reading>()
    .To(Sink.Count<Reading>(), "one", out ResultSlot<long> oneSlot);

Branch<Reading> workerTwo = Flow.For<Reading>()
    .To(Sink.Count<Reading>(), "two", out ResultSlot<long> twoSlot);

RunnableGraph balanced = Source.From(readings).BalanceTo(workerOne, workerTwo);

await using (RunHandle run = await host.MaterializeAsync(balanced))
{
    long one = await run.GetValueAsync(oneSlot);
    long two = await run.GetValueAsync(twoSlot);

    Console.WriteLine($"balance     {one} + {two} = {one + two} of {readings.Length}");

    await run.Completion;
}

// ---- 5. fork and merge: two treatments, one stream back ----------------

RunnableGraph forked = Source.From(readings)
    .ForkMerge(
        Flow.For<Reading>().Where(reading => reading.Celsius >= 20).Select(reading => $"warm {reading.Celsius}"),
        Flow.For<Reading>().Where(reading => reading.Celsius < 20).Select(reading => $"cold {reading.Celsius}"))
    .To(Sink.Count<string>(), "labelled", out ResultSlot<long> labelled);

await using (RunHandle run = await host.MaterializeAsync(forked))
{
    Console.WriteLine($"forkMerge   {await run.GetValueAsync(labelled)} labelled of {readings.Length}");

    await run.Completion;
}

// ---- 6. a side branch that leaves the main stream alone ----------------

Branch<Reading> auditTrail = Flow.For<Reading>()
    .To(Sink.Count<Reading>(), "audited", out ResultSlot<long> audited);

RunnableGraph withSide = Source.From(readings)
    .AlsoTo(auditTrail)
    .Where(reading => reading.Celsius >= 20)
    .To(Sink.Count<Reading>(), "kept", out ResultSlot<long> kept);

await using (RunHandle run = await host.MaterializeAsync(withSide))
{
    Console.WriteLine($"alsoTo      audited {await run.GetValueAsync(audited)}, kept {await run.GetValueAsync(kept)}");

    await run.Completion;
}

// ---- 7. a side branch that takes elements out of the main stream -------

Branch<Reading> quarantine = Flow.For<Reading>()
    .To(Sink.Collect<Reading>(upToSixteen), "suspect", out ResultSlot<IReadOnlyList<Reading>> suspect);

RunnableGraph withDiversion = Source.From(readings)
    .DivertTo(reading => reading.Celsius < 15, quarantine)
    .To(Sink.Collect<Reading>(upToSixteen), "clean", out ResultSlot<IReadOnlyList<Reading>> clean);

await using (RunHandle run = await host.MaterializeAsync(withDiversion))
{
    IReadOnlyList<Reading> diverted = await run.GetValueAsync(suspect);
    IReadOnlyList<Reading> carried = await run.GetValueAsync(clean);

    Console.WriteLine($"divertTo    suspect {string.Join(' ', diverted.Select(r => r.Celsius))}, " +
        $"clean {string.Join(' ', carried.Select(r => r.Celsius))}");

    await run.Completion;
}

internal sealed record Reading(string Station, int Celsius);
```

```console
dotnet run
```

```
broadcast   warm 3, northern 2
partition   north  21 12
partition   south  17 26
partition   east   24 19
zip         north:21 south:17 east:24
concat      1 2 3 8 9
balance     3 + 3 = 6 of 6
forkMerge   6 labelled of 6
alsoTo      audited 6, kept 3
divertTo    suspect 12, clean 21 17 24 26 19
```

## The syntax, spelled out

Three types do all the work, and confusing them is what makes this hard.

| Type | What it is | How you get one |
|---|---|---|
| `Source<T>` | An open stream. Nothing consumes it yet. | `Source.From(...)` and every operator on a source. |
| `Flow<TIn, TOut>` | A step with **both ends open**. Not attached to anything. | `Flow.For<T>()` and every operator on a flow. |
| `Branch<T>` | A [branch](../reference/glossary.md#branch): a flow that **ends in a sink**. Closed at one end, open at the other. | `someFlow.To(someSink)` — the `To` on a *flow* answers a branch, not a graph. |

That last row is the one to hold on to. `.To(...)` on a `Source` closes the whole
thing and gives you a `RunnableGraph`. The very same `.To(...)` on a `Flow` gives
you a `Branch<T>` — a leg with a sink on it, still waiting to be attached
upstream. So the recipe is always:

```csharp
// 1. build each leg as a Branch, declaring its result slot as you close it
Branch<Reading> leg = Flow.For<Reading>()
    .Where(...)                                        // any operators you like
    .To(Sink.Count<Reading>(), "warm", out var slot);   // .To on a Flow -> Branch

// 2. hand every leg to the junction, which closes the graph
RunnableGraph graph = Source.From(readings).BroadcastTo(leg, otherLeg);
```

The junction call is what answers the `RunnableGraph`, which is why the legs must
exist first, and why each leg names its own [result
slot](../reference/glossary.md#result-slot) — one run then answers as many
questions as it has legs.

`Flow.For<T>()` is the identity flow. It looks like it does nothing, and that is
exactly what it is for: it gives you something of the right type to hang
operators on, and a leg that only collects can be written `Flow.For<T>().To(...)`
with nothing in between — which is what the partition legs above do.

## One in, many out

| Junction | Rule | The output above |
|---|---|---|
| `BroadcastTo` | Every element to **every** leg. | Both counts read all six readings: 3 warm, 2 northern. They overlap; this is not a split. |
| `PartitionTo` | **You** route each element, by index. | Two readings per station, and every reading in exactly one place. |
| `BalanceTo` | Each element to **whichever leg is ready**. | `3 + 3 = 6` — the total is guaranteed, the split is not. |
| `AlsoTo` | A side leg; the main stream **carries on unchanged**. | Six audited, and the main stream still filtered down to 3. |
| `DivertTo` | A side leg by predicate; matching elements **leave** the main stream. | Reading 12 went to quarantine and is absent from `clean`. |
| `ForkMerge` | Two treatments of every element, merged back into one stream. | All six labelled, because each reading went to both legs and one of them kept it. |

The one to get right is **broadcast versus balance**. Broadcast is "both of these
things must see the whole stream" — an audit log and a projection, a metric and a
write. Balance is "spread this work". Reaching for broadcast when you meant
balance doubles your downstream load silently; reaching for balance when you
meant broadcast loses half of each destination's input just as silently.

And note the difference between `AlsoTo` and `DivertTo`: one copies, the other
removes. `AlsoTo` is a tap; `DivertTo` is a switch.

## Many in, one out

These are operators on a source rather than junction calls, because the result is
still one open stream:

| Operator | Rule | Ordering |
|---|---|---|
| `Zip` | One element from **each** input, combined into a row. | Deterministic. Advances at the speed of its slowest leg by construction. |
| `Concat` | The first input **entirely**, then the second. | Deterministic — `1 2 3 8 9` above. |
| `Merge` | Elements in whatever order they arrive. | **Not** deterministic. Use it when you do not care, and do not assert on the order in a test. |
| `Interleave` | A declared number from each leg in turn. | Deterministic. |
| `CombineLatest` | Emits whenever any leg produces, combined with the most recent from the others. | Deterministic in content, driven by arrival. |

`Zip` also has a form that answers tuples instead of taking a combining function
(`source.Zip(other)` → `Source<(T First, T2 Second)>`), and there is a `Fork` /
`Fork(...).Zip()` pair for splitting one source down two flows and zipping the
results back. `UnzipTo` goes the other way: one input carrying a composite, two
branches carrying its parts.

## The trade-offs

**Every junction is a [boundary](../reference/glossary.md#boundary).** A junction
holds an element until every leg it owes has taken it, which is the third and
last place in a pipeline where more than one element is in flight. That is
bounded and small — one element per junction — but it is not zero.

**A slow leg holds up its siblings, and that is the bounded-memory guarantee
working.** A broadcast asks every leg for room before it pulls, so a leg that
stops consuming stops the junction, which stops the source. Nothing anywhere
accumulates on behalf of a slow leg. If you want a leg to be able to fall behind,
give *that leg* a [buffer](../reference/glossary.md#buffer) and choose its
overflow policy — see [Bounding memory](bounding-memory.md). That is a decision,
and the graph will say you made it.

**Every leg's completion matters.** A branching graph is done when all of its
legs are done. A leg whose sink never finishes keeps the whole run alive.

**A partition router must return a valid index.** `PartitionTo` takes
`Func<T, int>` against the branches you passed, positionally. An index outside
that range fails the run; `Array.IndexOf` returning `-1` for an unknown station is
the classic way to write that bug. Route unknowns to a real leg instead.

**Result slots make branching worth it.** Two legs, two slots, one run, two
answers — the alternative is two runs over the same source, which reads it twice.

## The failure modes

| Symptom | Cause |
|---|---|
| `error CS1503: cannot convert from 'Orleans.Dataflow.RunnableGraph' to 'Orleans.Dataflow.Branch<T>'` | You built the leg from a `Source` instead of a `Flow`. A leg starts at `Flow.For<T>()`; only the junction attaches it to the source. |
| The run never completes | One leg's sink never finishes. All legs must end. |
| A branch's slot never resolves | Same cause — a result resolves when that leg's stream ends. |
| The whole pipeline runs at the speed of the slowest leg | Working as designed. Buffer the slow leg if it may fall behind. |
| A `Merge` produces a different order on every run | Working as designed. Use `Concat` or `Interleave` if order across legs is the point. |
| `A partition's routing function answered 2, and this junction is wired to 2 outputs, so only 0 to 1 name one.` | The router returned an index with no branch behind it. The run fails rather than discarding the element, which is the choice the message explains. |

## Where to look next

- [Bounding memory](bounding-memory.md) — how to let one leg fall behind on
  purpose.
- [Windows and keys](windows-and-keys.md) — branching by key instead of by
  predicate, with a bound on how many keys stay live.
- The repository's junctions sample runs a broadcast in both languages:
  [`samples/Orleans.Dataflow.Samples/CSharp/Junctions.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/Junctions.cs).
  Run it with `dotnet run --project samples/Orleans.Dataflow.Samples -- --only junctions`.
