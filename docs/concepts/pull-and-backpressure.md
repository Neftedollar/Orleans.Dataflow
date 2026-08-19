# Pull and backpressure

*Why does a fast producer never flood a slow consumer?*

Because nothing in a pipeline produces an element until something downstream has
asked for one. That single rule is the whole answer, and this page is about what
follows from it — including the three places where it is deliberately relaxed,
and what each of those places costs you in memory.

## Demand travels backwards

Elements travel forwards. **Demand travels backwards.**

The [terminal](../reference/glossary.md#terminal) — the engine's word for the
thing at the end of your pipeline — asks the stage before it for an element. That
stage asks the one before it. The request walks all the way back to the
[Source](../reference/glossary.md#source), which produces **one** element in
answer to **one** request, and that element then travels forwards through the
same chain.

This is called [strict pull](../reference/glossary.md#pull), and it is the
default. You can watch it happen. Here is a source that logs each element as it
produces it, and a sink that logs each element as it consumes it, with nothing
between them:

```csharp
RunnableGraph noBoundary = Source.From(Produce(4, log))
    .To(s => s.ForEach(n => log.Add($"consumed {n}")));

await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(noBoundary);

await run.Completion;
```

The log:

```text
produced 1 | consumed 1 | produced 2 | consumed 2 | produced 3 | consumed 3 | produced 4 | consumed 4
```

Perfectly interleaved, and it stays perfectly interleaved whether the source has
four elements or four billion. At every instant exactly one element exists inside
this pipeline. Nothing accumulates, because nothing is produced that was not
asked for.

[Backpressure](../reference/glossary.md#backpressure) is not a feature you turn
on here. It is what you get by default, and it is not configurable, because there
is nothing to configure: a source that is only ever asked for one element at a
time cannot get ahead. What you configure are the places where you have decided
to let it get ahead.

## The three boundaries

There are exactly three ways to have more than one element in flight, and all
three are things **you** put in the graph. The glossary calls them
[boundaries](../reference/glossary.md#boundary).

### A buffer holds up to its declared capacity

```csharp
RunnableGraph boundary = Source.From(Produce(8, log))
    .Buffer(new BufferOptions { Capacity = 3 })
    .To(s => s.ForEach(n => { gate.Wait(); log.Add($"consumed {n}"); }));
```

The sink here stops dead on the first element it is handed. Let the run go for
200 ms and then count what the source produced:

```text
produced before anything was consumed: 5
```

Five, and every one of the five is accounted for: one is at the sink, three are
in the [buffer](../reference/glossary.md#buffer) that was declared to hold three,
and one is in the source's hand, waiting for room. The sixth element does not
exist and will not exist until the sink takes one. The source is *stopped* — not
throttled, not sampled, stopped — by a consumer that is not consuming.

`Capacity` is `required` and there is no spelling for an unbounded buffer. That
is deliberate: a buffer is the one place in a linear graph where elements pile
up, so its size is your decision to make and not the runtime's to guess. An
unbounded default would be a memory leak that compiles.

### An asynchronous stage holds up to its declared parallelism

```csharp
RunnableGraph concurrent = Source.From(Enumerable.Range(1, 40))
    .SelectAsync(
        new ParallelismOptions { MaxConcurrency = 4 },
        async (n, token) => { await Task.Delay(5, token); return n; })
    .To(s => s.Aggregate(0L, (count, _) => count + 1), "done", out ResultSlot<long> done);
```

```text
mapped 40 elements, peak in flight 4
```

Four, never five, over forty elements. A call in flight is credit spent: an
element cannot enter the stage until one of the four in progress has finished.
[MaxConcurrency](../reference/glossary.md#parallelism) is a bound and not a
target — the engine never runs more, and runs fewer when there is less to do.
This makes "four at a time" a statement about memory as much as about
throughput.

The sample application proves the bound rather than asserting it: its
asynchronous mapping holds every invocation until the declared number are inside
it together, so a run whose bound was **not** honored would hang rather than
print a wrong number.

> From the `async-work` scenario,
> [`samples/Orleans.Dataflow.Samples/CSharp/AsyncWork.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/AsyncWork.cs).
> The snippet above was compiled and run separately for this page.

### A junction holds an element until every leg has taken it

A [junction](../reference/glossary.md#junction) — a stage with more than one
input or more than one output — is the third boundary. A
[broadcast](../reference/glossary.md#broadcast) does not pull an element from
upstream until **every** live leg has room for it, so one slow leg paces the
whole stream:

```csharp
RunnableGraph paced = Source.From(Enumerable.Range(1, 20))
    .BroadcastTo(
        Flow.For<int>().To(s => s.ForEach(n => fastLeg.Add(n))),
        Flow.For<int>().To(s => s.ForEach(_ => slow.Wait())));
```

With the second leg standing still for 200 ms:

```text
the fast leg got 2 while the slow leg took 0
```

Two — one at the fast sink and one on the way to it — and then everything stops.
Release the slow leg and both legs see all twenty. Nothing anywhere accumulated
on behalf of the leg that was not keeping up, which is precisely the memory
property a broadcast is bought for. [Branching](branching.md) has the per-junction
numbers.

## The rule this gives you

> **A pipeline's memory is bounded by what its boundaries declare, not by how
> long the stream is.**

Memorize that sentence; it is the one this whole page exists to earn. It means
you can reason about a pipeline's memory by reading the graph, without knowing
anything about the data.

Here is the demonstration. One fused chain — a source, a `Where`, a `Select`, a
fold, and no declared boundary anywhere — run twice, measuring how far the live
heap rises during the run:

```text
        10 elements  sum                   27  peak rise      5,376 bytes
10,000,000 elements  sum   33,333,326,666,667  peak rise     34,144 bytes
```

A million times as much data, and the live heap rises by kilobytes in both cases.
The difference between the two readings is measurement noise from sampling a
faster run more times — not the stream. That is what "bounded by what the
boundaries declare" means when there are no boundaries to declare: one element.

The published benchmark measures the same claim across seven shapes at one
million elements each, and the numbers line up with what each graph declared:

| Shape | Peak live heap | The bound it was measured against |
|---|---|---|
| fused chain | 144 bytes | one element in flight |
| buffer of 1024 | 17,184 bytes | 1,024 elements |
| async map, parallelism 4 | 4,016 bytes | 4 calls in flight |
| broadcast into two sinks | 4,816 bytes | one element per leg |
| bounded group-by, 16 keys | 7,200 bytes | 16 live keys |
| grain-call sink shape | 4,392 bytes | 8 calls in flight |
| **collect to a list** | **56,388,584 bytes** | **the declared maximum — this one is meant to grow** |

The last row is the reason to believe the first six. Every other claim is of the
form *the peak did not grow*, and an instrument that could not see growth would
report all of them as passing while measuring nothing. It is also the other half
of the promise: **memory follows what you declared.** Declare a bound of a
thousand and a thousand is held. Declare a bound of a million and a million is
held. Nothing here promises that a pipeline cannot be written to use memory —
only that it cannot use memory you did not ask for. See
[BENCHMARKS.md](../BENCHMARKS.md) for the machine, the method, and what the
numbers leave out.

## What a full buffer does

A buffer that is full has to do something when the next element is offered, and
**you** say what. There are five answers:

| Policy | What happens | Loses elements |
|---|---|---|
| `Backpressure` | The producing segment waits until there is room. | No |
| `DropOldest` | The oldest buffered element is discarded to make room. | Yes |
| `DropNewest` | The arriving element is discarded; the buffer keeps what it has. | Yes |
| `DropBuffer` | Everything buffered is discarded and the arriving element is buffered alone. | Yes |
| `Fail` | The run fails with `BufferOverflowException`. | The run stops |

`Backpressure` is the default, and it is the only lossless one. Be exact about
what it buys: backpressure is **prefetch, not loss**. Under it a buffer lets the
producer run ahead of the consumer by at most the declared capacity and then
stops it. The effect of a slow consumer is that the producer is slowed to its
rate — never that the producer's elements are thrown away.

The other four discard, and that is the point of naming them: **dropping is a
decision you state, never a thing that happens to you.** The sample runs the same
shape twice, changing nothing but the policy, so the two kept sets are a
statement about the policy:

```text
declared-buffer-capacity              3
orders-offered                        9
drop-oldest/orders-the-sink-saw       order-000 order-006 order-007 order-008
drop-oldest/orders-dropped            5
drop-newest/orders-the-sink-saw       order-000 order-001 order-002 order-003
drop-newest/orders-dropped            5
```

> From the `backpressure` scenario,
> [`samples/Orleans.Dataflow.Samples/CSharp/Backpressure.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/Backpressure.cs).

Read those two rows against each other. Nine orders were offered into a buffer of
three whose sink was standing still. `DropOldest` kept the tail of the stream —
the last three, plus the one already at the sink. `DropNewest` kept the head. Both
lost exactly five, and **both counted them**: `orders-dropped` comes from the
run's own [snapshot](../reference/glossary.md#snapshot), not from the sample's
bookkeeping. A dropping policy that lost elements silently would be the one thing
you could not operate around; every drop lands on a counter the run publishes.

`Fail` is the fifth answer and it treats overflow as a defect of the pipeline
rather than a condition to absorb — a buffer sized on an assumption reports that
the assumption was wrong instead of hiding it:

```text
BufferOverflowException: A buffer of capacity 2 was full when an element was offered to
it, and its overflow policy is 'Fail'. Raise the capacity, slow the source, or choose a
policy that drops.
```

## Three things worth knowing before you tune anything

**Adjacent stages are fused; a boundary exists only where you put one.** A
`Where` followed by a `Select` followed by another `Where` is one loop, not three
queues. That is why the fused chain above holds one element and not three. It is
also why the benchmark's throughput spread is two orders of magnitude between its
fastest and slowest shape: what costs is *crossing a boundary*, not the length of
a fused chain.

**A buffer immediately before an asynchronous stage becomes that stage's input
channel** — one queue, not two — which is what keeps "total memory is the sum of
declared capacities" literally true. Two adjacent buffers do *not* merge, though;
their capacities add. Both halves of that are measurable. Counting what a source
manages to produce while everything downstream stands still:

| Shape | Produced before anything moved |
|---|---|
| `Buffer(3)` → blocked sink | 5 |
| `Buffer(3)` → `SelectAsync(MaxConcurrency: 1)` blocked | **5** — the buffer *is* the stage's input channel |
| `Buffer(3)` → `Buffer(3)` → blocked sink | **9** — two queues, and the capacities add |

The extra two in each row are the element the downstream is standing on and the
one in the source's hand waiting for room; the difference between 5 and 9 is
exactly the second buffer.

**Backpressure is shared, and on a cluster that is visible.** A run reading from
an Orleans stream under the backpressure policy, if it stalls, stalls the
provider's pulling agent for that whole queue — and an unrelated subscriber on
the same stream stops receiving. That is correct behavior for a bounded system
and it is a cost you may not be able to pay, in which case the answer is to
declare a dropping policy on that source instead. [Adapters](../reference/adapters.md)
states this per source.

## Where to go next

- [Bounding memory](../guides/bounding-memory.md) — a complete program pairing a
  fast source with a slow sink.
- [Doing asynchronous work](../guides/async-work.md) — calling something slow per
  element with a bound you choose.
- [Branching](branching.md) — what each junction holds, and why a fan-in's
  completion rule is not obvious.
- [Options](../reference/options.md) — every field of `BufferOptions`,
  `ParallelismOptions`, and the rest, with defaults.
