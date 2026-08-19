# Operators

Every operator the library has, in both languages, grouped by what it does. If
you are looking for "is there an operator for X", the answer is on this page. If
you know the operator and want the other language's spelling, it is on the same
row.

## How to read this page

**The two frontends are the same vocabulary.** C# is a fluent chain of methods on
`Source<T>` and `Flow<TIn, TOut>`; F# is a module of functions that take the
source or the flow *last*, so `|>` composes them. The two produce the same
[graph document](glossary.md#graph-document), byte for byte.

```csharp
Source.From(orders)
    .Where(order => order.IsValid)
    .Select(OrderDocument.FromEvent)
    .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> processed);
```

```fsharp
Source.ofSeq orders
|> Source.filter (fun order -> order.IsValid)
|> Source.map OrderDocument.ofEvent
|> Source.toResult "processed" (Sink.aggregate 0L (fun count _ -> count + 1L))
```

**Almost every operator exists twice.** In C# it is a method on `Source<T>` *and*
a method on `Flow<TIn, TOut>`, with the same parameters; in F# it is a function in
the `Source` module *and* one in the `Flow` module, with the same name. A table
below gives one row per operator and the spelling is the same on either.

`Source<T>` carries 49 operators over 71 overloads; `Flow<TIn, TOut>` carries 33
over 46. **Nothing is on a flow that is not also on a source**, and the sixteen
that are on a source alone are the ones a stream is needed for: `AlsoTo`,
`Append`, `BalanceTo`, `BroadcastTo`, `CombineLatest`, `Concat`, `DivertTo`,
`FanIn`, `FanOutTo`, `Fork`, `ForkMerge`, `Interleave`, `Merge`, `PartitionTo`,
`Prepend`, `Zip`. `To` is on both, with three extra overloads on a source: the
tuple-returning forms that hand back the graph and its slot together. On a flow,
`To` answers a `Branch<TIn>` and the slot arrives through the `out` parameter —
or, in F#, through the tuple `Branch.toResult` answers with.

**The columns.** *Holds* is what the stage keeps in memory at one moment — the
number that matters when you are reasoning about a run's live heap, since a
pipeline's memory is bounded by what its [boundaries](glossary.md#boundary)
declare and never by how long the stream is. *Ordering* is what the operator does
to the order elements come out in.

**The F# names are not translations, they are the F# name for the concept.**
`Select` is `map`, `Where` is `filter`, `SelectMany` is `collect`. Where F# has
something C# does not — `choose`, and the `Async<'T>` overloads — the row says
so.

**Examples.** The snippet above is lifted verbatim from `samples/`, where both
spellings run in continuous integration and are checked against each other. Every
other snippet on this page was compiled *and run* in a scratch project written for
this page, and every value a comment claims — `103`, `14`, `6, 8, 10, 12, 14`, and
the rest — is what the run produced.

**The two columns are derived, not asserted.** Each C#/F# pair in the tables below
was built into a closed graph in both spellings and their canonical
[fingerprints](glossary.md#fingerprint) compared: eighty-two pairs, all agreeing.
Two spellings of one operator produce the same document, byte for byte, or the row
is wrong.

---

## Creating a stream

Eighteen ways to start, plus two the F# frontend adds for `Async<'T>`. All of
them are static: `Source.X(…)` in C#, `Source.x …` in F#. Building one runs
nothing — a `Source<T>` is a value. (`Source` carries a nineteenth static member,
`UnzipTo`, which *closes* a graph rather than starting one; it is under
[splitting](#splitting-one-in-several-out).)

| C# | F# | What it does | Holds | Ordering |
|---|---|---|---|---|
| `Source.From(elements)` | `Source.ofSeq elements` | Emits an in-memory sequence. The enumerator is created and disposed per run. | nothing | the sequence's |
| `Source.FromAsyncEnumerable(elements)` | `Source.ofAsyncEnumerable elements` | Emits an asynchronous sequence; cancellation and async disposal flow from the run. | nothing | the sequence's |
| `Source.Single(value)` | `Source.single value` | Emits one element and completes. | nothing | — |
| `Source.Empty<T>()` | `Source.empty` | Emits nothing and completes at once. | nothing | — |
| `Source.Never<T>()` | `Source.never` | Emits nothing and never ends of its own accord. | nothing | — |
| `Source.Failed<T>(exception)` | `Source.failed exception` | Fails the run without emitting anything. | nothing | — |
| `Source.Range(start, count)` | `Source.range start count` | Emits a run of consecutive integers. | nothing | ascending |
| `Source.Repeat(value, count)` | `Source.repeat count value` | Emits one value a declared number of times. Note the argument order differs. | nothing | — |
| `Source.Cycle(elements)` | `Source.cycle elements` | Repeats an in-memory sequence for as long as it is pulled. | nothing | the sequence's, repeating |
| `Source.FromTask(task)` | `Source.ofTask task` | Emits the value of one task. | nothing | — |
| `Source.FromFactory(factory)` | `Source.ofFactory factory` | Emits one element a factory produces, once per run. | nothing | — |
| `Source.FromAsyncFactory(factory)` | `Source.ofTaskFactory factory` | Emits one element an asynchronous factory produces, once per run. | nothing | — |
| — | `Source.ofAsync computation` | The `Async<'T>` form of the above. F# only. | nothing | — |
| `Source.Unfold(seed, generator)` | `Source.unfold generator seed` | Produces elements from a state it carries. The generator answers the next element and the next state, or ends. | one state | the generator's |
| `Source.UnfoldAsync(seed, generator)` | `Source.unfoldTask generator seed` | The asynchronous unfold. | one state | the generator's |
| — | `Source.unfoldAsync generator seed` | The `Async<'T>` form of the above. F# only. | one state | the generator's |
| `Source.FromChannel(reader)` | `Source.ofChannel reader` | Reads a `Channel<T>` the author owns. Two runs of one graph compete for its elements. | nothing | the channel's |
| `Source.Queue<T>(options, controlName)` | `Source.queue options controlName` | A bounded queue producers push into while the run is running. Reached through a [control slot](run-handles.md#result-slots-and-control-slots) of the given name. | its declared capacity | offer order |
| `Source.Tick(initialDelay, interval)` | `Source.tick initialDelay interval` | Emits the number of every tick of an interval, counting from zero. | nothing | ascending |
| `Source.FromRegistered(stage, occurrenceName, parameters)` | `Source.ofRegistered stage occurrenceName parameters` | One named occurrence of a [registered stage](glossary.md#registered-stage) — the deployable form. See [provider SDK](provider-sdk.md). | the stage's | the stage's |

A tick that comes due while the run is busy is skipped rather than queued, and
the tick number is the contract, so a consumer that fell behind can see that it
did. A queue's offer is answered `Accepted`, `Dropped`, `Closed`, or `Failed` —
acceptance is admission into the queue and never downstream completion.

**The unfold generators are named delegate types**, because they use `out`
parameters that a `Func` cannot express. `UnfoldGenerator<TState, T>.Invoke(state,
out value, out next)` answers `bool` — `false` ends the stream — and
`AsyncUnfoldGenerator<TState, T>.Invoke(state, cancellationToken)` answers a
`Task<UnfoldStep<TState, T>?>`, where `null` ends the stream and an
`UnfoldStep<TState, T>` carries `Value` and `Next`.

```csharp
RunnableGraph graph = Source.Range(1, 100)
    .Prepend(0)
    .Append(101, 102)
    .To(s => s.Count(), "seen", out ResultSlot<long> seen);

await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(graph);

long counted = await run.GetValueAsync(seen);   // 103
await run.Completion;
```

```fsharp
let graph, seen =
    Source.range 1 100
    |> Source.prepend (Source.single 0)
    |> Source.append (Source.ofSeq [ 101; 102 ])
    |> Source.toResult "seen" Sink.count

let host = LocalDataflowHost()
use! run = host.MaterializeAsync graph
let! counted = run |> Run.value seen CancellationToken.None   // 103

do! run.Completion
```

---

## Transforming

| C# | F# | What it does | Holds | Ordering |
|---|---|---|---|---|
| `Select(selector)` | `Flow.map` / `Source.map` | One element in, one out. | nothing | preserved |
| `SelectMany(selector)` | `Flow.collect` / `Source.collect` | Replaces every element with a sequence, emitted one at a time. | one enumerator | preserved, flattened |
| — | `Flow.choose` / `Source.choose` | Maps and filters in one step with a `ValueOption`. F# only. | nothing | preserved |
| — | `Flow.chooseOption` / `Source.chooseOption` | The `Option` form of the above. F# only. | nothing | preserved |
| `SelectAsync(options, selector)` | `Flow.mapTask` / `Source.mapTask` | An asynchronous map with a declared [parallelism](glossary.md#parallelism), emitting in input order. | up to `MaxConcurrency` in flight, plus finished results waiting for their turn | preserved |
| `SelectAsyncUnordered(options, selector)` | `Flow.mapTaskUnordered` / `Source.mapTaskUnordered` | The same, emitting each result as soon as it exists. | up to `MaxConcurrency` in flight | completion order |
| `SelectValueTaskAsync(options, selector)` | `Flow.mapValueTask` / `Source.mapValueTask` | The `ValueTask` form of the ordered map, for a callback that usually completes synchronously. | as above | preserved |
| `SelectValueTaskAsyncUnordered(options, selector)` | `Flow.mapValueTaskUnordered` / `Source.mapValueTaskUnordered` | The `ValueTask` form of the unordered map. | as above | completion order |
| — | `Flow.mapAsync` / `Source.mapAsync` | The `Async<'T>` form of the ordered map. F# only. | as above | preserved |
| — | `Flow.mapAsyncUnordered` / `Source.mapAsyncUnordered` | The `Async<'T>` form of the unordered map. F# only. | as above | completion order |
| `MergeMap(options, IEnumerable selector)` | `Flow.mergeMap` / `Source.mergeMap` | Expands several elements into sequences at once and merges them. | up to `MaxConcurrency` open enumerators | unspecified across the merged sequences |
| `MergeMap(options, IAsyncEnumerable selector)` | `Flow.mergeMapAsyncEnumerable` / `Source.mergeMapAsyncEnumerable` | The asynchronous-sequence form. | up to `MaxConcurrency` open enumerations | unspecified across the merged sequences |
| `Scan(seed, folder)` | `Flow.scan` / `Source.scan` | A running fold that emits every intermediate state. | one state | preserved |
| `ScanAsync(seed, folder)` | `Flow.scanTask` / `Source.scanTask` | The asynchronous running fold. | one state | preserved |
| — | `Flow.scanAsync` / `Source.scanAsync` | The `Async<'T>` form of the above. F# only. | one state | preserved |
| `Scan(seed, folder, export, restore)` | `Flow.scanDurable` / `Source.scanDurable` | A running fold whose state a [durable](glossary.md#durable-run) scope can checkpoint, given a codec to and from canonical JSON. | one state | preserved |

Ordered and unordered is the choice worth thinking about. Ordered means a
finished result waits for the ones before it, so one slow callback holds up
everything behind it; unordered emits as results land. Ordered is the default
choice; choose unordered when the callbacks vary a lot in duration.

```csharp
RunnableGraph graph = Source.From(new[] { "1", "2", "3" })
    .Select(int.Parse)
    .SelectMany(n => Enumerable.Repeat(n, n))
    .Scan(0L, (total, n) => total + n)
    .To(s => s.Last(), "total", out ResultSlot<long> total);
// total resolves to 14
```

```fsharp
let graph, total =
    Source.ofSeq [ "1"; "2"; "3" ]
    |> Source.map int
    |> Source.collect (fun n -> Seq.replicate n n)
    |> Source.scan 0L (fun total n -> total + int64 n)
    |> Source.toResult "total" Sink.last
// total resolves to 14
```

---

## Filtering and slicing

| C# | F# | What it does | Holds | Ordering |
|---|---|---|---|---|
| `Where(predicate)` | `Flow.filter` / `Source.filter` | Passes the elements a predicate accepts. | nothing | preserved |
| `Take(count)` | `Flow.take` / `Source.take` | Passes a declared number of elements, then ends the stream. | nothing | preserved |
| `Skip(count)` | `Flow.skip` / `Source.skip` | Drops a declared number of elements. | nothing | preserved |
| `TakeWhile(predicate)` | `Flow.takeWhile` / `Source.takeWhile` | Passes elements while a predicate holds; the element that breaks it is not delivered. | nothing | preserved |
| `TakeThrough(predicate)` | `Flow.takeThrough` / `Source.takeThrough` | The same, *delivering* the element that ends it. | nothing | preserved |
| `SkipWhile(predicate)` | `Flow.skipWhile` / `Source.skipWhile` | Drops elements while a predicate holds, then passes everything. | nothing | preserved |
| `DeduplicateConsecutive()` | `Flow.deduplicateConsecutive` / `Source.deduplicateConsecutive` | Drops an element equal to the one immediately before it. | one element | preserved |
| `Distinct(options)` | `Flow.distinct` / `Source.distinct` | Passes the first occurrence of every element, comparing with `EqualityComparer<T>.Default`. | up to [`MaxTrackedKeys`](options.md#distinctoptions) keys | preserved |

`Distinct` is the first operator whose memory grows with the *data* rather than
with the graph, which is why its bound is required. The key past the bound
either fails the run with
[`TrackedKeyOverflowException`](errors.md#trackedkeyoverflowexception) or evicts
the oldest key — and evicting means an element already emitted can come through
a second time, so the stream is then distinct over a window rather than over its
history.

```csharp
RunnableGraph graph = Source.Range(1, 20)
    .Where(n => n % 2 == 0)
    .Skip(2)
    .Take(5)
    .Distinct(new DistinctOptions { MaxTrackedKeys = 64 })
    .To(s => s.Collect(new CollectOptions { MaxElements = 16 }), "kept", out ResultSlot<IReadOnlyList<int>> kept);
// kept resolves to 6, 8, 10, 12, 14
```

```fsharp
let graph, kept =
    Source.range 1 20
    |> Source.filter (fun n -> n % 2 = 0)
    |> Source.skip 2
    |> Source.take 5
    |> Source.distinct (DistinctOptions(MaxTrackedKeys = 64))
    |> Source.toResult "kept" (Sink.collect (CollectOptions(MaxElements = 16)))
// kept resolves to [6; 8; 10; 12; 14]
```

---

## Batching and windowing

| C# | F# | What it does | Holds | Ordering |
|---|---|---|---|---|
| `Grouped(size)` | `Flow.grouped` / `Source.grouped` | Collects elements into lists of a declared size, emitting each the moment it fills. | one group | preserved |
| `GroupedWithin(maxElements, window)` | `Flow.groupedWithin` / `Source.groupedWithin` | The same, closing a group early when the window elapses. | one group | preserved |
| `GroupedWithin(maxElements, maxWeight, window, cost)` | `Flow.groupedWeightedWithin` / `Source.groupedWeightedWithin` | Closes a group by count, by a weight you compute, or by the clock. | one group | preserved |
| `Sliding(size, step)` | `Flow.sliding` / `Source.sliding` | Emits an overlapping window of a declared size, advancing by a declared step. | one window | preserved |
| `GroupBy(options, keySelector, group)` | `Flow.groupBy` / `Source.groupBy` | Runs one substream of the given flow per key. | one live substream per key, up to [`MaxActiveKeys`](options.md#groupbyoptions) | per substream; unspecified across keys |

The last group of a `Grouped` or `GroupedWithin` is emitted when the stream ends
and is short. A cancellation abandons it, as it abandons everything else in
flight. A `Sliding` emits its buffer as one final short window only if that
buffer holds an element no full window has already carried.

The weight bound of the weighted form is never exceeded, because the group
closes *before* the element that would break it; an element whose own weight
exceeds `maxWeight` is refused rather than waiting for a group it could never
fit in.

`GroupBy` is the sharpest way this vocabulary lets memory grow with data — one
running substream per distinct key — so the bound is required, and the key past
it either fails the run or evicts the least recently used key's substream.

```csharp
RunnableGraph graph = Source.Range(1, 10)
    .Grouped(3)
    .Select(batch => batch.Sum())
    .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "sums", out ResultSlot<IReadOnlyList<int>> sums);
// sums resolves to 6, 15, 24, 10  — the last group holds one element

_ = Source.Range(1, 10).GroupBy(
    new GroupByOptions { MaxActiveKeys = 8 },
    n => n % 3,
    Flow.For<int>().Grouped(2).Select(batch => batch.Count));
```

```fsharp
Source.range 1 10
|> Source.groupBy
    (GroupByOptions(MaxActiveKeys = 8))
    (fun n -> n % 3)
    (Flow.grouped 2 |> Flow.andThen (Flow.map Seq.length))
```

---

## Timing and rate

Every one of these reads the clock the host was given, never
`TimeProvider.System` directly, which is what makes a deterministic test of them
possible. See [hosting](hosting.md#the-local-host).

| C# | F# | What it does | Holds | Ordering |
|---|---|---|---|---|
| `InitialDelay(delay)` | `Flow.initialDelay` / `Source.initialDelay` | Holds the *first* element until a duration has passed; everything after it is untouched. | one element | preserved |
| `Delay(delay, holdback)` | `Flow.delay` / `Source.delay` | Holds *every* element for a declared duration, buffering the ones that arrive meanwhile. | up to the holdback's capacity | preserved |
| `Timeout(gap)` | `Flow.timeout` / `Source.timeout` | Fails the run with [`StreamTimeoutException`](errors.md#streamtimeoutexception) when the gap between two elements — or between the run starting and the first element — exceeds the declared one. | nothing | preserved |
| `TakeWithin(window)` | `Flow.takeWithin` / `Source.takeWithin` | Ends the stream when a duration has passed. | nothing | preserved |
| `SkipWithin(window)` | `Flow.skipWithin` / `Source.skipWithin` | Drops every element until a duration has passed. | nothing | preserved |
| `Throttle(options)` | `Flow.throttle` / `Source.throttle` | Holds the stream to a declared rate, using a token bucket. | nothing (it waits rather than buffering) | preserved |
| `Throttle(options, cost)` | `Flow.throttleBy` / `Source.throttleBy` | The same, where an element costs what a function answers rather than one unit. | nothing | preserved |

**Every duration in this table is positive, and zero is refused rather than
admitted as a no-op.** A delay of no time, a window of no duration, and a timeout
that has already elapsed are operators an author meant something else by; leaving
the operator out is the spelling for "no delay", and it costs nothing at run
time. `Timeout.InfiniteTimeSpan` is refused by the same test, for the same
reason. That rule reaches `Source.Tick` too: both its initial delay and its
interval must be positive.

A `Timeout` reports silence and never slowness: an element that takes a long
time to travel through the stages *below* the timeout is not a gap, because the
gap is measured where the stage stands. Its clock is the host's, so a run held by
`PauseAsync` for longer than the declared gap fails when the timer fires — a
pause holds the elements, not the clock.

A throttle in the [shaping mode](options.md#throttleoptions) waits; in the
enforcing mode it fails the run with
[`RateLimitExceededException`](errors.md#ratelimitexceededexception). Both modes
fail for an element whose cost exceeds the whole bucket, because no amount of
waiting could ever admit it.

```csharp
_ = Source.Range(1, 10).Throttle(new ThrottleOptions { Elements = 10, Per = TimeSpan.FromSeconds(1) });
_ = Source.Range(1, 10).Delay(TimeSpan.FromMilliseconds(10), new BufferOptions { Capacity = 16 });
_ = Source.Range(1, 10).Timeout(TimeSpan.FromSeconds(5));
```

```fsharp
Source.range 1 10 |> Source.throttle (ThrottleOptions(Elements = 10, Per = TimeSpan.FromSeconds 1.0))
Source.range 1 10 |> Source.delay (TimeSpan.FromMilliseconds 10.0) (BufferOptions(Capacity = 16))
Source.range 1 10 |> Source.timeout (TimeSpan.FromSeconds 5.0)
```

---

## Bounding memory and steering a run

| C# | F# | What it does | Holds | Ordering |
|---|---|---|---|---|
| `Buffer(options)` | `Flow.buffer` / `Source.buffer` | A declared holding place between two stages; the ordinary way to let a fast producer and a slow consumer coexist. | up to `Capacity` | preserved (except under `DropOldest` / `DropBuffer`, which lose elements) |
| `Valve(controlName, initialMode)` | `Flow.valve` / `Source.valve` | A gate the author opens and closes *while the run is running*, reached through a [control slot](run-handles.md#result-slots-and-control-slots) of the given name. | nothing | preserved |

A buffer is a [boundary](glossary.md#boundary): without one, exactly one element
is in flight in a pipeline at any moment. Even a capacity of one is a real
boundary — it decouples the segments on either side into two loops, which a
fused chain is not. The elements a buffer holds are counted against nothing
else, so a graph's total buffered memory is the sum of the capacities its author
declared.

What a full buffer does is the [overflow policy](options.md#bufferoptions), and
four of the five values discard elements. Every drop is counted and shows up on
the run's [snapshot](run-handles.md#runsnapshot).

```csharp
_ = Source.Range(1, 10).Buffer(new BufferOptions { Capacity = 32, OverflowPolicy = OverflowPolicy.DropOldest });
_ = Source.Range(1, 10).Valve("gate", ValveMode.Open);
```

```fsharp
Source.range 1 10 |> Source.buffer (BufferOptions(Capacity = 32, OverflowPolicy = OverflowPolicy.DropOldest))
Source.range 1 10 |> Source.valve "gate" ValveMode.Open
```

---

## Failure and durability

| C# | F# | What it does | Holds | Ordering |
|---|---|---|---|---|
| `Supervised(options, scope)` | `Flow.supervised` / `Source.supervised` | Declares a [supervision scope](glossary.md#supervision-scope): a region whose failures are answered by a policy rather than by failing the run. Takes any [form](options.md#supervisionoptions) but `Recover`. | the scope's own | the scope's own |
| `Supervised(options, scope, fallback)` | `Flow.supervisedRecovering` / `Source.supervisedRecovering` | The recovering scope, and it takes `Form = Recover` and nothing else: the first failure inside it emits the declared fallback and ends the scope's stream *successfully*. | the scope's own | preserved |
| `Durable(scope)` | `Flow.durable` / `Source.durable` | Declares a scope whose stateful stages are included in a [checkpoint](glossary.md#checkpoint), so a resume continues them rather than restarting them. | the scope's own | the scope's own |

A supervised element that uses every attempt a retrying scope allowed is a
[poison element](glossary.md#poison-element), and what happens to it is what
[`OnExhaustion`](options.md#supervisionoptions) declared: fail the run, drop the
element, or reset every stage in the scope. Both counts — supervised failures and
poison elements — are on the run's snapshot.

The recovering form is a different boundary and it is worth reading twice:
everything above the scope stops, everything below it drains, and the run reports
success. Nothing is retried and nothing is dropped, because the stream the scope
was producing is over.

Inside a durable scope, a `Scan` that was given `export` and `restore` (F#
`scanDurable`) contributes its state to the checkpoint; a plain `Scan` does not,
and restarts from its seed.

```csharp
_ = Source.Range(1, 10).Supervised(
    new SupervisionOptions
    {
        Form = SupervisionForm.Retry,
        MaxAttempts = 3,
        Backoff = [TimeSpan.Zero, TimeSpan.FromMilliseconds(50)],
        OnExhaustion = RetryExhaustion.Resume,
    },
    Flow.For<int>().Select(n => n * 2));

_ = Source.Range(1, 10).Durable(Flow.For<int>().Scan(0L, (t, n) => t + n));
```

```fsharp
Source.range 1 10
|> Source.supervised
    (SupervisionOptions(
        Form = SupervisionForm.Retry,
        MaxAttempts = 3,
        Backoff = [| TimeSpan.Zero; TimeSpan.FromMilliseconds 50.0 |],
        OnExhaustion = RetryExhaustion.Resume))
    (Flow.map (fun n -> n * 2))

Source.range 1 10 |> Source.durable (Flow.scan 0L (fun t n -> t + int64 n))
```

---

## Splitting and joining

A [junction](glossary.md#junction) is a stage with more than one input or more
than one output, and there are exactly nine kinds. Each holds an element until
every leg it owes has taken it, which is why a junction is a
[boundary](glossary.md#boundary).

**A local junction carries between two and eight legs.** Fewer than two is not a
junction; more than eight is refused at authoring.

### Splitting: one in, several out

| C# | F# | What it does | Holds | Ordering |
|---|---|---|---|---|
| `BroadcastTo(branches…)` | `Source.broadcastTo branches` | Every element to every leg. Closes the graph. | one element until every leg has taken it | preserved on each leg |
| `BalanceTo(branches…)` | `Source.balanceTo branches` | Each element to exactly one leg, whichever has room. Closes the graph. | one element | preserved overall; each leg sees a subset |
| `PartitionTo(router, branches…)` | `Source.partitionTo router branches` | Each element to the leg a routing function names. Closes the graph. | one element | preserved on each leg |
| `Source.UnzipTo(left, right)` | `Source.unzipTo left right` | Sends each half of a pair to a branch of its own. Closes the graph. Extension method on a source of `(TLeft, TRight)`. | one row | preserved on each leg |
| `AlsoTo(side)` | `Source.alsoTo side` | Sends every element to a branch *as well* and continues with the stream. | one element until both the branch and the main line have taken it | preserved |
| `DivertTo(predicate, side)` | `Source.divertTo predicate side` | Sends the elements a predicate accepts to a branch, and continues with the rest. | one element | preserved on each side |

`AlsoTo` and `DivertTo` are a broadcast and a partition with one leg named the
main line: the stage holds one element and waits for the leg that element belongs
on, so a side branch that is slow to take an element holds the main line up for
exactly as long.

### Joining: several in, one out

| C# | F# | What it does | Holds | Ordering |
|---|---|---|---|---|
| `Merge(other)` / `Merge(second, third)` | `Source.merge other` / `Source.merge3 second third` | Emits from whichever input has an element. | one element per input | each input's elements keep their relative order; nothing is promised across inputs |
| `Concat(next)` | `Source.concat next` | Emits the first input entirely, then the second. The later input is not pulled at all until its turn. | one element | fully deterministic |
| `Interleave(other, segmentSize)` | `Source.interleave other segmentSize` | Takes a declared number of elements from each input in turn. | one element per input | decided by the rotation, not by arrival |
| `Zip(other)` | `Source.zip other` | One element from each input, combined into a pair. Advances at the speed of the slowest input. | one element per input | preserved |
| `Zip(other, combine)` | `Source.zipWith other combine` | The same, through a function instead of a pair. | one element per input | preserved |
| `CombineLatest(other, combine)` | `Source.combineLatest other combine` | Emits on every arrival once both inputs have produced, combining it with the other's latest. | the latest element of each input | arrival order |
| `Prepend(head)` / `Prepend(elements…)` | `Source.prepend head` | Emits another source's — or a fixed run of — elements first. A concat with the arguments the other way round. | one element | fully deterministic |
| `Append(tail)` / `Append(elements…)` | `Source.append tail` | Emits another source's — or a fixed run of — elements last. | one element | fully deterministic |

`CombineLatest` is not a lockstep join and is deliberately a different word for
it: a fast input produces many rows against one slow element. It is the join for
a stream against a *setting*, not for two streams of matching rows.

The F# `prepend` and `append` take a source; the C# overloads taking a fixed run
of elements have no F# spelling — write `Source.ofSeq [ … ]` and pipe it in.

### Forking: split and rejoin in one expression

| C# | F# | What it does | Holds | Ordering |
|---|---|---|---|---|
| `Fork(left, right)` then `.Zip()` | `Source.fork left right` then `Fork.zip` | Sends every element down two flows at once, rejoined as a pair. | one element per leg | preserved |
| `Fork(left, right)` then `.Zip(combine)` | `Source.fork left right` then `Fork.zipWith combine` | The same, through a function. | one element per leg | preserved |
| `ForkMerge(left, right)` | `Source.forkMerge left right` | Sends every element down two flows and takes whichever result arrives first. | one element per leg | one element in, two out, in whatever order the paths finish |

`ForkMerge` is the shape a race is written in. It is a merge and not a zip, so
the two derivations of one element are *not* paired and nothing waits for the
slower path.

### Registered junctions

A junction a provider registered, addressed by name so the graph stays
deployable. All four take an occurrence name and a canonical payload.

| C# | F# | Shape |
|---|---|---|
| `FanOutTo(junction, occurrenceName, parameters, branches…)` | `Source.fanOutToRegistered junction occurrenceName parameters branches` | one in, *n* legs of one contract; closes the graph |
| `FanOutTo(junction, occurrenceName, parameters, left, right)` | `Source.fanOutToRegisteredPair junction occurrenceName parameters left right` | one in, two unlike legs; closes the graph |
| `FanIn(junction, occurrenceName, parameters, others…)` | `Source.fanInRegistered junction occurrenceName parameters others` | *n* inputs of one contract, one out |
| `FanIn(junction, occurrenceName, parameters, other)` | `Source.fanInRegisteredPair junction occurrenceName parameters other` | two unlike inputs, one out |

The number of legs is read from the stage's specification rather than restated
at the call, and the position of a branch is the specification's own canonical
port order. See [provider SDK](provider-sdk.md#junction-shapes).

```csharp
Branch<int> evens = Flow.For<int>()
    .Where(n => n % 2 == 0)
    .To(s => s.Count(), "even", out ResultSlot<long> even);

Branch<int> odds = Flow.For<int>()
    .Where(n => n % 2 == 1)
    .To(s => s.Count(), "odd", out ResultSlot<long> odd);

RunnableGraph graph = Source.Range(1, 10).BroadcastTo(evens, odds);
// even resolves to 5, odd resolves to 5
```

```fsharp
let evens, even = Flow.filter (fun n -> n % 2 = 0) |> Branch.toResult "even" Sink.count
let odds, odd = Flow.filter (fun n -> n % 2 = 1) |> Branch.toResult "odd" Sink.count

let graph = Source.range 1 10 |> Source.broadcastTo [ evens; odds ]
// even resolves to 5, odd resolves to 5
```

---

## Ending a stream

A [sink](glossary.md#sink) consumes elements; some produce a value when the
stream ends, and that value arrives through a
[result slot](run-handles.md#result-slots-and-control-slots).

**Two ways to spell a sink in C#.** `Sink.Count<T>()` names the element type;
`s => s.Count()` inside a `To` takes it from the chain. They build the same
stage — the `s` the lambda receives is a `SinkFactory<T>`, which `Sink.For<T>()`
also hands you directly, and it carries the same twelve members the static class
does minus `For` itself. F# has one spelling, `Sink.count`, because the module
functions are already generic.

### Sinks that produce no value

| C# | F# | What it does |
|---|---|---|
| `Sink.Ignore<T>()` / `s => s.Ignore()` | `Sink.ignore` | Consumes and discards; materializes terminal completion. |
| `Sink.ForEach<T>(callback)` / `s => s.ForEach(…)` | `Sink.forEach` | Calls back for each element, one at a time. |
| `Sink.ForEachAsync<T>(options, callback)` / `s => s.ForEachAsync(…)` | `Sink.forEachTask` | Calls back asynchronously with a declared parallelism bound. |
| — | `Sink.forEachAsync` | The `Async<'T>` form of the above. F# only. |
| `Sink.ToChannel<T>(writer)` / `s => s.ToChannel(…)` | `Sink.toChannel` | Writes each element to a `Channel<T>` the author owns. Acceptance by the channel is not processing by its consumer. |

### Sinks that produce a value

| C# | F# | Result | Holds |
|---|---|---|---|
| `Sink.Count<T>()` / `s => s.Count()` | `Sink.count` | `long` — how many elements arrived | one number |
| `Sink.First<T>()` / `s => s.First()` | `Sink.first` | the first element; fails an empty stream | one element |
| `Sink.FirstOrDefault<T>()` / `s => s.FirstOrDefault()` | `Sink.firstOrDefault` | the first element, or `default` | one element |
| `Sink.Last<T>()` / `s => s.Last()` | `Sink.last` | the last element; fails an empty stream | one element |
| `Sink.LastOrDefault<T>()` / `s => s.LastOrDefault()` | `Sink.lastOrDefault` | the last element, or `default` | one element |
| `Sink.Collect<T>(options)` / `s => s.Collect(…)` | `Sink.collect` | `IReadOnlyList<T>` of everything that arrived | up to [`MaxElements`](options.md#collectoptions) |
| `Sink.Aggregate<T, TState>(seed, folder)` / `s => s.Aggregate(…)` | `Sink.aggregate` | the folded state | one state |
| `Sink.AggregateAsync<T, TState>(seed, folder)` / `s => s.AggregateAsync(…)` | `Sink.aggregateTask` | the folded state | one state |
| — | `Sink.aggregateAsync` | The `Async<'T>` form of the above. F# only. | one state |

`Collect` is the only sink whose state grows with the stream, which is why its
bound is required and has no unbounded spelling. The element past the bound
fails the run with
[`CollectOverflowException`](errors.md#collectoverflowexception) rather than
truncating, because a truncated list is a wrong answer in the shape of a right
one. If you want the first *n*, write `Take(n)`.

`SinkWithResult<TIn, TResult>.ToSink()` — and the explicit conversion that spells
the same thing — turns a result-bearing sink into a plain one, which is how you
run its work and deliberately discard its value.

### Closing a graph

`To` on a source closes it into a `RunnableGraph`; `To` on a flow closes it into
a `Branch<TIn>`, which is what a junction takes.

| C# | F# | Produces |
|---|---|---|
| `source.To(sink)` | `Source.toSink sink source` | `RunnableGraph` |
| `source.To(s => …)` | — (F# uses the `Sink` module directly) | `RunnableGraph` |
| `source.To(sink, slotName, out slot)` | — | `RunnableGraph`, slot as an output |
| `source.To(sink, slotName)` | `Source.toResult slotName sink source` | `(RunnableGraph, ResultSlot<TResult>)` |
| `source.To(registeredSink, occurrenceName, parameters)` | `Source.toRegistered stage occurrenceName parameters source` | `RunnableGraph` |
| `source.To(registeredSink, occurrenceName, parameters, slotName, out slot)` | — | `RunnableGraph`, slot as an output |
| `source.To(registeredSink, occurrenceName, parameters, slotName)` | `Source.toRegisteredResult slotName stage occurrenceName parameters source` | `(RunnableGraph, ResultSlot<TResult>)` |
| `flow.To(sink)` | `Branch.toSink sink flow` | `Branch<TIn>` |
| `flow.To(sink, slotName, out slot)` | `Branch.toResult slotName sink flow` | `Branch<TIn>` (F# answers a tuple) |
| `flow.To(registeredSink, occurrenceName, parameters)` | `Branch.toRegistered stage occurrenceName parameters flow` | `Branch<TIn>` |
| `flow.To(registeredSinkWithResult, occurrenceName, parameters, slotName, out slot)` | `Branch.toRegisteredResult slotName stage occurrenceName parameters flow` | `Branch<TIn>` (F# answers a tuple) |

**Closing with a result-bearing sink and no name for the result does not
compile.** There are four overloads — two on `Source<T>`, two on
`Flow<TIn, TOut>` — that exist only to make that mistake a compiler error with a
useful message. Without them, `To(countingSink)` would be a wrong-type call whose
one compiler-suggested repair is a cast that silently drops the result. Binding
to the guard instead says what to write: `To(sink, "name")` for the tuple form,
`To(sink, "name", out var slot)` for the fluent form, or `To(sink.ToSink())` to
run the sink and discard its result deliberately. They are the only members on
this page that cannot be called.

---

## Reusing and composing

| C# | F# | What it does |
|---|---|---|
| `Flow.For<T>()` | `Flow.identity` | The empty flow of one element type: the start of a reusable chain. |
| `source.Via(flow)` | `Source.via flow source` | Extends a source with a reusable flow. |
| `flow.Via(next)` | `Flow.andThen next flow` | Extends a flow with another flow. |
| `source.Via(registeredFlow, occurrenceName, parameters)` | `Source.viaRegistered stage occurrenceName parameters source` | Extends a source with one named occurrence of a registered stage. |
| `flow.Via(registeredFlow, occurrenceName, parameters)` | `Flow.andThenRegistered stage occurrenceName parameters flow` | The flow form of the same. |
| `graph.AsPipeline(id, revision)` | `Pipeline.define id revision graph` | Gives a closed graph an identity and a [revision](glossary.md#revision), producing the `PipelineDefinition` a cluster can run. Refuses a graph carrying a delegate or an auto-generated occurrence name, listing every violation at once. |

A `Flow<TIn, TOut>` with nothing attached to either end is a legitimate value to
keep in a variable and use in three pipelines.

```csharp
Flow<int, string> render = Flow.For<int>()
    .Where(n => n > 0)
    .Select(n => n.ToString(CultureInfo.InvariantCulture));

_ = Source.Range(1, 10).Via(render).To(Sink.Ignore<string>());
```

```fsharp
let render: Flow<int, string> =
    Flow.filter (fun n -> n > 0) |> Flow.andThen (Flow.map string)

Source.range 1 10 |> Source.via render |> Source.toSink Sink.ignore
```

`RunnableGraph` and `PipelineDefinition` carry what a closed graph knows about
itself:

| Member | On | What it is |
|---|---|---|
| `Document` | both | the [graph document](glossary.md#graph-document) |
| `Fingerprint` | both | the SHA-256 of its canonical bytes |
| `ResultSlots` | `RunnableGraph` | the identifiers of every slot the graph declares |
| `Control<TControl>(name)` | `RunnableGraph` | the [control slot](run-handles.md#result-slots-and-control-slots) of that name; throws when there is none |
| `TryGetControl<TControl>(name, out slot)` | `RunnableGraph` | the same, answering `false` instead of throwing |
| `AsPipeline(id, revision)` | `RunnableGraph` | the deployable form |
| `Id`, `Revision` | `PipelineDefinition` | the identity it was given |
| `ResultSlot<TResult>(name, contract)` | `PipelineDefinition` | recovers a slot by name and result contract, for a caller that did not author the graph |

---

## What is not here

- **Anything that touches an outside system** is an adapter rather than an
  operator, and lives in [adapters](adapters.md) — Orleans streams, grain calls,
  reminders, observers, broadcast channels.
- **Anything that runs a stage of your own by name** is the provider seam, and
  lives in [provider SDK](provider-sdk.md).
- **The numbers these operators take** — capacities, bounds, policies — are in
  [options](options.md).
- **The exceptions they raise** are in [errors](errors.md).
- **There is no `Repartition`, no `Window` over event time, and no join on a
  key.** `GroupBy` plus a window is the shape that exists; a temporal join is not
  in this vocabulary.
