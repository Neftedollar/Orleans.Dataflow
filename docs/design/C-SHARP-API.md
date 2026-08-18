# C# API direction

- Status: Design baseline for milestone M1; the final names and shapes are fixed by an API ADR with compile prototypes
- Package direction: `Orleans.Dataflow` (authoring), `Orleans.Dataflow.Abstractions` (definition contracts)

C# is the primary authoring frontend. This document records the surface being
designed, real usage examples, and the questions the M1 API ADR must settle.
Everything here compiles against the semantic decisions already fixed by
ADR 0001 (three planes), ADR 0002 (result slots, no `TMat` threading), and
ADR 0003 (canonical documents).

## Shape of the surface

```csharp
Source<T>           // reusable description of where elements enter a graph
Flow<TIn, TOut>     // reusable typed transformation
Sink<T>             // reusable terminal consumer
Branch<TIn>         // one leg of a junction: everything it feeds, ending in a sink
Fork<T1, T2>        // one stream through two flows, awaiting its rejoin
RunnableGraph       // closed, validated graph; not generic over results
ResultSlot<T>       // typed declaration of one result or runtime control
RunHandle           // resolves slots for one materialized run
PipelineDefinition  // named, versioned deployable definition
```

Authoring values are immutable and reusable; composing them never starts
work. Static factories live on non-generic companion classes (`Source.From`,
`Sink.Ignore`), operators are instance methods so IntelliSense carries the
whole vocabulary.

## Representative usage

Linear pipeline with a materialized result (the compiling form; see
[ADR 0004](../architecture/0004-csharp-api-baseline.md)):

```csharp
Source<OrderCreated> orders = Source.From(orderEvents);

Flow<OrderCreated, OrderDocument> normalize =
    Flow.For<OrderCreated>()
        .Where(order => order.IsValid)
        .Select(OrderDocument.FromEvent);

RunnableGraph graph = orders
    .Via(normalize)
    .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> processed);

await using RunHandle run = await host.MaterializeAsync(graph, cancellationToken);
long count = await run.GetValueAsync(processed, cancellationToken);
```

Counter-example, kept deliberately: `To(Sink.Aggregate(0L, (count, _) => count + 1), ...)`
does not compile (`CS0411` — the element type appears only as an implicit
lambda parameter, and C# does not flow the outer call's element type inward).
The sink-factory lambda pins the element type from the source, which is why
the `s => s.Aggregate(...)` form needs zero type arguments and zero annotations.

Reuse of one flow in two graphs (each closure numbers its own occurrences;
the two documents are independent even where the generated ids coincide):

```csharp
Flow<OrderCreated, OrderDocument> normalize = /* as above */;

RunnableGraph toSearchIndex = orders.Via(normalize).To(searchIndexSink);
RunnableGraph toArchive     = orders.Via(normalize).To(archiveSink);
```

Async enrichment with explicit concurrency and ordering:

```csharp
Flow<OrderDocument, PricedOrder> price =
    Flow.For<OrderDocument>()
        .SelectAsync(
            new ParallelismOptions { MaxConcurrency = 16, Ordering = ElementOrdering.PreserveInput },
            (order, ct) => pricingClient.PriceAsync(order, ct));
```

## Naming rules

- Where LINQ semantics match, LINQ names win: `Select`, `Where`, `SelectMany`,
  `Take`, `Skip`, `TakeWhile`, `SkipWhile`, `Distinct`, `Aggregate` for the
  fold-shaped sink.
- Where the streaming behavior differs from LINQ expectations, an unambiguous
  additional name is used instead of silently surprising callers:
  `TakeThrough` (inclusive), `GroupedWithin`, `Throttle`, `Buffer`,
  `RecoverWith`.
- Async variants are explicit, not overload-inferred: `SelectAsync` (`Task`),
  `SelectValueTaskAsync` (`ValueTask`), each with ordered semantics by
  default and an `Unordered` sibling, and `ScanAsync`/`AggregateAsync` for the
  two folds. Callbacks receive a `CancellationToken`; cancellation reaches
  in-flight work. The one place two overloads do compete for a lambda is
  `MergeMap`, and they cannot be confused: an `IAsyncEnumerable<TNext>` is not
  an `IEnumerable<TNext>`, so the answer's own type picks the overload.
- Option types are per-concern records (`ParallelismOptions`,
  `BufferOptions`, `RestartOptions`, source/sink adapter options), never one
  generic options bag. Names make ownership obvious; a source option cannot
  be passed where a sink option belongs.
- Nullable annotations everywhere; no ambiguous overload pairs (a delegate
  overload and an options overload must not compete for the same lambda).

## Result slots in linear composition

Decided by [ADR 0004](../architecture/0004-csharp-api-baseline.md) on compile
evidence: the hybrid of a typed carrier and a mandatory slot name.
`Sink.Aggregate` and every result-bearing factory return
`SinkWithResult<TIn, TResult>`; `To` offers three instance overloads —
`To(Sink<T>)`, the tuple form `To(sinkWithResult, "name")`, and the fluent
form `To(sinkWithResult, "name", out ResultSlot<TResult> slot)` — plus
sink-factory lambda variants (`To(s => s.Aggregate(...), "name", out var slot)`)
that make inference total. The mandatory name separates overloads by arity
(dropping a result is always explicit), and gives every slot an
author-stable durable identity instead of a positional machine name.

Attaching one sink value twice yields two distinct slots (two names), and a
slot binds to the fingerprint of the document that declared it — plus, for
nondeployable graphs, the built instance's authoring nonce (ADR 0004
section 4) — never to the sink value itself.

## Junctions: fan-out, fan-in, and the diamond

Decided by [ADR 0006](../architecture/0006-multiport-authoring.md) on nine compile
prototypes, which live on as tests. The cautionary tale is Akka's GraphDSL — a
builder with port objects that its own users avoid — so the common shapes read as
sentences on the values that already exist, and the full generality of the
definition plane stays reachable through the fragment algebra rather than through
a second DSL.

**A branch is a value.** `Branch<TIn>` is a sink-terminated continuation:
everything one junction leg feeds. It is built by a `To` family on
`Flow<TIn, TOut>` that mirrors `Source<T>`'s — plain sinks, sink factories,
result-bearing sinks with a mandatory slot name and an `out ResultSlot<TResult>`,
and the registered-stage forms — and it exists as a type because a leg has no
receiver to hang off: type information flows left to right from sources, and a leg
is built right to left from its sink. `Flow.For<T>()` is the anchor that states the
branch's input type, which is why it graduated from convenience to load-bearing.

**Fan-out is a terminal call on the source**, closing the graph the way `To` closes
a chain:

```csharp
RunnableGraph graph = orders.BroadcastTo(
    Flow.For<Order>().To(s => s.Count(), "counted", out ResultSlot<long> counted),
    Flow.For<Order>().To(s => s.Aggregate(0m, (sum, o) => sum + o.Amount), "totaled", out ResultSlot<decimal> totaled));
```

`BalanceTo` spreads work, `PartitionTo(router, …)` classifies by the element,
`UnzipTo(left, right)` splits a source of pairs into two differently typed legs,
and `AlsoTo(branch)` is the tap — broadcast sugar that keeps the main line
flowing and returns a `Source<T>`.

**Fan-in is a combinator on sources**, and each returns a `Source<T>` so the chain
continues: `a.Merge(b)`, `a.Merge(b, c)`, `a.Concat(b)`,
`a.Interleave(b, segmentSize)`, `a.Zip(b)` for pairs, `a.Zip(b, combine)`, and
`a.CombineLatest(b, combine)`.

**The diamond is a carrier.** `source.Fork(left, right)` broadcasts one stream
through two flows and returns `Fork<T1, T2>`, whose `Zip()` / `Zip(combine)` rejoin
positionally — legal without a buffer exactly because both sides descend from one
broadcast — and `source.ForkMerge(left, right)` is the unordered rejoin for
race-and-take-first shapes. A tree cannot express re-convergence, which is why the
carrier exists and why nothing else needs one.

### Named multiple results

A result-bearing branch names its own slot where its sink is written, so a junction
graph declares one result per such branch and one run resolves each of them. Two
branches under one name are refused by the document's uniqueness rule.

A branch's slot is the one slot that exists before its graph does: the branch is
written as an argument of the junction call that consumes it, so the sink — and
therefore the name — is fixed one expression before there is a document to
fingerprint. The consequences are stated rather than hidden. The slot names its
graph from the junction call onwards; reading `slot.Graph` earlier throws instead
of answering with a fingerprint of nothing; and a branch that declares a result
closes exactly one graph, because handing it to a second junction call would leave
the first graph's slot pointing at the second graph. A branch that declares no
result is reusable without limit, exactly as a flow is.

### Order, identity, and what a junction costs

Branch order is argument order and is identity-bearing: the first branch's
occurrences are numbered before the second's, so swapping two arguments produces a
different document with a different fingerprint — the same rule reordering a chain
follows. Two builds of one program produce byte-identical documents.

Every junction *this* surface spells is a stage of the `local` vocabulary —
`broadcast`, `balance`, `partition`, `unzip`, `merge`, `concat`, `interleave`,
`zip`, `combine-latest` — and that has two consequences worth stating plainly:

- A graph with a local junction in it declares `nondeployable`, because every local
  stage requires it, **and** `ephemeral-identity`, because these spellings have no
  place to write a name. Both hold even when the source, the flows, and every
  branch sink are registered stages under names the author chose.
- Local ports declare the opaque contract `local-opaque@v1`, so wiring a registered
  stage to a junction is a seam and the graph compiler reports one
  `element-contract-mismatch` per seam — the same rule a mixed chain has broken
  since ADR 0004, at the same place, for the same reason.

Both are costs of a *local* junction, and M4.5 removed the reason they were
unavoidable: a provider can register a junction of its own.

### Registered junctions

`RegisteredStage.FanOut` and `RegisteredStage.FanIn` produce typed handles over a
registered multi-port stage, checked port by port against the catalog at the line
that declares the handle — every port, not just the two a linear handle has:

```csharp
RegisteredFanOut<OrderDocument, OrderDocument> split =
    RegisteredStage.FanOut(catalog, splitRef, orderDocument, orderDocument);
```

They attach through ADR 0006's own shapes, with the occurrence name and payload
every registered attachment carries — a fan-out is a terminal call taking
branches, a fan-in is a combinator on sources:

```csharp
RunnableGraph graph = Source.FromRegistered(orderSource, "orders-in", sourceParameters)
    .Via(normalize, "normalize", normalizeParameters)
    .FanOutTo(
        split,
        "split",
        splitParameters,
        Flow.For<OrderDocument>().To(countSink, "count-left", sinkParameters, "left", out ResultSlot<long> left),
        Flow.For<OrderDocument>().To(countSink, "count-right", sinkParameters, "right", out ResultSlot<long> right));

Source<OrderDocument> joined = first.FanIn(join, "join", joinParameters, second);
```

**That graph declares no capability token at all**, so `AsPipeline` accepts it and
a branching pipeline is deployable for the first time. What each rule above costs
is unchanged; what changed is that a fully registered graph is no longer subject
to either.

Four factories, because two shapes have legs or inputs of unlike element types
and a type argument cannot be a parameter: `FanOut<TIn, TOut>` (*n* legs of one
contract) and `FanOut<TIn, TLeft, TRight>` (the unzip shape),
`FanIn<TIn, TOut>` (*n* inputs of one contract) and
`FanIn<TFirst, TSecond, TOut>` (the zip shape).

Two rules are worth stating because they are the specification's rather than the
author's:

- **Arity is read, not asked for.** How many legs a junction has is a fact about
  the registered stage; a call supplies exactly `junction.Legs` branches, or
  exactly `junction.Inputs` streams counting the receiver, and a call with the
  wrong number is refused naming both numbers.
- **Position is the specification's canonical port order**, ordinal by port name.
  The first branch is wired to the first output port that sorts, not to the one
  the provider happened to write first — and the same order is what a provider's
  own router or combiner answers in.

What the junction *does* with an element is the provider's, stated by the runtime
its factory builds and configured by the occurrence's payload; nothing on these
calls takes a router or a combiner, which is the same difference every registered
stage has from a lambda one.

**Mixing is unchanged.** A lambda branch under a registered junction is still one
`element-contract-mismatch`, because a local port still declares
`local-opaque@v1`. What M4.5 changed is that a fully registered graph no longer
has such an edge.

### What is deliberately not here

- **No fluent cycle spelling.** A loop is authored through the fragment algebra,
  where edges are explicit; a fluent cycle would hide the one thing the cycle rule
  needs an author to see, the relieving boundary. `GraphFragmentComposer.Wire`
  joins an open output to an open input of one fragment and is that path — it is
  also what re-convergence needs, which `Connect` cannot express because it merges
  two fragments and can never join one to itself.
- **No graph-builder DSL.** Nine programs covered every junction without one, and
  the algebra remains the escape hatch for arbitrary topology.
- **Two- and three-input overloads only.** Wider joins chain
  (`a.Merge(b).Merge(c)`), and the chain is honest about being two nodes: merge is
  associative, the two documents are not interchangeable, and that is stated rather
  than papered over with a flattening rewrite.
- **No tuple form of a branch's `To`.** An `out` parameter is legal on a branch
  precisely because branches are built as arguments; a tuple there would have to be
  unpacked into a statement first, which is the shape the fluent form avoids.

### Guards against a dropped result

The result-dropping foot-gun of ADR 0004 section 3 exists on a branch too, and is
closed the same way: `Flow<TIn, TOut>.To(sinkWithResult)` and
`To(s => s.Count())` are `[Obsolete(error: true)]` overloads whose diagnostic names
the two correct spellings. Without them the compiler's one suggested repair is a
cast to `Sink<TOut>`, which compiles and silently discards the result. Discarding
one deliberately stays available and stays explicit: `To(s => s.Count().ToSink())`.
The registered result-bearing form needs no guard, because
`RegisteredSinkWithResult<TIn, TResult>` does not convert to `RegisteredSink<TIn>`
at all.

## Local stage vocabulary

The lambda-first slice authors against five built-in `local` stages:
`from-enumerable`, `select`, `where`, `fold`, `ignore`, all major version 1,
registered in the public `LocalStageCatalog` so every authored document
validates cleanly through `GraphCompiler`. Because local graphs are typed by
C# generics and delegates never enter the document, every local port
declares the single opaque element contract `local-opaque@v1`, parameters
are the empty payload under `local-parameters@v1`, and the fold's result
port carries `local-fold-result@v1` — which `fold-async` declares too, because
awaiting is not a different shape and an identity is not renamed to add one.
The document stage id stays `fold` —
the semantic name — while the C# surface spells it `Sink.Aggregate` per the
naming rules and the F# frontend will spell it `Sink.fold`. Delegates and
the aggregate seed live in an internal binding table on `RunnableGraph`,
keyed by node id, for the local runtime to bind at materialization; that
table is the concrete meaning of the `nondeployable` token every such
document declares, and auto-named occurrences add `ephemeral-identity`.

## Timing, rate, and the clock

The operators that read a clock are ordinary members of `Source<T>` and
`Flow<TIn,TOut>`, mirrored on both per the ADR 0004 discipline, and .NET-first
in name per the naming rules above:

| Spelling | What it does | What it takes |
|---|---|---|
| `Source.Tick(initialDelay, interval)` | emits the number of every tick, skipping the ticks a slow consumer missed | two positive durations |
| `Delay(delay, holdback)` | shifts every element by the delay, holding at most the declared number at once | a positive duration and `BufferOptions` |
| `InitialDelay(delay)` | holds the first element until the duration has passed since the run started | a positive duration |
| `Timeout(gap)` | fails the run when the stream goes quiet for longer than the gap | a positive duration |
| `TakeWithin(window)` | ends the stream when the window closes | a positive duration |
| `SkipWithin(window)` | drops everything that arrives before the window closes | a positive duration |
| `Throttle(options)` | holds the stream to a declared rate | `ThrottleOptions` |
| `Throttle(options, cost)` | the same, charged by what each element is worth | `ThrottleOptions` and `Func<T,int>` |
| `Valve(controlName, initialMode)` | holds the stream while its control is closed | a control name and a `ValveMode` |

`ThrottleOptions` is a per-concern record like every other (`Elements`, `Per`,
`MaximumBurst`, `Mode`), and `ThrottleMode` is two declared values —
`Shaping`, which waits, and `Enforcing`, which fails the run with
`RateLimitExceededException`. A delay's holdback is spelled with the very
`BufferOptions` a buffer takes, for the reason a queue's bounds are: a
capacity and an overflow policy are a capacity and an overflow policy wherever
they stand. Every duration is required to be positive — zero, negative, and
`Timeout.InfiniteTimeSpan` all describe an operator that should have been left
out — and every refusal names the operator's own parameter.

**The clock is the host's.** `new LocalDataflowHost(timeProvider)` (and the
overload that also registers providers) fixes the `TimeProvider` every run that
host starts measures by; the default is `TimeProvider.System`. Nothing about
the clock reaches the document, so a graph has one fingerprint under any
clock, and `Orleans.Dataflow.Testing.TestClock` is what turns "after exactly
the delay, and not a tick before" into a test rather than a hope. `Timeout`
fails with `StreamTimeoutException`, a `TimeoutException` subclass, so a caller
can tell a stream's own silence from a timed-out call in their callback.

`Valve` is the one of these that reads no clock and the first control this
surface declares in the middle of a chain: the run hands out an `IValve` under
the name the author gave, `Close()` holds the stream where the valve stands and
backpressures everything above it, `Open()` lets it go, and neither ever
drops an element. The state it starts in is `ValveMode.Open` unless the author
says otherwise, and that state is in the document because a graph whose valve
starts closed produces nothing until something opens it.

## Batching, flattening, deduplication, and sequence edits

The M4.3 wave-2 operators are ordinary members of `Source<T>` and
`Flow<TIn,TOut>` wherever a chain can hold them, and .NET-first in name:

| Spelling | What it does | What it takes |
|---|---|---|
| `Grouped(size)` | collects elements into lists of a declared size | a positive count |
| `Sliding(size, step)` | emits a window of the declared size every time it holds one, advancing by the step | two positive counts |
| `GroupedWithin(maxElements, window)` | closes a group on its count or on its window, whichever comes first | a positive count and a positive duration |
| `GroupedWithin(maxElements, maxWeight, window, cost)` | the same, with a third bound on what the group weighs | two positive counts, a positive duration, and `Func<T,int>` |
| `SelectMany(selector)` | replaces every element with the elements of the sequence it answers | `Func<T,IEnumerable<TNext>>` |
| `Distinct(options)` | passes the first occurrence of every element, within a declared bound | `DistinctOptions` |
| `DeduplicateConsecutive()` | drops an element equal to the one immediately before it | nothing |
| `Prepend(source)` / `Prepend(elements)` | emits another stream, or a fixed run, before this one | a `Source<T>` or a `params T[]` |
| `Append(source)` / `Append(elements)` | emits another stream, or a fixed run, after this one | a `Source<T>` or a `params T[]` |
| `DivertTo(predicate, branch)` | sends the accepted elements to a branch and continues with the rest | `Func<T,bool>` and a `Branch<T>` |

**Every batch emits its last partial group when its stream ends**, and never an
empty one, so a stream of seven grouped by three gives three groups and a
stream of six gives two. `Sliding` is the one with a subtler rule and it is one
sentence: the end of the stream emits the buffer as a final window *only if it
holds an element no window has carried*, which covers both familiar cases — a
stream shorter than the window emits everything it had, and a stream that ended
mid-overlap emits nothing new. The groups are `IReadOnlyList<T>`, copied out per
group, so a group an author keeps is theirs.

**A batch closed by a clock is a boundary and a batch closed by a count is
not.** `GroupedWithin` runs as its own segment with one bounded handoff in
front of it, exactly as an asynchronous stage does, because only a segment
waiting on its own input can be woken by a clock while nothing is arriving —
and emitting on silence is the whole reason to write it instead of `Grouped`.
The window belongs to the group rather than to the stage: it starts when the
group's first element arrives, so an empty window emits nothing because no
window is running. The weighted form closes the group *before* the element that
would break its bound, so the bound is never exceeded; a negative weight and a
weight no group could ever carry both fail the run, as they do for a throttle
by cost.

**`SelectMany` is concat-map: one inner sequence read to its end before the
next element is asked for**, so the order of the result is a function of the
input alone. The inner sequence is never collected — the run reads it one
element at a time, a bounded boundary below the stage backpressures the
enumeration, a pause parks between two inner elements, and a cancellation
abandons the rest — so an endless inner sequence is a stream this runtime paces
rather than a loop the run disappears into. A function answering an empty
sequence drops its element; one answering `null` fails the run. **Bounded-parallel
flattening is `MergeMap` and lands in wave 3**, one section below.

**`DistinctOptions` now carries a policy beside its bound.**
`KeyOverflowPolicy.Fail` is the default and keeps the operator's promise
exactly — everything emitted was the first of its key, and a bound that was
sized wrong says so with `TrackedKeyOverflowException`.
`KeyOverflowPolicy.EvictOldest` is the deliberate weakening, and what it costs
is worth reading twice: an element whose key was evicted is emitted again if it
arrives again, so the stream is distinct over a window of the last
`MaxTrackedKeys` keys rather than over its history. Age is when a key was first
remembered, so a repeat does not refresh it. `DeduplicateConsecutive` is the
other one and needs no bound at all: one element of memory, whatever the stream
carries, and it collapses runs rather than history.

**The sequence edits add no stage to the vocabulary**, which is the honest thing
to say about them. `a.Append(b)` and `a.Concat(b)` build the same document and
fingerprint identically; `a.Prepend(b)` is `b.Concat(a)`; and `DivertTo` is a
two-legged partition with the main line on its first leg, the same shape
`AlsoTo` gives a broadcast. So each inherits its junction's contract whole,
including the parts that cost something — a concat holds its later input's
source parked in a bounded channel while the earlier one plays out, and a
partition holds one element and waits for the leg it belongs on, so a slow
diverted branch holds the main line up for exactly that long. **They are on
`Source<T>` only**: a junction joins two streams and a `Flow<TIn,TOut>` is a
chain with one open input, so there is nowhere for the second stream to enter.

## Bounded-parallel flattening and the asynchronous folds

The M4.3 wave-3 operators are three, and they are the three shapes the earlier
waves named as missing: the other half of flattening, and the two folds that
await.

| Spelling | What it does | What it takes |
|---|---|---|
| `MergeMap(options, selector)` | merges the sequences of several elements at once, unordered across them | `ParallelismOptions` and `Func<T,IAsyncEnumerable<TNext>>` |
| `MergeMap(options, selector)` | the same over an ordinary sequence | `ParallelismOptions` and `Func<T,IEnumerable<TNext>>` |
| `ScanAsync(seed, folder)` | folds every element through an awaiting function and emits each state | a seed and `Func<TState,T,CancellationToken,Task<TState>>` |
| `Sink.AggregateAsync(seed, folder)` | folds every element through an awaiting function into a declared result | a seed and `Func<TState,T,CancellationToken,Task<TState>>` |

**One sentence is the whole of `MergeMap`'s order contract, and both halves of
it matter: emission is unordered across inner sequences, and the order of each
inner sequence is preserved.** An element is emitted as soon as the sequence it
came from produces it, whichever sequence that is; and a sequence is never asked
for its next element until the one before it has been delivered, which is why
its own order survives being interleaved with every other's. `SelectMany` is
the ordered half — one sequence read to its end before the next element is
asked for — and an author who needs the result to be a function of the input
alone wants that one.

**`MaxConcurrency` counts open sequences, and a slot is freed when a sequence
ends** rather than when it produces one more element. An empty inner sequence
frees its slot at once; an endless one holds its slot for as long as the run
lasts, which is worth knowing before writing one. Nothing is collected: each
open sequence holds at most the one element it has produced and not handed over,
so a bounded boundary below the stage paces all of them together and a buffer
written in front of the stage is its own input channel, exactly as it is for an
asynchronous stage.

**What a stop does to it, in three sentences.** A failure of any inner
sequence, or of the function itself, faults the run and cancels the rest —
every sequence the stage opened is released on every terminal path, and
releasing one means awaiting its own `DisposeAsync` rather than starting it. A
shutdown plays the sequences already open out to their natural end, because
their elements were admitted. A cancellation abandons them and releases them at
once. A function answering `null` fails the run, exactly as a concat-map's
does.

**The ordinary-sequence spelling is the same operator and the same node**: both
build a `merge-map` with the same payload and the same fingerprint, because how
an author's sequence produces its elements is behavior in the way the body of a
mapping function is. Its one price is stated rather than hidden — an ordinary
sequence is advanced on the segment's own thread, so an inner sequence that
*blocks* holds up every other sequence open beside it, which is what the
asynchronous spelling exists for.

**The two asynchronous folds declare no concurrency, and the absence is the
contract.** The state the next element folds into is this fold's answer, so one
fold runs at a time by construction rather than by an admission rule — there is
no bound for an author to write down, no window to hold, and no boundary: the
wait happens on the segment's own thread exactly where a synchronous fold's work
would. `ScanAsync` is `Scan` with a fold that awaits and keeps every one of its
promises (one state out per element in, the seed not emitted, an empty stream
emitting nothing, fresh state per run). `Sink.AggregateAsync` is `Aggregate`
with a fold that awaits, and it is the terminal `ForEachAsync` is not: it
resolves a declared slot when the run ends, where `ForEachAsync` declares a
bound because its callbacks are independent and declares no result because it
accumulates nothing. Both folders receive the run's own token; a failure
mid-fold faults the run with the author's own exception; a pause parks between
two folds, holding the state the last one produced; and a shutdown resolves what
was folded so far.

## Grouping by key

The M4.4 operator is one member on `Source<T>` and `Flow<TIn,TOut>`, mirrored
per the ADR 0004 discipline, and it is the first one that takes a *flow* as an
argument:

| Spelling | What it does | What it takes |
|---|---|---|
| `GroupBy(options, keySelector, group)` | runs one instance of `group` per key and merges what they emit | `GroupByOptions`, `Func<T,TKey>`, and `Flow<T,TOut>` |

**The substream flow is declared once and instantiated per key.** `group` is an
ordinary flow value — reusable, immutable, composable into as many graphs as you
like — and every key gets its own instance of every stage in it, so two keys'
scans keep two states and two keys' batches build two groups. There is no
`Source<Source<T>>` and nothing to consume or dispose: an author writes what a
key's stream *is*, and the runtime runs one of those per key.

**One sentence is the whole of the order contract, and both halves matter:
emission is unordered across keys, and the order of each key's own substream is
preserved.** What a substream emits leaves as it happens, so the keys interleave
downstream in the order their elements arrived; and one element is pushed
through one key's chain to its end before the next element is looked at, which
is why a key's own order survives being interleaved with every other's.

**The bound is the contract.** `GroupByOptions.MaxActiveKeys` is required and
there is no unbounded spelling, exactly as for `Distinct`. A key already active
costs nothing new however many elements it carries, and a key whose substream
ended of its own accord — a `Take` inside the group flow reaching its bound —
keeps its place, because remembering that a key ended is what keeps it ended.
What the key past the bound costs is `ActiveKeyOverflowPolicy`:

- `Fail` (the default) faults the run with `TrackedKeyOverflowException`, naming
  the bound and the key that broke it.
- `EvictIdle` flushes the key that has waited longest for an element — whatever
  its stages were holding walks downstream at that moment — and then forgets it.
  **Eviction is a flush-and-forget, so a later element of that key starts a
  fresh substream and one key can appear more than once downstream**, with a
  scan restarting from its seed and a batch from an empty group. That is what
  bounded means here, and it is the only reading of this policy to rely on.

**The end of the stream flushes every key still open, in the order its substream
opened.** A shutdown does the same, because it ends the stream as running out
does; a cancellation abandons what every key was holding; and a pause parks
between two elements with every key's state intact.

**The group flow holds element stages only, and that is this version's honesty
rather than a hidden limit.** It is fused per key, so it holds `Select`,
`Where`, `Scan`, `Take`, `Skip`, `TakeWhile`, `TakeThrough`, `SkipWhile`,
`Distinct`, `DeduplicateConsecutive`, `Grouped`, and `Sliding`. An asynchronous
stage, a buffer, a junction, and a stage that reads the clock each want a
segment, a channel, or a run of their own, and one per key is not something a
fused stage can hold; the refusal is an `ArgumentException` at the call site
naming every offending stage and its position. `SelectMany` and a nested
`GroupBy` are refused for this operator's own reasons — an inner sequence would
be materialized rather than streamed, and a nested bound is a different feature
with a contract of its own to state.

**A stage inside the group flow that ends its stream ends that key and not the
run.** Its residues walk downstream at once and every later element of that key
is dropped, while the run carries on delivering the other keys'.

**The group flow is in the document.** `local-group-by-parameters@v1` carries
the bound, the policy, and one entry per stage of the flow with that stage's own
reference and its own payload, so two graphs grouping through different flows
have different fingerprints. The delegates — the key function, the key type's
equality, and everything inside the flow — stay in the binding table, where
every behavior stays.

## Supervision

The M5.1 operator is two members on `Source<T>` and `Flow<TIn,TOut>`, mirrored
per the ADR 0004 discipline, and it is the second one that takes a *flow* as an
argument:

| Spelling | What it does | What it takes |
|---|---|---|
| `Supervised(options, scope)` | owns the per-element execution of `scope` and answers its failures by the declared form | `SupervisionOptions` and a `Flow<T,TOut>` |
| `Supervised(options, scope, fallback)` | the same, for `SupervisionForm.Recover`, which emits an element rather than dropping one | the above plus a `TOut` |

The name is a participle for the reason `Grouped`, `Sliding`, and
`GroupedWithin` are: it reads as "this stream, supervised by this policy, over
this flow". Two spellings rather than one with an optional argument, because
the fallback is what separates a scope that emits an element from one that drops
one — and each spelling refuses the other's forms by name, so a call site cannot
declare `Recover` and forget to say what it recovers with.

**Supervision is a scope, and a scope is a stage.** The flow is declared once,
the scope owns one instance of it, and a failure raised inside that instance is
answered by the declared form instead of failing the run. Everything outside the
scope keeps the engine's own rule: a failure fails the run, which is the default
and stays the default.

**The four forms.**

- `Resume` drops the failing element and keeps the scope's stage state, so a
  `Scan` inside it goes on counting and a half-filled `Grouped` stays open.
- `RestartStage` drops the element and rebuilds every stage inside the scope
  from its seed. What "reset" means is exact because the chain is declared: a
  scan returns to its seed, a distinct forgets its keys, a batch abandons its
  open group.
- `Retry` offers the element to the scope again, up to `MaxAttempts` (counted
  including the first), waiting the `Backoff` ladder's rung on the run's clock
  between attempts, and applies `OnExhaustion` — `Fail` (the default), `Resume`,
  or `RestartStage` — to an element that used them all.
- `Recover` emits the fallback and ends the scope's stream **successfully**:
  everything above stops, everything below drains, the result slots resolve, and
  the run reports success. Recovering with an *alternate source* is a different
  capability with a boundary of its own and is deliberately not a knob here.

**A retry re-offers to the scope's first stage.** That is what "the element is
offered to the scope again" means for a chain the scope owns whole, so a
stateful stage inside a retrying scope sees the element once per attempt. It is
the reason to keep a retrying scope small, and the reason the exhaustion answer
can escalate to `RestartStage`.

**The backoff ladder is explicit and the last rung repeats.** `Backoff` is an
`IReadOnlyList<TimeSpan>` in attempt order; a ladder shorter than the attempt
count reads as "and then this long every time", and an empty one means every
re-offer happens at once. A rung of zero is legal, unlike every other duration
in this vocabulary, because "try again now" is the ordinary shape of a first
rung. **There is no jitter in this version**: jitter spreads a fleet's restarts
and a per-element retry inside one run has no fleet, and a random source would
make "the waits are exactly what the document says" a statistical claim instead
of an asserted one. The waits take the same bracket every wait here takes — a
pause takes effect at once and holds the re-offer, a shutdown releases the wait
and the element is delivered without the rest of the rung being paid, and a
cancellation abandons it.

**The retry members are refused on the other three forms.** `MaxAttempts`,
`Backoff`, and `OnExhaustion` say nothing about a scope that never re-offers, so
setting one on `Resume` is an `ArgumentException` rather than a number written
into a document nothing would read it from.

**What a scope does not catch**, and each of these is stated rather than
discovered:

- A **cancellation** is not a failure and no form weakens it.
- A failure **outside** every scope fails the run, unchanged — including one on
  a sibling junction leg, and one a stage below the scope raises.
- A failure of the **machinery** rather than of an author's stage — a payload
  the runtime cannot read, a chain holding a shape a scope cannot execute, two
  planes describing different chains — is refused at materialization, before the
  run has an element to supervise.
- A failure raised while a stream is **ending** is not supervised: the residue
  walk has no failing element to drop and nothing to re-offer, so it travels to
  the run like any unsupervised failure.

**The scope holds element stages only, and that is this version's honesty.** It
owns the execution of its chain element by element, so an asynchronous stage, a
buffer, a junction, and a stage that reads the clock are refused where the flow
is composed, by an `ArgumentException` naming every offending stage and its
position. `SelectMany` is refused for a reason of this operator's own: its
sequence is read by the run *after* the scope's own method has returned, so a
failure raised while it was enumerated would fall outside the scope it appears
to be inside. A nested scope and a `GroupBy` are refused as recorded deferrals,
and a scope is itself refused inside a group flow, because a scope reads the run's
clock and one instance per key of that is not something a fused stage can hold.

**The policy is in the document.** `local-supervision-parameters@v1` carries
`form`, `scope` — one entry per stage with that stage's own reference and
payload — and, only for the retrying form, `maxAttempts`, `backoffTicks`, and
`onExhaustion`. Two graphs supervised differently have different fingerprints.
The fallback a recovering scope emits is not there: it is a value of an element
type no local document names, so it travels in the binding table exactly as
`Source.Single`'s element does. **No form names an exception type** — a policy
filtering by type would need CLR names in a document, which the definition plane
forbids — so a scope supervises every failure inside it alike; a declared failure
taxonomy is a recorded deferral.

**Counting.** A supervised failure is never silent: the run counts every failure
its scopes intercepted and, separately, every element a retrying scope gave up on
(ADR 0007's *poison element*). Both are internal for the reason the dropped-element
counter is — what an author will read them through is a monitor, which is a later
checkpoint with a shape of its own.

## Testing: failure injection

`Orleans.Dataflow.Testing.TestFlow.FaultPoint<T>(...)` is ADR 0007's injection
seam, and it is an **ordinary stage a document names** rather than a hook into
the engine: it validates against the catalog, it changes the fingerprint, and it
composes anywhere an element stage stands — including inside a supervision
scope, which is what makes a scope's own tests deterministic.

| Spelling | What it does |
|---|---|
| `FaultPoint<T>(mode, firstFailure)` | throws where its declared arming says to, exposing no control |
| `FaultPoint<T>(mode, firstFailure, fault)` | the same, throwing what a factory over the arrival answers |
| `FaultPoint<T>(controlName, mode, firstFailure)` | the same, exposing an `IFaultPoint` under a name |
| `FaultPoint<T>(controlName, mode, firstFailure, fault)` | both |

The arming is `FaultPointMode` — `Never`, `Once`, `Always` — and a one-based
*arrival*. It is declared in the graph rather than only set through the control
because a run starts as soon as it is materialized: a test that had to resolve a
control first would be racing the elements it wanted to fail. The control is for
re-arming a run whose elements a test is pacing through a source probe, and for
reading `ElementsSeen` and `FaultsThrown`; re-arming counts from the *next*
arrival, where the declared arming counts from the first of the run.

**A retry's re-offer is an arrival of its own**, so a scope that offered one
element three times leaves three in `ElementsSeen` — which is what makes "the
scope really did retry" a number rather than an inference. The counter belongs
to the run, not to the stage instance, so a scope that restarts its stages does
not reset it.

**The control-bearing spelling is refused inside a scope**, by name: the stages
of a scope's chain are not nodes of the document, so a slot declared on one
would be a slot nothing could resolve. Use the spelling without a control there —
its declared arming is the whole of what such a fault point does.

## Durability: options, scopes, and resume

A run becomes durable by being started with `DurableRunOptions` — a store, a name
for the run, and when a checkpoint is due — rather than by anything in the graph:

```csharp
InMemoryCheckpointStore store = new();   // a deployment brings its own

DurableRunOptions durable = new()
{
    Store = store,
    Run = RunId.Create("nightly-2026-08-18"),
    Interval = TimeSpan.FromSeconds(30),
    EveryElements = 1000,
};

await using RunHandle run = await host.MaterializeDurableAsync(graph, durable);
```

**Timing is declared and never implicit.** Both bounds are optional and either
enables checkpointing; a run that declares neither never touches the store at
all. There is no default interval, because a default would make every durable
run pay for a cadence nobody chose.

**The run is named by whoever will resume it.** An ordinary run gets a fresh
identity per materialization, because two runs of one graph are two runs; a
durable run is named by its author, because a resume is *the same run
continuing*. A checkpoint is keyed by graph *and* run, and a locally authored
graph has no identity of its own — every one of them is `anonymous` — so the run
name is what separates two checkpoints in practice, and a resume against a
different graph under one name is caught by the fingerprint rather than by the
key:

```csharp
await using RunHandle resumed = await host.MaterializeFromCheckpointAsync(graph, durable);
```

That reads the checkpoint with its ETag and continues it: sources that declare a
cursor reopen at the stored position, durable scopes take back the state they
exported, and marking sinks take back their counts. **Everything else resets** —
a scan outside a durable scope returns to its seed, a batch abandons its group, a
distinct forgets its keys — because a resumed run builds every stage from the
very factories a fresh run builds them from. A resume against a different
fingerprint or revision is refused by name: v1 resumes at the same revision only.

**What survives is declared by a scope.** `Durable` wraps a flow whose stages'
state a checkpoint carries:

```csharp
RunnableGraph graph = Source.From(orders)
    .Durable(Flow.For<Order>().Scan(
        0L,
        (total, order) => total + order.Amount,
        total => CanonicalJsonValue.Parse($"{{\"total\":{total}}}"),
        state => state.ToElement().GetProperty("total").GetInt64()))
    .To(sink);
```

Three things about that are the whole design.

- **It is not a supervision form.** Supervision answers what a failing element
  costs and this answers what a dead process costs, and the one place they would
  overlap is a contradiction — `RestartStage` resets every state in its scope and
  `durable-state` keeps every state across a resume. A durable scope inside a
  supervised section is a composition and reads as one.
- **The scope holds stages whose state is a canonical value.** A mapping and a
  filter hold nothing, a take and a skip hold a count, and a scan holds whatever
  its codec writes. A `Distinct`, a `Grouped`, a `Sliding`, and the two prefix
  operators are refused **by name** at authoring, because a checkpoint could not
  carry what they hold and a resume would silently reset it.
- **The state codec is the author's**, and it has to be: a state is a value of a
  type no document names, so only the author can say what it looks like written
  down. It is a pair of delegates, so it changes no fingerprint — which is why a
  scan bound without one is refused when the graph is *materialized* rather than
  when it is validated.

A graph holding a durable scope declares the `durable-state` capability token, so
a host that does not know what durable state is refuses the document instead of
running it without durability.

**What a resume promises is at-least-once between commit marks.** Every element a
source delivered after the last capture is delivered again, so a sink sees the
elements between the stored cursor and the crash a second time. Nothing anywhere
says exactly-once. Where a graph *holds* elements between a cursor and its sink
at capture time — a batch, a window — those elements were counted by the cursor
and never committed, so a resume loses them; the checkpoint carries both numbers,
so the gap is a measurement rather than a surprise, and the fix is to put the
holding stage inside a durable scope.

A capture holds the run at the pause machinery's own safe points for its whole
duration, the store write included. That cost is stated rather than hidden, and a
shorter interval buys a smaller replay window with throughput.

## Durability in a cluster

The same three questions, answered by a deployment instead of by a call. **Where
checkpoints go is the silo's**, registered beside the catalog and the factories:

```csharp
silo.AddOrleansDataflow(dataflow => dataflow
    .AddCatalog(providerCatalog)
    .AddFactory(providerId, new MyStageFactory())
    .UseCheckpointStore(services => services.GetRequiredService<ICheckpointStore>()));
```

There is no default, for the reason the coordinator's grain storage has none: an
in-memory default would let a deployment believe its runs were durable while
their positions died with the process. A silo that registers none runs no durable
pipeline and says so at the declaration — by name, before anything has run —
rather than at the first capture.

**What the run is called and when it checkpoints are the author's**, and they
travel on the materialization:

```csharp
await using OrleansRunHandle run = await host.MaterializeDurableAsync(
    pipeline,
    new DurablePipelineOptions
    {
        RunId = "nightly-2026-08-18",
        Interval = TimeSpan.FromSeconds(30),
        EveryElements = 1000,
    });
```

**The run identity is the one API semantic durability changes, and it is worth
reading twice.** `MaterializeAsync` names each run afresh, so calling it twice
gives two runs that both live. `MaterializeDurableAsync` is named by the caller,
so **calling it twice under one name addresses one run**: the second call hands
back a handle to the run already executing, or continues it from its checkpoint
if the silo that was hosting it has died. Two independent durable runs of one
pipeline are two names, exactly as two files are two names. A name allocated per
attempt would contradict resume outright — nothing would be able to find the
previous attempt's position.

Three consequences follow, and all three are surface rather than folklore:

- **One name, one document.** Declaring a name that already exists with a
  *different* document is refused with `PipelineResumeRefusedException`, carrying
  both fingerprints. V1 continues one document per durable run identity; an
  edited pipeline runs under a name of its own, and cross-revision migration
  stays a recorded deferral.
- **A crash is not reported as a loss.** `PipelineRunLostException` is
  unreachable for a durable run whose checkpoint exists: the poll that would have
  reported the loss is what brings the run back. A durable run that died *before*
  its first capture reports the loss like any other, because there is no position
  to continue from.
- **The handle follows the run rather than the attempt.** A resumed attempt
  claims a fresh ownership epoch, so `OrleansRunHandle.Epoch` moves while
  `Ticket.Epoch` records what was issued at the start. The handle adopts the new
  epoch from the fencing refusal that names it and carries on; an ordinary handle
  never does, because an ordinary run has no later attempt to follow.

### A run that has ended stays ended

**A durable run reports how it finished, and a later call is told rather than
asked to guess.** A checkpoint says *where* a run reached and never *whether* it
is over, so before M5.4 a run that completed and then lost its activation was
continued and re-ran its tail. Now the attempt tells its coordinator the terminal
state it reached, the declaration records it, and:

```csharp
await using (OrleansRunHandle first = await host.MaterializeDurableAsync(pipeline, durable))
{
    await first.Completion;                       // the stream ended
}

await using OrleansRunHandle again = await host.MaterializeDurableAsync(pipeline, durable);

await again.Completion;                           // returns at once; nothing ran a second time
```

Three things about that are worth stating rather than discovering:

- **Completing and failing end a run; cancelling does not.** A deactivation
  cancels the run it was hosting, so treating cancellation as an ending would
  retire a durable run every time its silo recycled. A cancelled durable run is
  continued by its next activation exactly as a crashed one is.
- **The stored checkpoint is kept.** What retires a run is the declaration, not
  the store — where a run got to is the question asked after it ends. Forgetting
  it is explicit: `ICheckpointStore.ClearAsync`, or a replacement.
- **A run's results still live only as long as its activation.** A finished run
  reports its phase to a later caller and not its values; reading a slot after
  the activation is gone reports the loss, naming the ending rather than
  suggesting the attempt vanished.

### Replacing a durable run is explicit, and it destroys

`MaterializeDurableAsync` refuses a name that holds a different document. The
call that means the other thing says so:

```csharp
await using OrleansRunHandle replacement = await host.ReplaceDurableRunAsync(
    pipeline,                                     // the revision taking the name over
    new DurablePipelineOptions { RunId = "nightly-2026-08-18", EveryElements = 1000 });
```

It **clears the stored checkpoint** — a position taken of the old document cannot
describe the new one, and migrating it is a recorded deferral rather than
something a cluster will guess at — and **supersedes the previous attempt with a
fresh epoch**, so the document runs from the beginning under the name it took
over.

- **The document does not have to differ.** Replacing a name with the very
  document it already held is how a finished durable run is run again, and how a
  failed one is retried; both destroy the same thing, which is why they are one
  call.
- **The previous attempt is abandoned by the call's second hop.** The coordinator
  only fences it — the member that rewrites the register may not await a run
  grain — but a run grain has one activation cluster-wide, so the activation this
  call then asks to start is the one hosting the old attempt and disposes its
  engine first. What is left over is the window between the two hops: a capture
  taken in it is refused by a store the old attempt no longer holds an ETag for
  (`CheckpointConflictException`). A replacement is an operator's decision, which
  is why this is stated rather than smoothed over.
- **A handle from before a replacement follows the name.** Adoption is
  forward-only and durable-only, so an old handle takes the replacement's epoch
  and controls it. Reading a *result* through it is still refused, because the
  run grain checks the declaring document's fingerprint.

### A resume is validated by the silo that caught it

A resume picks its host by which silo survived, so a half-upgraded cluster can
accept a durable run on one silo and be unable to execute it on the next. The
resumed materialization therefore validates against **its own host's** catalog
exactly as a start does, and refuses with `PipelineRejectedException` naming the
stage it cannot resolve. The declaration and the checkpoint are untouched, so a
later activation on a silo that publishes the vocabulary continues the run from
where it stopped.

Two silos with **different catalog fingerprints** resume one another's runs
whenever every stage still resolves: a catalog fingerprint is the identity of a
host's whole vocabulary, and the only fingerprints a resume compares are the
checkpoint's and the document's.

**What a resume replays is the adapter's own answer**, per adapter, in
[ADAPTERS.md](../ADAPTERS.md). An Orleans stream source stores the sequence token
of the element the run delivered and reopens the subscription there, so a durable
run over a rewindable provider replays from its position instead of from now;
every other Orleans source declares no cursor and resumes from now, which is
stated in its row rather than generalized.

## Delegates and deployability

Lambda-based operators (`Select(x => ...)`) construct graphs that carry the
`nondeployable` capability: they run locally but are rejected for
persistence, distribution, and durable resume before execution
(ADR 0001). Durable pipelines reference registered stages through stable
identities; the registration API and its ergonomics are an M1 concern, and
the graph compiler is the enforcement point either way. The default authoring
experience stays lambda-first, because local execution and tests are the
common case.

## Running registered stages in process

A registered graph is deployable, and since M4.5 it is also runnable without a
cluster. `LocalDataflowHost` takes the same two registrations a silo takes, and
the same `IDataflowStageFactory` value:

```csharp
LocalDataflowHost host = new(builder => builder
    .AddCatalog(providerCatalog)
    .AddFactory(providerId, new MyStageFactory()));

await using RunHandle run = await host.MaterializeAsync(graph);
```

`ILocalDataflowBuilder` mirrors `IOrleansDataflowBuilder` member for member where
the two hosts have the same question to answer, and both take the seam that lives
in the core package — so a provider writes its vocabulary once and it runs in
either runtime. A host given the catalog and no factory validates a document and
refuses it at materialization, naming the provider that has nothing to build it;
a host given neither refuses it at validation, naming every unresolvable node.
The registered catalog is added to the local vocabulary rather than replacing it,
because a lambda stage and a registered stage compose in one chain.

The details of what a factory may build, and of what it is told, are in
[REGISTERED-STAGES.md](REGISTERED-STAGES.md).

## Run control

`RunHandle` is the single control surface: result resolution
(`GetValueAsync(slot)`), completion awaiting and shutdown/kill-switch as
intrinsics (ADR 0004 section 5 — they are properties of every run, not
declared slots), and `IAsyncDisposable` for deterministic teardown.
`host.MaterializeAsync(graph)` is the only way work starts; materializing
the same graph twice yields independent runs.
`MaterializeDurableAsync(graph, durable)` and
`MaterializeFromCheckpointAsync(graph, durable)` are the same call with a
checkpoint story attached, and are described under Durability above.

## Open questions

Resolved by [ADR 0004](../architecture/0004-csharp-api-baseline.md):
result-slot exposure (hybrid carrier + mandatory name), `Flow.For<T>()`
naming, instance methods over extensions, non-generic `RunnableGraph`,
slot-to-run binding via `GraphFingerprint`, completion/shutdown as
`RunHandle` intrinsics, and the `ephemeral-identity`/`nondeployable`
capability split.

Still open:

1. Whether `PipelineDefinition` creation is `graph.AsPipeline(id, revision)`
   or a `Pipeline.Define(...)` static — and where deployability validation
   surfaces in the signature.
2. Exact option-record set for M2 operators and their required members.
3. Namespace layout: everything in `Orleans.Dataflow` versus splitting
   operator extensions into `Orleans.Dataflow.Operators`.
4. LINQ query syntax: `from x in source where ... select ...` already binds
   to `Select`/`Where` today. Whether query syntax is supported surface
   (tested) or explicitly unsupported is decided with the M4 `SelectMany`
   shape, because query syntax demands the three-parameter LINQ
   `SelectMany` while a streaming flatten wants a different one.

## Constraints carried from the F# frontend

The C# surface must not make the later F# API impossible (GOAL.md): no
mutable builders as the only path, no overload families whose meaning depends
on inference tricks, options as data not delegate soup, and every semantic
reachable through plain typed values. The F# design constraints live in
[F-SHARP-API.md](F-SHARP-API.md) and bind C# decisions from the start.
