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
  default and an `Unordered` sibling. Callbacks receive a
  `CancellationToken`; cancellation reaches in-flight work.
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

Every junction is a stage of the `local` vocabulary — `broadcast`, `balance`,
`partition`, `unzip`, `merge`, `concat`, `interleave`, `zip`, `combine-latest` —
and that has two consequences worth stating plainly:

- A graph with a junction in it declares `nondeployable`, because every local stage
  requires it, **and** `ephemeral-identity`, because this surface has no spelling
  for naming a junction occurrence. Both hold even when the source, the flows, and
  every branch sink are registered stages under names the author chose.
- Local ports declare the opaque contract `local-opaque@v1`, so wiring a registered
  stage to a junction is a seam and the graph compiler reports one
  `element-contract-mismatch` per seam — the same rule a mixed chain has broken
  since ADR 0004, at the same place, for the same reason.

**A fan-out pipeline built entirely from registered stages therefore cannot exist
today.** The junction between the registered stages cannot itself be registered,
and that waits for the provider SDK to open junction registration. Until then a
junction graph is a local graph whatever its branches are made of.

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
port carries `local-fold-result@v1`. The document stage id stays `fold` —
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

## Delegates and deployability

Lambda-based operators (`Select(x => ...)`) construct graphs that carry the
`nondeployable` capability: they run locally but are rejected for
persistence, distribution, and durable resume before execution
(ADR 0001). Durable pipelines reference registered stages through stable
identities; the registration API and its ergonomics are an M1 concern, and
the graph compiler is the enforcement point either way. The default authoring
experience stays lambda-first, because local execution and tests are the
common case.

## Run control

`RunHandle` is the single control surface: result resolution
(`GetValueAsync(slot)`), completion awaiting and shutdown/kill-switch as
intrinsics (ADR 0004 section 5 — they are properties of every run, not
declared slots), and `IAsyncDisposable` for deterministic teardown.
`host.MaterializeAsync(graph)` is the only way work starts; materializing
the same graph twice yields independent runs.

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
