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

Linear pipeline with a materialized result:

```csharp
Source<OrderCreated> orders = Source.From(orderEvents);

Flow<OrderCreated, OrderDocument> normalize =
    Flow.For<OrderCreated>()
        .Where(order => order.IsValid)
        .Select(OrderDocument.FromEvent);

RunnableGraph graph = orders
    .Via(normalize)
    .To(Sink.Fold(0L, (count, _) => count + 1), out ResultSlot<long> processed);

await using RunHandle run = await host.MaterializeAsync(graph, cancellationToken);
long count = await run.GetValueAsync(processed, cancellationToken);
```

Reuse of one flow in two graphs (independent node identities per import):

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

ADR 0002 leaves the exposure shape open. Three candidates go to the compile
prototype; the ADR picks one:

1. **`out` parameter at attachment** (currently favored):
   `source.Via(flow).To(Sink.Fold(...), out ResultSlot<long> total)`.
   Reads fluently, binds the slot to the sink's occurrence, keeps
   `RunnableGraph` non-generic. Requires `To` overloads per result-bearing
   sink family.
2. **Typed carrier**: `Sink.Fold` returns `SinkWithResult<TIn, TResult>`;
   `To` on that type returns `(RunnableGraph Graph, ResultSlot<TResult> Slot)`.
   No `out`, but tuple returns read worse in fluent chains.
3. **Post-hoc exposure**: `GraphBuilder.ExposeResult(occurrence, name)`.
   Always available in the full graph builder; too ceremonial as the only
   path for linear pipelines.

Whichever wins: attaching one sink value twice yields two distinct slots, and
a slot binds to an occurrence in a graph, never to the sink value itself.

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
(`GetValueAsync(slot)`), lifecycle observation (completion as a slot),
shutdown/kill-switch controls (slots as well), and `IAsyncDisposable` for
deterministic teardown. `host.MaterializeAsync(graph)` is the only way work
starts; materializing the same graph twice yields independent runs.

## Open questions for the M1 ADR

1. Result-slot exposure shape (candidates above).
2. `Flow.For<T>()` versus `Flow.Create<T>()` versus implicit flow start on
   `Source` only.
3. Whether `PipelineDefinition` creation is `graph.AsPipeline(id, revision)`
   or a `Pipeline.Define(...)` static — and where deployability validation
   surfaces in the signature.
4. Exact option-record set for M2 operators and their required members.
5. Namespace layout: everything in `Orleans.Dataflow` versus splitting
   operator extensions into `Orleans.Dataflow.Operators`.

## Constraints carried from the F# frontend

The C# surface must not make the later F# API impossible (GOAL.md): no
mutable builders as the only path, no overload families whose meaning depends
on inference tricks, options as data not delegate soup, and every semantic
reachable through plain typed values. The F# design constraints live in
[F-SHARP-API.md](F-SHARP-API.md) and bind C# decisions from the start.
