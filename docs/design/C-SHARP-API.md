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
    .To(s => s.Fold(0L, (count, _) => count + 1), "processed", out ResultSlot<long> processed);

await using RunHandle run = await host.MaterializeAsync(graph, cancellationToken);
long count = await run.GetValueAsync(processed, cancellationToken);
```

Counter-example, kept deliberately: `To(Sink.Fold(0L, (count, _) => count + 1), ...)`
does not compile (`CS0411` — the element type appears only as an implicit
lambda parameter, and C# does not flow the outer call's element type inward).
The sink-factory lambda pins the element type from the source, which is why
the `s => s.Fold(...)` form needs zero type arguments and zero annotations.

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

Decided by [ADR 0004](../architecture/0004-csharp-api-baseline.md) on compile
evidence: the hybrid of a typed carrier and a mandatory slot name.
`Sink.Fold` and every result-bearing factory return
`SinkWithResult<TIn, TResult>`; `To` offers three instance overloads —
`To(Sink<T>)`, the tuple form `To(sinkWithResult, "name")`, and the fluent
form `To(sinkWithResult, "name", out ResultSlot<TResult> slot)` — plus
sink-factory lambda variants (`To(s => s.Fold(...), "name", out var slot)`)
that make inference total. The mandatory name separates overloads by arity
(dropping a result is always explicit), and gives every slot an
author-stable durable identity instead of a positional machine name.

Attaching one sink value twice yields two distinct slots (two names), and a
slot binds to the fingerprint of the document that declared it, never to the
sink value itself.

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

## Constraints carried from the F# frontend

The C# surface must not make the later F# API impossible (GOAL.md): no
mutable builders as the only path, no overload families whose meaning depends
on inference tricks, options as data not delegate soup, and every semantic
reachable through plain typed values. The F# design constraints live in
[F-SHARP-API.md](F-SHARP-API.md) and bind C# decisions from the start.
