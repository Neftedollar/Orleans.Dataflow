# Orleans.Dataflow

> [!WARNING]
> This project is an early work in progress. Its API, runtime model, and storage contracts are not stable. It is not ready for production use, and no package will be published before the 1.0.0 readiness criteria are satisfied.

Orleans.Dataflow is a typed distributed dataflow library for [Microsoft Orleans](https://github.com/dotnet/orleans).

The goal is an original, Orleans-native implementation with the expressive power expected from a modern streaming and dataflow system: declarative `Source<T>`, `Flow<TIn, TOut>`, and `Sink<T>` values; reusable and composable flow fragments; backpressure; explicit lifecycle and failure semantics; typed result slots resolved through run handles; and adapters for Orleans and external systems.

Akka.NET Streams is an important capability reference, not an implementation to port. Orleans.Dataflow will define its own contracts around virtual actors, durable identities, Orleans Streams, reminders, grains, placement, persistence, and cluster lifecycle.

## Direction

- C# API first, backed by a language-neutral immutable graph model.
- Future idiomatic F# API built as an equal frontend, not a thin wrapper around C# overloads.
- Clear separation between source configuration, reusable flow stages, and sink configuration.
- Flows and complete graphs are values which can be composed into larger graphs.
- Stable stage and pipeline identities for durable or side-effecting stages.
- No persisted delegates, closures, or language-specific function representations.
- Extensible source and sink adapters without coupling the core graph model to every integration.
- Tests, documentation, fault semantics, and compatibility gates are part of the API contract.

## Current state

Linear local pipelines work end to end: the C# authoring API composes
immutable `Source<T>`/`Flow<TIn, TOut>`/`Sink<T>` values into a validated,
canonically serialized graph document, and the local runtime executes it
under strict pull (one element in flight) with distinct completion, failure,
cancellation, and graceful-shutdown outcomes and typed result-slot
resolution:

```csharp
RunnableGraph graph = Source.From(orderEvents)
    .Where(order => order.IsValid)
    .Select(OrderDocument.FromEvent)
    .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> processed);

await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(graph, cancellationToken);
long count = await run.GetValueAsync(processed, cancellationToken);
```

Underneath: stable identifiers, the immutable graph document with structural
validation, canonical serialization with golden fixtures, graph
fingerprints, stage catalog contracts, and a graph compiler with stable
diagnostic rules. Not yet here: buffered/parallel operators, junctions, any
Orleans execution, supervision, or durability — the
[capability matrix](docs/CAPABILITY-MATRIX.md) tracks honest per-capability
status, and the [roadmap](docs/ROADMAP.md) orders the work ahead.

## Development policy before 1.0.0

Until 1.0.0, development is intentionally performed through frequent, reviewed commits directly to `main`. Pull requests and package publication begin with the 1.0.0 release process. Repository documentation remains explicit about incomplete behavior and must not make production-readiness claims without qualification evidence.

Detailed goals, architecture decisions, parity tracking, and milestone criteria will live under `docs/`.
