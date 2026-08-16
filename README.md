# Orleans.Dataflow

> [!WARNING]
> This project is an early work in progress. Its API, runtime model, and storage contracts are not stable. It is not ready for production use, and no package will be published before the 1.0.0 readiness criteria are satisfied.

Orleans.Dataflow is a typed distributed dataflow library for [Microsoft Orleans](https://github.com/dotnet/orleans).

The goal is an original, Orleans-native implementation with the expressive power expected from a modern streaming and dataflow system: declarative `Source<T>`, `Flow<TIn, TOut>`, and `Sink<TIn, TMaterialized>` values; reusable and composable flow fragments; backpressure; explicit lifecycle and failure semantics; materialized values; and adapters for Orleans and external systems.

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

## Development policy before 1.0.0

Until 1.0.0, development is intentionally performed through frequent, reviewed commits directly to `main`. Pull requests and package publication begin with the 1.0.0 release process. Repository documentation remains explicit about incomplete behavior and must not make production-readiness claims without qualification evidence.

Detailed goals, architecture decisions, parity tracking, and milestone criteria will live under `docs/`.
