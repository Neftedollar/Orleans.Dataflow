# Orleans.Dataflow

> [!WARNING]
> This project is an early work in progress. Its API, runtime model, and storage contracts are not stable. It is not ready for production use, and no package will be published before the 1.0.0 readiness criteria are satisfied.

Orleans.Dataflow is a typed distributed dataflow library for [Microsoft Orleans](https://github.com/dotnet/orleans).

The goal is an original, Orleans-native implementation with the expressive power expected from a modern streaming and dataflow system: declarative `Source<T>`, `Flow<TIn, TOut>`, and `Sink<T>` values; reusable and composable flow fragments; backpressure; explicit lifecycle and failure semantics; typed result slots resolved through run handles; and adapters for Orleans and external systems.

Akka.NET Streams is an important capability reference, not an implementation to port. Orleans.Dataflow will define its own contracts around virtual actors, durable identities, Orleans Streams, reminders, grains, placement, persistence, and cluster lifecycle.

## Start here

Three documents explain the system; everything else assumes them.

- [**Project goal**](docs/GOAL.md) defines the user-facing vocabulary — `Source<T>`,
  `Flow<TIn, TOut>`, `Sink<T>`, `RunnableGraph`, `ResultSlot<T>`, `RunHandle` — and what
  each of those words is allowed to mean. It is the only page that does, so it is the one
  to read before any code.
- [**ADR 0001: separate authoring, definition, and runtime planes**](docs/architecture/0001-definition-runtime-authoring-planes.md)
  is the split the source tree is laid out by. The `Authoring/`, `Definition/`, and
  `Runtime/` folders under `src/` *are* that decision, and no file inside them says so;
  this is where a reader learns why a type lives where it does.
- [**ADR 0004: C# authoring API baseline**](docs/architecture/0004-csharp-api-baseline.md)
  is the shape of the public C# surface, argued section by section. Source comments cite it
  by section number more often than they cite anything else, so a comment reading "ADR 0004
  section 7" is pointing at a numbered section of this file.

Below those, [`docs/design/`](docs/design) holds one semantics contract per area — the
[local](docs/design/LOCAL-RUNTIME.md) and [Orleans](docs/design/ORLEANS-RUNTIME.md)
runtimes, the [definition model](docs/design/DEFINITION-MODEL.md), the
[C#](docs/design/C-SHARP-API.md) and [F#](docs/design/F-SHARP-API.md) frontends,
[registered stages](docs/design/REGISTERED-STAGES.md), and the
[fragment algebra](docs/design/FRAGMENT-ALGEBRA.md). Those state what the runtimes are held
to rather than describing what they happen to do, and each opens with a status line naming
the milestone that wrote it — worth reading first, because several of them grew section by
section as the milestones landed.
[`docs/architecture/`](docs/architecture) holds the numbered decisions; the code cites them
by number throughout, and a citation always names the file carrying that number. Which
package a namespace comes from is a table in
[compatibility](docs/COMPATIBILITY.md#namespaces-and-the-packages-they-come-from), because
two of the namespaces span two assemblies each.

## Direction

- C# API first, backed by a language-neutral immutable graph model.
- Idiomatic F# API as an equal frontend over the same graph algebra, not a thin wrapper around C# overloads.
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

The same graph authored in F# — the equal frontend over the same algebra,
producing a byte-identical document (the fingerprint-equality invariant the
F# suite holds for every operator):

```fsharp
let graph, processed =
    Source.ofSeq orderEvents
    |> Source.filter (fun order -> order.IsValid)
    |> Source.map OrderDocument.ofEvent
    |> Source.toResult "processed" (Sink.aggregate 0L (fun count _ -> count + 1L))

let! run = LocalDataflowHost().MaterializeAsync(graph, cancellationToken)
let! count = run |> Run.value processed cancellationToken
```

Underneath: stable identifiers, the immutable graph document with structural
validation, canonical serialization with golden fixtures, graph
fingerprints, stage catalog contracts, and a graph compiler with stable
diagnostic rules. On top of that, as of M5: the full operator and junction
vocabulary, Orleans execution with fenced run lifecycle, supervision scopes,
durable runs with checkpoint resume across silo death, and OpenTelemetry.
As of M7 the F# frontend ships beside it: the full linear and junction
vocabulary, registered spellings, and pipelines, with every spelling proven
byte-identical to its C# twin. Not yet here: the 1.0 qualification pass — the
[capability matrix](docs/CAPABILITY-MATRIX.md) tracks honest per-capability
status, the [roadmap](docs/ROADMAP.md) orders the work ahead,
[operations](docs/OPERATIONS.md) is the deployment runbook,
[benchmarks](docs/BENCHMARKS.md) is what the runtime costs and holds, with the
grade of those numbers stated beside them, and
[compatibility](docs/COMPATIBILITY.md) is what it runs on and what its public
API guarantee covers.

## Development policy before 1.0.0

Until 1.0.0, development is intentionally performed through frequent, reviewed commits directly to `main`. Pull requests and package publication begin with the 1.0.0 release process. Repository documentation remains explicit about incomplete behavior and must not make production-readiness claims without qualification evidence.

Detailed goals, architecture decisions, parity tracking, and milestone criteria live under `docs/`; the start-here section above is the way in.
