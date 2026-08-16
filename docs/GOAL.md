# Project goal

## Outcome

Build Orleans.Dataflow as a typed, composable, Orleans-native dataflow system for .NET.

The system must let an application describe an immutable graph from typed sources, through reusable typed flows, to typed sinks; validate that graph before execution; materialize it into an observable runtime; apply bounded backpressure across local and distributed boundaries; and expose precise lifecycle, ordering, failure, supervision, and durability behavior.

The first public frontend is C#. The graph model and runtime contracts must remain language-neutral so that a later F# frontend can be idiomatic rather than an overload-heavy copy of the C# API.

Akka.NET Streams is the primary capability benchmark. The goal is logical capability parity where those capabilities make sense for Orleans, plus Orleans-native capabilities which follow from virtual actors, grains, reminders, Orleans Streams, placement, persistence, and cluster lifecycle. This is not a source or implementation port.

## User-facing model

The durable vocabulary is:

- `Source<T>`: a reusable description of where elements enter a graph;
- `Flow<TIn, TOut>`: a reusable, composable transformation from one typed stream shape to another;
- `Sink<T>`: a reusable terminal consumer of elements;
- `RunnableGraph`: a closed graph which can be validated and materialized;
- `PipelineDefinition`: a named and versioned deployable definition when durable Orleans execution is requested;
- `ResultSlot<T>`: a typed declaration of one materialized result or runtime control (completion, fold result, first/last element, ingress control, queue control, hub endpoint, monitor, metrics snapshot, shutdown control);
- `RunHandle`: the control and observation surface of one materialized run; it resolves `ResultSlot<T>` declarations into runtime values.

Stream shapes carry element types only. Materialized results do not thread through authoring types as an extra generic parameter; they are named typed slots resolved from the run handle ([ADR 0002](architecture/0002-result-slots.md)). A run accepts only slots of the graph identity, revision, and import scope from which it was materialized.

Names remain design candidates until the public API baseline is approved. The semantic separation is not optional.

## Required composability

The API must support all of the following without rebuilding operators by hand:

1. linear source-to-sink graphs;
2. reusable `Flow<TIn, TOut>` fragments;
3. composition of flows into larger flows;
4. composition of complete graphs where their ports and materialized values permit it;
5. explicit fan-in, fan-out, partition, broadcast, balance, merge, concat, and zip shapes;
6. explicit declaration and typed resolution of result slots;
7. source and sink adapters implemented outside the core package;
8. stable identities for durable, stateful, or side-effecting stages.

Graph construction must be declarative. Creating or composing a graph must not start background work.

## Semantic invariants

### Backpressure and bounds

- Demand and capacity are explicit runtime concepts, including across Orleans calls or streams.
- An actor mailbox, grain call, stream publication, channel write, or queue offer is not treated as downstream completion.
- Buffers are bounded by default and have an explicit overflow policy.
- A successful ingress offer reports acceptance into a documented boundary, not durable end-to-end processing unless the selected adapter actually proves that stronger condition.

### Lifecycle

- Normal completion, failure, downstream cancellation, external shutdown, abort, pause, drain, and restart are distinct states.
- Cancellation propagates to in-flight asynchronous work through a stage execution context.
- Materializing the same graph more than once creates independent runtime state unless an explicitly named durable identity says otherwise.
- Resource-owning stages release resources on completion, failure, and cancellation.

### Ordering and concurrency

- Every concurrent operator states whether it preserves input order, is unordered, or preserves order per key.
- Cross-input merge ordering is undefined unless the operator contract supplies an ordering rule.
- Async boundaries and placement boundaries are visible in the graph model and documentation.

### Failure and supervision

- Stop, resume, restart-stage, retry-element, recover-with-value, recover-with-source, and restart-section are not aliases.
- Stateful stage restart specifies which state is reset and which durable state survives.
- Retrying side effects requires an explicit idempotency or delivery contract.

### Identity and serialization

- Named pipelines and durable stages have stable IDs and explicit versions.
- Runtime implementations are resolved from registrations at startup; durable topology contains descriptors and IDs, not delegates or closures.
- Changing the behavior of a registered stateless delegate requires a pipeline version change when that behavior participates in a durable definition.
- Language-specific facades may use delegates while constructing a local definition, but the validated deployable form must resolve them to stable registrations.

## Source ambition

The architecture must be capable of supporting, through core primitives or optional adapters:

- collections, async enumerables, tasks, channels, queues, generators, ticks, and resource unfolding;
- Orleans Streams subscriptions and publications;
- timers and reminders;
- individual grain calls, keyed grain stages, controlled groups of grains, and observer/notification bridges;
- files and .NET streams;
- HTTP polling, request/response and webhook ingress;
- Reactive Streams, `IObservable<T>`, and .NET event bridges where bounded buffering is explicit;
- messaging systems such as Kafka through optional provider packages;
- database change feeds or paged queries through optional provider packages.

This list is a product direction, not a claim that all adapters belong in the core package or in the first milestone.

## Sink ambition

The architecture must support, through core primitives or optional adapters:

- ignore, collect, first/last, fold, asynchronous callbacks, and externally materialized consumers;
- Orleans Streams and grain calls with an explicit acknowledgement/delivery boundary;
- channels, queues, observables, files, and .NET streams;
- databases, Kafka and other brokers;
- HTTP APIs, serverless endpoints, webhooks, and SignalR;
- custom sinks through a stable provider extension contract.

Every adapter must document acknowledgement, batching, retry, ordering, checkpoint, replay, and idempotency behavior.

## F# compatibility goal

The C# implementation must not force the future F# API into C# naming or overload resolution.

The expected F# surface uses typed values and modules:

```fsharp
Source.fromOrleansStream<OrderCreated> sourceOptions
|> Source.via normalizeOrders
|> Source.toSink orderSink
|> Pipeline.define (PipelineId "orders") (PipelineVersion 1)
```

Reusable flow composition should read naturally:

```fsharp
let normalizeOrders : Flow<OrderCreated, OrderDocument> =
    Flow.filter isValid
    |> Flow.andThen (Flow.map OrderDocument.ofEvent)
```

Source options, flow/operator options, and sink options must be separately named and typed. `Task`, `ValueTask`, and F# `Async` operators must be distinct enough for reliable type inference. A computation expression may later provide optional declaration sugar, but it is not the foundation of graph composition.

An optional future `Orleans.Dataflow.OrleansFSharp` package may integrate with the functional grain contracts from Orleans.FSharp specification 003. The core project does not depend on Orleans.FSharp.

## Constraints

- Independent repository and package family.
- C# API and runtime first; F# implementation begins after the C# API and its parity gate are substantially complete.
- Public documentation and examples are in English.
- No package publication, tag, or release before the explicit 1.0.0 decision.
- Direct, frequent, reviewed commits to `main` are the temporary pre-1.0 workflow.
- No production-readiness claims without real multi-silo, failure, recovery, and capacity evidence.
- Prefer explicit contracts and conventional .NET APIs over hidden code generation or public metaprogramming tricks.

## Definition of done for 1.0.0

Version 1.0.0 is eligible for release only when all of these are true:

1. The C# public API is reviewed for naming, nullability, cancellation, diagnostics, generic variance, AOT/trimming impact, and binary compatibility.
2. The approved capability matrix has no unexplained essential gap.
3. Graph validation rejects type, port, identity, cycle, registration, and unsupported durability errors before runtime execution where possible.
4. Backpressure, completion, failure, cancellation, ordering, materialized values, buffers, async boundaries, fan-in/out, supervision, restart, and graph reuse have executable contract tests.
5. Orleans-native sources, sinks, keyed stages, placement boundaries, failover, deactivation/reactivation, and rolling upgrade behavior have multi-silo integration tests.
6. Every shipped adapter states its delivery and idempotency boundary and has fault tests for it.
7. Runtime resources are bounded by default and measured under representative load.
8. OpenTelemetry metrics/traces and diagnostic graph inspection are documented and tested.
9. The F# compatibility constraints are validated against a prototype frontend, even if the separate F# package is scheduled immediately after the C# release candidate.
10. API reference, conceptual documentation, migration/versioning policy, examples, and a clean-room getting-started verification are complete.
11. Package contents, deterministic builds, dependency locks, signing/provenance, and release automation pass their gates.
12. A separate explicit release decision authorizes the first tag and NuGet publication.

Until every applicable criterion is evidenced, the project remains WIP regardless of how much code exists.
