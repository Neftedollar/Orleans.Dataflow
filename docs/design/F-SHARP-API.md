# F# API direction

- Status: M7 in flight (started 2026-08-18, after the M6 gate passed)
- Package: `Orleans.Dataflow.FSharp`

This document was written before any F# existed so that C# and graph-core
decisions could not accidentally make an idiomatic F# API impossible; M7 now
builds against it. The phase order:

1. **M7.1 — seam and first vertical slice.** Friend-assembly access opens the
   four seams the compile prototype proved sufficient (below); the package
   skeleton, the `Source`/`Flow`/`Sink`/`Pipeline` modules in their minimal
   form, and one end-to-end slice — author in F#, materialize on the local
   host, resolve a slot — with the fingerprint-equality invariant as a test:
   an F#-authored graph and its C#-authored twin produce byte-identical
   documents. This phase fixes every convention, so it is authored by the
   architect rather than delegated.
2. **M7.2 — operator breadth** over the algebra: the effect-explicit map
   families, filtering/choosing, batching, timing, rate, deduplication,
   supervision and durable-scope spellings, per the C# vocabulary and this
   document's naming rules.
3. **M7.3 — junctions and the `Graph` module**: typed branches over the
   fragment algebra, the nine junctions, `Fork` diamonds, cycles through
   `GraphFragmentComposer.Wire`.
4. **M7.4 — runs and hosts**: `RunHandle` (completion, watch, snapshot,
   pause), durable options, the Orleans host, registered-stage spellings.
5. **M7.5 — examples, docs authored as F#, and the M7 exit review.**

## Principles

1. F# is an equal authoring frontend over the same immutable graph algebra and definition plane.
2. Public F# composition uses typed values, modules, functions, and `|>`.
3. `Source`, `Flow`, and `Sink` remain visibly distinct concepts.
4. Source configuration, operator/flow configuration, sink configuration, run configuration, and host configuration use different types.
5. Reusable flows and whole graph fragments are first-class values which can be composed.
6. The API avoids overload guessing, mutable builders, optional-argument bags, public SRTP tricks, and broad auto-open modules.
7. No F# function value, closure, record-of-functions, or `Async` workflow is serialized into a durable topology.

## Naming

Public types, records, unions, and interfaces use PascalCase. Module functions and parameters use camelCase. Generic parameter names use meaningful PascalCase names when they improve signatures, for example `'Input`, `'Output`, and `'Result`.

The package should use a namespace and qualified companion modules:

```fsharp
namespace Orleans.Dataflow.FSharp

[<RequireQualifiedAccess>]
module Source = ...

[<RequireQualifiedAccess>]
module Flow = ...

[<RequireQualifiedAccess>]
module Sink = ...

[<RequireQualifiedAccess>]
module Graph = ...

[<RequireQualifiedAccess>]
module Pipeline = ...
```

`[<AutoOpen>]` is not used for the main DSL. Qualified names make it immediately clear whether configuration or an operation belongs to a source, flow, sink, graph, or pipeline.

## Pipeline-friendly argument order

The value being transformed is the final argument. This follows the shape of `List.map`, `Seq.filter`, and other conventional F# modules.

Conceptual signatures:

```fsharp
val Source.via :
    flow: Flow<'Input, 'Output> ->
    source: Source<'Input> ->
        Source<'Output>

val Source.toSink :
    sink: Sink<'Input> ->
    source: Source<'Input> ->
        RunnableGraph

val Flow.andThen :
    next: Flow<'Middle, 'Output> ->
    current: Flow<'Input, 'Middle> ->
        Flow<'Input, 'Output>
```

Stream shapes carry element types only. Materialized results are typed named result slots resolved from a run handle ([ADR 0002](../architecture/0002-result-slots.md)), so no `'Materialized` parameter threads through composition:

```fsharp
let result : ValueTask<'T> =
    runHandle
    |> RunHandle.getValueTask resultSlot
```

The argument order and conceptual separation are stable requirements.

## Basic composition

```fsharp
let normalizeOrders : Flow<OrderCreated, OrderDocument> =
    Flow.filter isValidOrder
    |> Flow.andThen (Flow.map OrderDocument.ofEvent)

let definition =
    Source.fromOrleansStream<OrderCreated> orderSourceOptions
    |> Source.via normalizeOrders
    |> Source.via enrichOrders
    |> Source.toSink orderSink
    |> Pipeline.define
        (PipelineId "orders-to-documents")
        (PipelineVersion 1)
```

`Flow.andThen` is the primary readable function. A symbolic composition operator may be evaluated later, but documentation must not require users to remember it.

## Configuration types

A generic `SourceOptions`, `FlowOptions`, or `SinkOptions` type is insufficient for real adapters. It invites unrelated fields and hides ownership. Prefer specific, stable records:

```fsharp
type OrleansStreamSourceOptions =
    { ProviderName : string
      StreamNamespace : string
      StreamId : StreamId
      StartPosition : StreamStartPosition
      Ingress : IngressBufferOptions }

type ParallelMapOptions =
    { Parallelism : int
      Ordering : ParallelOrdering
      Failure : FailurePolicy }

type HttpSinkOptions =
    { ClientName : string
      MaximumInFlight : int
      Timeout : TimeSpan
      Idempotency : HttpIdempotencyOptions }

type RunOptions =
    { Placement : PlacementOptions
      ResourceLimits : ResourceLimitOptions
      Observability : ObservabilityOptions }
```

Shared small policy records are acceptable, but adapter-specific records own adapter-specific behavior. Record fields use PascalCase; local values use camelCase.

If an option type is expected to evolve incompatibly or must hide invariants, use an opaque type with module constructors rather than exposing a record representation indefinitely.

## Synchronous and asynchronous operators

Do not overload one `mapAsync` function across F# `Async`, `Task`, and `ValueTask`. The effect is visible in the name and signature:

```fsharp
val Flow.map :
    mapping: ('Input -> 'Output) ->
        Flow<'Input, 'Output>

val Flow.mapAsync :
    options: ParallelMapOptions ->
    mapping: ('Input -> Async<'Output>) ->
        Flow<'Input, 'Output>

val Flow.mapTask :
    options: ParallelMapOptions ->
    mapping: ('Input -> CancellationToken -> Task<'Output>) ->
        Flow<'Input, 'Output>

val Flow.mapValueTask :
    options: ParallelMapOptions ->
    mapping: ('Input -> CancellationToken -> ValueTask<'Output>) ->
        Flow<'Input, 'Output>
```

The prototype will validate the final naming against IDE completion and the current F# component guidelines. `ValueTask` is not the default F# effect; it exists only for explicit allocation-sensitive .NET interop.

Cancellation must reach the returned computation. Task expressions do not gain implicit stage cancellation merely because they return `Task`.

Optional mapping also keeps allocation and ergonomics choices explicit:

```fsharp
val Flow.choose : ('Input -> 'Output voption) -> Flow<'Input, 'Output>
val Flow.chooseOption : ('Input -> 'Output option) -> Flow<'Input, 'Output>
```

## Source, flow, and sink construction

Representative direction:

```fsharp
let orderSource =
    Source.fromOrleansStream<OrderCreated> orderSourceOptions

let enrichOrders =
    Flow.mapTask
        { Parallelism = 16
          Ordering = PreserveInputOrder
          Failure = FailurePolicy.stop }
        (fun order cancellationToken ->
            pricingClient.EnrichAsync(order, cancellationToken))

let orderSink =
    Sink.toOrleansStream<OrderDocument> orderSinkOptions
```

The option type names and function qualification make ownership clear at every point. A source option cannot be passed to a sink merely because both have a property called `BufferSize`.

## Whole-graph composition

Linear convenience functions are not the complete graph model. The future `Graph` module must expose typed port/shape values for fan-in, fan-out, reusable subgraphs, and named result slots.

Graph composition follows the same rules:

- immutable input values and a new output value;
- typed ports;
- stable import scopes for reusable fragments;
- no hidden execution;
- no custom-operation computation expression as the only way to represent topology.

A computation expression may later be added as optional syntax for declarations, particularly when it improves branching layout. It cannot own the graph semantics or make a topology inexpressible through ordinary typed functions.

## Orleans.FSharp specification-003 integration

An optional `Orleans.Dataflow.OrleansFSharp` package may adapt functional grain contracts without making them part of the core dependency graph.

The adapter must preserve:

- the phantom actor-brand type;
- the domain key and its codec;
- the user-authored API record whose fields are functions returning `Task`;
- selector validation and operation identity from the contract.

Conceptual direction:

```fsharp
val FunctionalGrainFlow.callBy :
    stageId: StageId ->
    contract: GrainContract<'Actor, 'Key, 'Api> ->
    keyOf: ('Input -> 'Key) ->
    invoke: ('Api -> 'Input -> Task<'Output>) ->
    options: KeyedCallOptions ->
        Flow<'Input, 'Output>
```

The phantom `'Actor` parameter must not be erased: it prevents a structurally similar API record from binding to the wrong functional grain contract. Bound API records are runtime values and are never persisted in the graph definition.

## Interop boundary

The F# package is optimized for F# callers. The main C# package remains the .NET-friendly surface. Do not contort F# functions into `System.Func`, tupled OO methods, or C# overload families merely to create a second C# facade.

**Binding rule.** The F# frontend binds to the shared, language-neutral
layers — the fragment algebra (`Orleans.Dataflow.Authoring.GraphFragment`
and `GraphFragmentComposer`), the definition plane, and the shared value
types (`RunnableGraph`, `ResultSlot<T>`, `GraphDocument`, fingerprints) —
and NEVER to the C# fluent facade. `Source<T>.Via(...)` is one frontend's
spelling over the algebra; wrapping it from F# would import every C#-ism
(instance-method chaining, `Func<>` conversions, `out` parameters,
overload families) into a package whose entire reason to exist is not
having them. A C# fluent API can never be idiomatic F#; the algebra can
serve both because it is functions over immutable values. Where a piece the
F# facade needs is currently internal to the C# package (the local stage
vocabulary and binding-table types), the answer at M7 is a language-neutral
seam: public when it is a genuine extension contract third parties should
also build against, or friend-assembly access (`InternalsVisibleTo` to the
F# package) when the surface should stay private to the package family —
the two packages ship from one repository in lockstep, so the friend
coupling is safe. What is never acceptable is a detour through the C#
fluent types.

Where types cross the shared graph core, use CLR-neutral opaque types and immutable descriptors. Keep F# records and discriminated unions inside the F# package unless their .NET representation and compatibility policy are deliberately public.

### Seam inventory (from the compile prototype, 2026-08-16)

An algebra-bound F# facade was built against the real assembly and produced
byte-identical documents to the C# fluent facade (fingerprints equal,
runtime-verified), proving the algebra is the sufficient shared substrate.
What the facade could NOT do, and therefore what the M7 seam must open
(friend-assembly access was proven to cover all of it):

1. construct `RunnableGraph` (internal constructor) — `Source.toSink` must
   return the shared closed-graph value;
2. mint `ResultSlot<'T>` with the authoring nonce (internal factory; the
   nonce is the third slot component — without it F# graphs would be
   strictly less safe than C# ones);
3. populate the local binding table (`LocalStageDescriptor` and friends are
   internal — the deepest blocker: an F#-authored lambda graph is
   un-materializable by construction);
4. share the local vocabulary and closure rules (`LocalVocabulary`,
   `LocalGraphBuilder`) — the prototype re-derived them by hand and matched
   byte-for-byte, but nothing keeps two copies agreeing; the seam must make
   drift impossible, not unlikely.

Findings that constrain the C# surface, proven by compilation: keep
`SinkWithResult.ToSink()` (F# has no spelling for a user-defined explicit
conversion); do not mirror `SinkFactory<'T>` in F# (it patches a C#-only
inference hole — F# infers the plain `Sink.Aggregate` call fine); the F#
facade uses one named function per operation, never overload families
(overloads are what degrade F# diagnostics to an FS0041 candidate dump);
the camelCase module-function convention is load-bearing, because an F#
module binding silently shadows a same-named C# static method; identity
structs need non-generic `System.IComparable` for F# `Set`/`Map` keying —
the F# `comparison` constraint requires exactly that interface, not
`IComparable<'T>` (proven by an FS0001 probe; both are now implemented,
explicitly, so no public member is added); and samples must
write `string value`, never `.ToString()`, on the readonly structs
(`FS0052` defensive-copy warning under warnings-as-errors).

## Expensive mistakes to avoid

- Making the F# package a file of extension methods over C# fluent overloads.
- Calling unrelated source, operator, sink, and runtime records all “options.”
- Using `sync`/`source`/`sink` abbreviations that look similar in completion lists.
- Using a computation expression as the only composition mechanism.
- Hiding `Task`, `ValueTask`, and `Async` behind SRTP or overload inference.
- Serializing F# functions, `Async` workflows, or records of bound grain functions.
- Erasing stable stage IDs or Orleans.FSharp phantom actor brands.
- Making reusable flow composition mutate or share one runtime stage instance.

## Primary references

- [F# component design guidelines: naming](https://learn.microsoft.com/en-us/dotnet/fsharp/style-guide/component-design-guidelines#naming-conventions)
- [F# object, type, and module design](https://learn.microsoft.com/en-us/dotnet/fsharp/style-guide/component-design-guidelines#object-type-and-module-design)
- [F# coding conventions](https://learn.microsoft.com/en-us/dotnet/fsharp/style-guide/conventions)
- [F# asynchronous programming](https://learn.microsoft.com/en-us/dotnet/fsharp/tutorials/async)
- [F# task expressions](https://learn.microsoft.com/en-us/dotnet/fsharp/language-reference/task-expressions)
- [F# records](https://learn.microsoft.com/en-us/dotnet/fsharp/language-reference/records)
- [Orleans.FSharp specification 003](https://github.com/Neftedollar/orleans-fsharp/blob/main/specs/003-functional-grain-runtime/spec.md)
