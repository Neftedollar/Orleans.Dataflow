# Authoring in F#

You write F#, and you want the pipeline to read like F# — pipe-first, no `out`
parameters, no fluent chain of instance methods — without giving up anything the
C# frontend can do.

`Orleans.Dataflow.FSharp` is a first-class frontend, not a wrapper. It binds to
the same graph algebra the C# facade binds to, which is why the two produce the
same [graph document](../reference/glossary.md#graph-document), byte for byte.

## The same pipeline, twice

C#, from
[`samples/Orleans.Dataflow.Samples/CSharp/FirstPipeline.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/FirstPipeline.cs):

```csharp
RunnableGraph graph = Source.From(orderEvents)
    .Where(order => order.IsValid)
    .Select(OrderDocument.FromEvent)
    .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> processed);

await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(graph, cancellationToken);

long processedOrders = await run.GetValueAsync(processed, cancellationToken);

await run.Completion;
```

F#, from
[`samples/Orleans.Dataflow.Samples.FSharp/FirstPipeline.fs`](../../samples/Orleans.Dataflow.Samples.FSharp/FirstPipeline.fs):

```fsharp
let graph, processed =
    Source.ofSeq orderEvents
    |> Source.filter (fun order -> order.IsValid)
    |> Source.map OrderDocument.ofEvent
    |> Source.toResult "processed" (Sink.aggregate 0L (fun count _ -> count + 1L))

let host = Orleans.Dataflow.LocalDataflowHost()

// A run handle is IAsyncDisposable, which `use!` binds and disposes at the end of the scope —
// on the way out of an exception as well as on the way out of the last line. Disposing stops
// the run and waits for it to be stopped.
use! run = host.MaterializeAsync(graph, cancellationToken)

let! count = run |> Run.value processed cancellationToken

do! run.Completion
```

Both are run in the same process by the sample application, which compares them
and exits non-zero if they disagree. They do not:

```text
fingerprints
graph   identical   csharp                                                                  fsharp
main    yes         sha256:30acffef99d8c77ba9db1a737f60e3f9c552276b44156814d92af51a9d63db37 sha256:30acffef99d8c77ba9db1a737f60e3f9c552276b44156814d92af51a9d63db37

observations
name                      agree   csharp   fsharp
orders-in-the-feed        yes     12       12
orders-the-filter-kept    yes     9        9
```

All eight sample scenarios are authored twice and checked this way, including the
cluster one and the durable one. **The authoring language stops mattering at the
document**: a pipeline written in F# is bytes a silo runs without knowing.

## The module layout

Seven modules, all `[<RequireQualifiedAccess>]`, none auto-opened. Qualified names
make it immediately clear whether an operation belongs to a source, a flow, a
sink, or a branch.

| Module | What it holds |
|---|---|
| `Source` | Construction (`ofSeq`, `ofAsyncEnumerable`, `queue`, `tick`, `unfold`, `ofRegistered`, …) and everything that composes *from* a source — the operators, the junctions, and the closing functions. |
| `Flow` | The same operators as reusable `Flow<'In,'Out>` values, plus `andThen`. |
| `Sink` | Terminals: `forEach`, `aggregate`, `collect`, `count`, `first`, `last`, `ignore`, `toChannel`, and their asynchronous forms. |
| `Branch` | Closing one leg of a junction: `toSink`, `toResult`, `toRegistered`, `toRegisteredResult`. |
| `Fork` | Junction outputs that carry on as flows: `zip`, `zipWith`. |
| `Pipeline` | One function: `define`. |
| `Run` | One function: `value`. |

Two of those are one function each on purpose. A run handle and a pipeline
definition are public runtime surface with no receiver-threading to smooth over
and no `out` parameter, so `run.Completion`, `run.WatchTermination`,
`run.Snapshot()`, `run.ShutdownAsync()`, `pipeline.Fingerprint`, and
`pipeline.ResultSlot(...)` read perfectly well as members. A module function per
member would be a second name in a completion list for the identical call.

There is deliberately no `Graph` module. Every junction is a function of a source
or of a branch, exactly where the C# facade puts it, and a second namespace would
be a second place to look for the same operation.

## How a pipeline reads

The value being transformed is the **final** argument throughout, following
`List.map` and `Seq.filter`, so a graph reads top to bottom under `|>`:

```fsharp
val Source.via : flow: Flow<'Input,'Output> -> source: Source<'Input> -> Source<'Output>
val Source.toSink : sink: Sink<'Input> -> source: Source<'Input> -> RunnableGraph
val Flow.andThen : next: Flow<'Middle,'Output> -> current: Flow<'Input,'Middle> -> Flow<'Input,'Output>
```

A flow is a value with nothing attached to either end, and keeping one in a
variable and using it in three pipelines is the intended thing to do:

```fsharp
let normalizeOrders : Flow<OrderCreated, OrderDocument> =
    Flow.filter isValidOrder
    |> Flow.andThen (Flow.map OrderDocument.ofEvent)
```

Effects are visible in the name rather than resolved by overloads —
`Flow.map`, `Flow.mapAsync`, `Flow.mapTask`, `Flow.mapValueTask`, each with an
`Unordered` sibling — because an overload family is what degrades F# diagnostics
to a candidate dump. There is one named function per operation, always.

## A deployable pipeline

The registered vocabulary has its own spellings, and `Pipeline.define` takes the
text and the number you write rather than the two identity structs. From
[`samples/Orleans.Dataflow.Samples.FSharp/Cluster.fs`](../../samples/Orleans.Dataflow.Samples.FSharp/Cluster.fs):

```fsharp
let graph, _ =
    Source.ofRegistered SampleVocabulary.Feed "feed" (SampleVocabulary.FeedParameters orders)
    |> Source.viaRegistered
        SampleVocabulary.Discount
        "discount"
        (SampleVocabulary.DiscountParameters discountPercent)
    |> Source.toRegisteredResult
        "accepted"
        SampleVocabulary.Tally
        "tally"
        (SampleVocabulary.TallyParameters "accepted-orders" minimumAmount)

let pipeline = graph |> Pipeline.define lineage revision
let accepted = pipeline.ResultSlot("accepted", SampleVocabulary.TallyContract)

use! run = sample.Cluster.MaterializeAsync(pipeline, cancellationToken)
let! ending = run.WatchTermination
let! tally = run.GetValueAsync(accepted, cancellationToken)
```

The hosts are **not** wrapped, and that is the same decision as `Run` and
`Pipeline` having one function each: past `AsPipeline` the authoring language is
invisible, so `OrleansDataflowHost` and `LocalDataflowHost` are called directly.
`MaterializeAsync`, `MaterializeDurableAsync`, `MaterializeFromCheckpointAsync`,
`Control<'T>`, and `DurableRunOptions` are all written straight from F#.

`DurableRunOptions` takes F#'s property-initialiser syntax without help, and a
`Nullable` where the C# spelling has an `int?` — from
[`Durable.fs`](../../samples/Orleans.Dataflow.Samples.FSharp/Durable.fs):

```fsharp
let durable () =
    Orleans.Dataflow.DurableRunOptions(
        Store = store,
        RunId = runId,
        EveryElements = Nullable everyElements
    )
```

## The five things you will hit in the first ten minutes

### 1. Do not `open Orleans.Dataflow`

```fsharp
open Orleans.Dataflow.FSharp        // yes
// open Orleans.Dataflow            // no
```

`Orleans.Dataflow` has its own `Source`, `Flow`, and `Sink` — the C# facade's
spellings of the very concepts this package's modules author with. Opening it
puts two of each name in scope, the later `open` wins for the *type* names, and
the error you get does not mention the cause:

```fsharp
open Orleans.Dataflow.FSharp
open Orleans.Dataflow                                    // the mistake

let s : Source<Order> = Source.ofSeq [ { Id = "a" } ]
```

```text
error FS0001: This expression was expected to have type    'Source<Order>'
              but here has type    'Source<'a>'
error FS0001: This expression was expected to have type    'Orleans.Dataflow.Source<Order>'
              but here has type    'Orleans.Dataflow.FSharp.Source<Order>'
```

The first line is the one you read, and it looks like a generics problem. The
second line is the answer. Note also that the *functions* still resolve — a file
that never writes a `Source<_>` annotation compiles happily with both namespaces
open, so the mistake can sit there until the day you add a type annotation.

Everything you need from that namespace is written out in full instead:
`Orleans.Dataflow.LocalDataflowHost()`, `Orleans.Dataflow.DurableRunOptions(...)`,
`Orleans.Dataflow.BufferOptions(...)`.

Every file in the F# sample carries a comment saying exactly this, which is a fair
measure of how often it comes up.

### 2. `use!` for a run handle, not `let!`

A run handle is `IAsyncDisposable`. `use!` binds it and disposes it at the end of
the scope, and disposing stops the run and waits for it to be stopped:

```fsharp
use! run = host.MaterializeAsync(graph, cancellationToken)

do! run.Completion
```

Write it that way rather than with a trailing `do! run.DisposeAsync()`, because
the hand-written form only disposes on the path that reaches it:

```fsharp
let! run = host.MaterializeAsync(graph, cancellationToken)
// anything here that throws …
do! run.DisposeAsync()          // … never runs, and the run stays alive
```

`use!` disposes on the exception path too. The one time to write the disposal out
is when the *order* matters — a durable run whose handle has to be gone before a
second host takes the same run identity up cannot wait for the end of the scope.
Say so in a comment where you do it.

### 3. Tuples where C# has `out`

`Source.toResult` and `Source.toRegisteredResult` answer `RunnableGraph * ResultSlot<'T>`.
F# already has the shape C# needs a keyword to express:

```fsharp
let graph, processed = Source.ofSeq orders |> Source.toResult "processed" Sink.count
```

Discard the slot with `_` when you do not want it — a cluster pipeline recovers
its slot from the pipeline rather than from the closing call, because a closed
graph's slot binds to that built instance while a pipeline's binds to the
fingerprint and the lineage.

### 4. Annotate, do not pass type arguments

Module functions cannot take explicit type arguments across assemblies (`FS0686`).
Where the element type appears only in the answer, write it as an annotation:

```fsharp
let orders : Source<Order> = Source.queue options "orders"
let nothing : Source<Order> = Source.failed (exn "no feed")
```

Generic *values* are fine as they are — `Source.empty<int>` works.

### 5. `string value`, never `.ToString()`

The identity types are readonly structs, and calling an instance method on one
takes a defensive copy — `FS0052`, which is an error under warnings-as-errors:

```fsharp
Observation.Of("run-status", string snapshot.Status)     // yes
// Observation.Of("run-status", snapshot.Status.ToString())   // FS0052
```

One more of the same family: `Sink.FirstOrDefault<T>()` and `LastOrDefault<T>()`
on the C# facade are uncallable from F# with a value type, because
`SinkWithResult<T, T?>` over unconstrained `T` asks F# to form `int | null`
(`FS3265`). The F# module's own `Sink.firstOrDefault` and `Sink.lastOrDefault`
are typed `SinkWithResult<'T,'T>` and avoid it — use those.

## Both frontends produce the same document

This is not an aspiration the packages try to hold; it is what the shared
substrate makes true. The F# frontend binds to the fragment algebra, the
definition plane, and the shared value types — `RunnableGraph`, `ResultSlot<'T>`,
`GraphDocument`, the fingerprints — and **never** to the C# fluent facade. A
fluent API cannot be idiomatic F#; the algebra serves both because it is
functions over immutable values.

Two things follow that are worth relying on:

- **A graph authored in either language has the same
  [fingerprint](../reference/glossary.md#fingerprint)**, so a durable run started
  by a C# process is continued by an F# one, and a pipeline whose author switched
  languages is the same pipeline.
- **Nothing F# ever reaches a document.** No function value, no closure, no
  record of functions, no `Async` workflow is serialised into a topology — which
  is the same rule the C# frontend lives under, for the same reason.

Testing this is the sample application's whole job, and it runs in continuous
integration.

## Next

- [Operators](../reference/operators.md) — every operator, C# and F# spellings side by side.
- [Your first pipeline](../start/first-pipeline.md) — the tutorial, in both languages.
- [Writing a custom stage](custom-stages.md) — a catalog is a published artifact rather than a language artifact, which is why the sample's is written in F# and consumed from C#.
- [Durable runs](durable-runs.md) — the F# spelling of durability, and the `Nullable` it needs.
