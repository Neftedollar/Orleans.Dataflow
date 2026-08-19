# How it works

*What are the moving parts, and why are there three of them?*

Ten minutes, no code to write. Read this before you build anything, and the rest
of the documentation will read as consequences rather than as rules.

## A pipeline is a value

In most streaming libraries, "building a pipeline" and "starting a pipeline" are
the same act: you wire something up and it begins to move. Here they are two
acts, separated on purpose, and almost everything else follows from that
separation.

```csharp
RunnableGraph graph = Source.From(orderEvents)
    .Where(order => order.IsValid)
    .Select(OrderDocument.FromEvent)
    .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> processed);
```

Nothing has happened. No thread exists, no connection is open, no element has
moved. `graph` is a value — an immutable one — that you can hold in a field, pass
to a method, compare with another, write to a log, or use three times. The same
four lines in F# produce the same value:

```fsharp
let graph, processed =
    Source.ofSeq orderEvents
    |> Source.filter (fun order -> order.IsValid)
    |> Source.map OrderDocument.ofEvent
    |> Source.toResult "processed" (Sink.aggregate 0L (fun count _ -> count + 1L))
```

Running it is a separate call, and it is the only call that starts anything:

```csharp
await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(graph, cancellationToken);

long processedOrders = await run.GetValueAsync(processed, cancellationToken);

await run.Completion;
```

> Both snippets are the `first-pipeline` scenario of the sample application, from
> [`samples/Orleans.Dataflow.Samples/CSharp/FirstPipeline.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/FirstPipeline.cs)
> and
> [`samples/Orleans.Dataflow.Samples.FSharp/FirstPipeline.fs`](../../samples/Orleans.Dataflow.Samples.FSharp/FirstPipeline.fs).
> Both run in continuous integration, and the runner checks that they build the
> same thing.

## That value is a document

Here is the part that surprises people. The value you built is not a graph of
objects holding your delegates. It is a **document** — JSON, with a fixed
spelling — describing stages, the connections between them, and numbers. Print
the one those four lines built and you get this — line breaks added for reading,
and each node's parameter-contract members elided at the `…`; the real thing is
one line with no whitespace at all:

```json
{
  "formatVersion": 1,
  "graphId": "anonymous",
  "revision": 1,
  "capabilities": ["ephemeral-identity", "nondeployable"],
  "nodes": [
    { "nodeId": "stage-0001", "stageRef": { "providerId": "local", "stageId": "from-enumerable", "majorVersion": 1 }, "parameters": {}, … },
    { "nodeId": "stage-0002", "stageRef": { "providerId": "local", "stageId": "where",           "majorVersion": 1 }, "parameters": {}, … },
    { "nodeId": "stage-0003", "stageRef": { "providerId": "local", "stageId": "select",          "majorVersion": 1 }, "parameters": {}, … },
    { "nodeId": "stage-0004", "stageRef": { "providerId": "local", "stageId": "fold",            "majorVersion": 1 }, "parameters": {}, … }
  ],
  "edges": [
    { "from": { "nodeId": "stage-0001", "portId": "out" }, "to": { "nodeId": "stage-0002", "portId": "in" } },
    { "from": { "nodeId": "stage-0002", "portId": "out" }, "to": { "nodeId": "stage-0003", "portId": "in" } },
    { "from": { "nodeId": "stage-0003", "portId": "out" }, "to": { "nodeId": "stage-0004", "portId": "in" } }
  ],
  "resultSlots": [
    { "resultSlotId": "processed", "resultContract": { "contractId": "local-fold-result", "majorVersion": 1 }, "producer": { "nodeId": "stage-0004", "portId": "result" } }
  ]
}
```

Read what is there and, more importantly, what is not. There is a node saying
"a `where` stands here". There is **no predicate**. There is a node saying "a
`select` stands here", and no projection. There is no CLR type name anywhere —
not `OrderEvent`, not `OrderDocument`. There is no connection string, no grain
reference, no service provider, no task.

**So where did your predicate go?** Beside the document, not inside it — and
this is the first thing to understand about the value you built. A
`RunnableGraph` is *two* things held together: the document above, and a table
of everything your code handed over, keyed by node identifier. The lambda
`order => order.IsValid` sits in that table under the `where` node, the
projection under the `select` node, and — this catches people — the
`orderEvents` sequence itself sits there under the source node. It is not only
code that stays outside. Your **data** stays outside too: the document says "a
sequence is read here", never which sequence, and never its elements.

That table lives in your process and travels nowhere. The document, meanwhile,
travels anywhere. The `nondeployable` token in the capabilities list above is the
document saying exactly that about itself: *some of my stages have behaviour that
only the process that built me can supply, so do not send me to a silo and expect
me to run.* Its neighbour `ephemeral-identity` says the same thing about names:
*my node names are positions I made up — `stage-0001`, `stage-0002` — rather than
names an author chose, so nothing durable can point at them.*

If that sounds like the design defeating itself, hold on: it is the honest
half of a trade, and [what the separation costs you](#what-the-separation-costs-you)
is where the other half is paid. First, what the document does hold.

The [graph document](../reference/glossary.md#graph-document) holds three kinds
of thing and only these three:

- **stages**, each named by a [stage kind](../reference/glossary.md#stage-kind)
  like `where` or `buffer` — a name in a vocabulary, never a type;
- **connections**, each an edge from one stage's output port to another's input
  port;
- **numbers and enumerations**, in each stage's parameter payload. A buffer's
  capacity is in the document; a buffer's *contents* never are. Change the
  capacity from 16 to 8 and you have a different document.

## Three things, and why each exists

So there are three separate things in play, and they have to be separate.

| What | What it is | Why it cannot be one of the others |
|---|---|---|
| **The authoring API** | `Source<T>`, `Flow<TIn,TOut>`, `Sink<T>`, the operators, `Branch<T>` — typed values in C# or F# | Types are how you catch a mistake at compile time, and types are exactly what cannot be written down in a portable document. |
| **The document** | The JSON above | It is what can be stored, hashed, compared, sent to another process, and read by a runtime that has never seen your assembly. A tree of objects holding delegates can be none of those. |
| **The engine** | Threads, channels, demand, the thing that actually moves elements | It is created fresh for every [run](../reference/glossary.md#run). Two runs of one document share nothing, which is only possible because the document holds no run-specific state. |

The authoring API compiles into the document. The engine is built *from* the
document — it looks each stage up by name in a [catalog](../reference/glossary.md#catalog)
that the host registered at startup, and builds an executor for it. A document
naming a stage the host does not know is refused by name before a single element
moves, rather than failing halfway through.

## What the separation buys you

Three things, and each is worth the price.

**The same pipeline runs locally or on a silo.** The document is the whole
description. Hand it to `LocalDataflowHost` and it runs in your process; hand it
to a [silo](../reference/glossary.md#silo) and it runs there. The engine on the
silo is the same engine, given the same document. Nothing about the pipeline
changes; only who is holding it.

**Two pipelines can be compared byte for byte.** The document has exactly one
spelling — members in a fixed order, one form for every number, no insignificant
whitespace. So the SHA-256 of its bytes is an *identity*, not a checksum: two
pipelines with the same [fingerprint](../reference/glossary.md#fingerprint) are
the same pipeline. This is not a theoretical property. The sample application
authors every scenario twice, once in C# and once in F#, and asserts that the two
fingerprints are equal — which is how a frontend that had drifted apart would be
caught by a build rather than by a user:

```text
fingerprints
graph        identical   csharp                    fsharp
main         yes         sha256:30acffef99d8…      sha256:30acffef99d8…
```

That is the run above, printed by the sample application. The two frontends
built the same 32 bytes.

**A dead run can be picked up elsewhere.** A run that writes its position into a
store writes the position *and* the document, both as bytes. When the process
holding the run dies, another process reads both back, rebuilds the engine from
the document, restores the position, and continues. It never needed the original
process's memory, because nothing that mattered was in it.

## What the separation costs you

Now the honest half, because there is no free lunch here.

**A lambda cannot travel.** `Where(order => order.IsValid)` is a delegate. A
delegate is a pointer into a specific assembly loaded into a specific process
with specific captured state; there is no correct way to write one into a JSON
document and there never will be. So a graph built from lambdas is
**local-only**, and it says so about itself. Ask for a deployable
[pipeline](../reference/glossary.md#pipeline) and you are refused, in words:

```text
This graph cannot become a PipelineDefinition because it breaks 2 deployability invariants:
1. it declares the capability 'ephemeral-identity', which says its node identifiers are
   positions rather than names, so nothing durable could be anchored to them; every
   occurrence of a pipeline is named by its author.
2. it declares the capability 'nondeployable', which says a stage's behavior is bound in
   this process and reaches no document, so nothing else could ever materialize it; every
   stage of a pipeline resolves from a catalog.
```

The refusal is deliberate and it is not a caveat you can suppress. The library
never tries to serialize a closure, never falls back to reflection over a type
name, and never lets a pipeline claim it is durable when it is not.

**The way out is to register the stage by name.** You write the same code, hand
it to the host at startup under a name, and refer to it by that name when you
author. Now the behavior is on the host and the document names it — so the
document is complete, and it can go anywhere the host is:

```csharp
RunnableGraph graph = Source
    .FromRegistered(SampleVocabulary.Feed, "feed", SampleVocabulary.FeedParameters(orders))
    .Via(SampleVocabulary.Discount, "discount", SampleVocabulary.DiscountParameters(DiscountPercent))
    .To(SampleVocabulary.Tally, "tally", SampleVocabulary.TallyParameters("accepted-orders", MinimumAmount),
        "accepted", out ResultSlot<long> _);

PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create("sample-orders"), GraphRevision.Create(1));
```

> From the `cluster` scenario,
> [`samples/Orleans.Dataflow.Samples/CSharp/Cluster.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/Cluster.cs).
> Every stage is registered and every [occurrence](../reference/glossary.md#occurrence)
> is named by the author — `"feed"`, `"discount"`, `"tally"` — which is what
> makes the document complete.

The cost is real: registration is a step, and you pay it before a pipeline can
leave your process. What it buys is that "this pipeline is deployable" is a
property the compiler and the document agree on, rather than a hope.

## A one-paragraph tour of running

Materializing turns a document into a running engine and hands you a
[run handle](../reference/glossary.md#run-handle). The engine pulls: the sink
asks the stage before it for an element, which asks the one before that, all the
way to the source, so a fast source can never outrun a slow sink. Values come
back through named [result slots](../reference/glossary.md#result-slot) —
`processed` in the first snippet — which you read from the handle. The run ends
in one of four ways: the source ran out, you shut it down, you cancelled it, or
something threw. Each does something different to your results, and the
difference matters.

## Where to go next

| If you want to know | Read |
|---|---|
| Why a fast producer never floods a slow consumer, and where memory actually goes | [Pull and backpressure](pull-and-backpressure.md) |
| What a pipeline *is*, exactly, and what makes two of them the same | [Graphs and identity](graphs-and-identity.md) |
| How a run ends, and how you get a value out of it | [Runs and results](runs-and-results.md) |
| What changes when a pipeline stops being a line | [Branching](branching.md) |
| What happens when your code throws, and what you can declare in advance | [Failure and supervision](failure-and-supervision.md) |
| What it takes for a run to survive its own process | [Durability](durability.md) |
| What runs where on a cluster, and who is allowed to call what | [The cluster model](cluster-model.md) |

If you would rather build something now: [Your first pipeline](../start/first-pipeline.md)
is a complete program, and [`samples/`](../../samples) is eight of them.
Every term this page used is defined in the [glossary](../reference/glossary.md).
