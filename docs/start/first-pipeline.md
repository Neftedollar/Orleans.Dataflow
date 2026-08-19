# Your first pipeline

After this page you have a [pipeline](../reference/glossary.md#pipeline) that
reads a sequence, filters it, transforms it, and hands you back a number — and
you have written it twice, once in C# and once in F#, and watched both print the
same [fingerprint](../reference/glossary.md#fingerprint). That last part is the
library's central property, and you should meet it now rather than take it on
trust later. Fifteen minutes.

## Before you start

- [ ] A project that references `Orleans.Dataflow` and builds
      ([Installation](installation.md)).
- [ ] Nothing else. This whole page runs in your own process; no silo, no
      cluster, no configuration.

## Step 1 — build a graph

Put this in `Program.cs`:

```csharp
using Orleans.Dataflow;

int[] readings = [3, 8, 2, 9, 4, 7];

RunnableGraph graph = Source.From(readings)
    .To(s => s.Count(), "seen", out ResultSlot<long> seen);
```

Three things just happened, and none of them was work.

`Source.From` made a [Source](../reference/glossary.md#source) — a *value*
describing where elements come from. `.To(...)` closed it with a
[Sink](../reference/glossary.md#sink) and produced a
[RunnableGraph](../reference/glossary.md#runnablegraph), which is also a value:
immutable, comparable, and inert. Nothing has been enumerated, nothing counted.

The `out ResultSlot<long> seen` is how the sink's value gets back to you. A
[result slot](../reference/glossary.md#result-slot) is a named, typed handle —
`"seen"` is the name, `long` is the type — and you hold it until there is a run
to ask.

## Step 2 — run it and read the slot

Add:

```csharp
await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(graph);

Console.WriteLine($"readings seen: {await run.GetValueAsync(seen)}");
Console.WriteLine($"fingerprint:   {graph.Fingerprint}");

await run.Completion;
```

`MaterializeAsync` is the only call on this page that starts anything — that is
what [materialize](../reference/glossary.md#materialize) means. It hands you a
[run handle](../reference/glossary.md#run-handle): the control surface of one
[run](../reference/glossary.md#run).

```console
dotnet run
```

```
readings seen: 6
fingerprint:   sha256:c231c5953148de42871dcf182fe58bf55ff4888d2de435645e2875954ea0b0a8
```

`GetValueAsync` waits for the stream to end before it answers, because a count
is not a count until there is nothing left to count. `run.Completion` is the
[completion](../reference/glossary.md#completion) — the run's outcome as an
awaitable — and awaiting it makes the run's failure your own. The `await using`
disposes the handle, which stops the run and waits for it.

## Step 3 — extend it

Now compose. Add a filter and a map between the source and the sink, and change
the sink to add up what survives:

```csharp
using Orleans.Dataflow;

int[] readings = [3, 8, 2, 9, 4, 7];

RunnableGraph graph = Source.From(readings)
    .Where(reading => reading % 2 == 0)
    .Select(reading => reading * 10)
    .To(s => s.Aggregate(0L, (running, reading) => running + reading), "total", out ResultSlot<long> total);

await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(graph);

long sum = await run.GetValueAsync(total);

await run.Completion;

Console.WriteLine($"total:       {sum}");
Console.WriteLine($"fingerprint: {graph.Fingerprint}");
```

```console
dotnet run
```

```
total:       140
fingerprint: sha256:2a9db67eaccff188a07e25228ab5d106e3ff59bc66a345f4402945a659df2f3c
```

Eight, two and four survive the filter; ten times each is eighty, twenty and
forty; and the fold adds them to a hundred and forty.

Notice what did *not* change. `.Where` and `.Select` each answered a new
`Source<int>`, leaving the old one untouched — a source is a value, so you can
keep it, pass it around, and use it in two pipelines. And the fingerprint *did*
change, because it is a fingerprint of the graph and the graph is different now.

Every sink is a fold underneath, including the ones that look like something
else: `Count()` is a fold that adds one, `Aggregate` is the general case, and
the engine calls that shape a [terminal](../reference/glossary.md#terminal).
You will meet the word in error messages.

## Step 4 — write it again in F#

Make a second project with `dotnet new console -lang F#`. Use the project file
from [Installation](installation.md#step-5--the-same-thing-in-f) verbatim — the
generated one does not build on the .NET 10 SDK, for a reason that has nothing to
do with this library. Then put this in `Program.fs`:

```fsharp
module Program

open System.Threading
open System.Threading.Tasks
open Orleans.Dataflow.FSharp

[<EntryPoint>]
let main _ =
    let readings = [ 3; 8; 2; 9; 4; 7 ]

    let graph, total =
        Source.ofSeq readings
        |> Source.filter (fun reading -> reading % 2 = 0)
        |> Source.map (fun reading -> reading * 10)
        |> Source.toResult "total" (Sink.aggregate 0L (fun running reading -> running + int64 reading))

    task {
        let host = Orleans.Dataflow.LocalDataflowHost()
        let! run = host.MaterializeAsync(graph, CancellationToken.None)
        let! sum = run |> Run.value total CancellationToken.None

        do! run.Completion
        do! run.DisposeAsync()

        printfn "total:       %d" sum
        printfn "fingerprint: %O" graph.Fingerprint
    }
    |> Task.WaitAll

    0
```

The differences are all F# being F#. Operators pipe rather than chain, so the
source is the *last* argument. `Source.toResult` answers a tuple where C# reaches
for an `out` parameter, because F# already has the shape. `Run.value` takes the
run last for the same reason. And `Orleans.Dataflow` itself is deliberately not
opened: its `Source`, `Flow` and `Sink` are the C# spellings of the same
concepts, and two of each name in scope produces errors that do not say so — so
`LocalDataflowHost` is written out in full.

```console
dotnet run
```

```
total:       140
fingerprint: sha256:2a9db67eaccff188a07e25228ab5d106e3ff59bc66a345f4402945a659df2f3c
```

## Step 5 — look at the two fingerprints

```
C#   sha256:2a9db67eaccff188a07e25228ab5d106e3ff59bc66a345f4402945a659df2f3c
F#   sha256:2a9db67eaccff188a07e25228ab5d106e3ff59bc66a345f4402945a659df2f3c
```

Byte for byte the same, from two compilers, two languages and two projects.

This is not a coincidence and it is not cosmetic. What you built in each language
was a [graph document](../reference/glossary.md#graph-document): a JSON
description naming the stages, their connections and their numbers. It carries
no delegate, no closure, no CLR type name. Written in
[canonical JSON](../reference/glossary.md#canonical-json) — one spelling per
meaning — it hashes to one value, and *that* is the fingerprint. Two pipelines
with the same fingerprint are the same pipeline, whoever wrote them and in
whatever language.

Everything else the library can do stands on this. A document that names no
code can be stored, sent to another process, and continued after the process
that wrote it is gone. You will use all three in the next two pages.

## When it does not work

| What you see | What it means |
|---|---|
| `The slot 'seen' belongs to a different graph: it was declared by the document sha256:…, and this is a run of sha256:…` | You built two graphs and read a slot from one against a run of the other. A slot resolves only against a run of the graph that declared it. |
| `The default ResultSlot names no result and cannot be resolved.` | An uninitialized `ResultSlot<T>` field or a discarded `out`. Get a slot by closing a graph with a result-bearing sink. |
| `error CS0619: … is obsolete: 'A result-bearing sink needs a name for its result: write To(s => s.Aggregate(seed, folder), "name") …'` | You wrote `.To(s => s.Count())` with no slot name. A sink that produces a value has to say what to call it. The message spells out all three ways to fix it, including discarding the result on purpose with `.ToSink()`. |
| `dotnet run` prints nothing and exits | You built a graph and never materialized it. Building starts nothing on purpose. |
| The F# fingerprint differs from the C# one | The graphs differ. Compare operator by operator — the numbers in the document (a buffer's capacity, a window's size) are part of it too. |

## What you learned

- Building a graph runs nothing; `MaterializeAsync` is the only call that starts
  a run.
- A `Source` is a value, so composing it gives you a new one and keeps the old.
- A sink's value comes back through a named, typed result slot.
- Every sink is a fold; the engine calls it a terminal.
- The same pipeline in C# and in F# produces the same document and therefore the
  same fingerprint.

Next: [Running on a silo](running-on-a-silo.md) — the same pipeline, executing
somewhere other than the process that wrote it.
