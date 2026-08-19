# Orleans.Dataflow

> Typed dataflow pipelines for [Microsoft Orleans](https://github.com/dotnet/orleans),
> in C# and F#, with bounded memory and runs that survive a restart.

> [!WARNING]
> Early work in progress. The API, the runtime model, and the storage contracts
> are not stable, this is not ready for production, and no package will be
> published before the 1.0.0 readiness criteria are met.

## Why this exists

Processing a stream inside an actor system is easy to start and hard to keep.
The queue in front of a slow consumer grows until the process dies. The work in
flight when a silo is recycled is simply gone. The pipeline lives as a tangle of
`Task`s that nothing can describe, so nothing can move it, version it, or
continue it somewhere else.

Orleans.Dataflow separates **the description of a pipeline from the code that
runs it**. What you build is an immutable document: stages, connections, and
numbers, with no delegates and no CLR types inside it. That one decision is what
makes the rest possible — the same pipeline runs in your process or on a silo,
two pipelines can be compared byte for byte, and a run that dies mid-stream can
be picked up by another process from where it stopped.

Akka.NET Streams is a capability reference rather than an implementation to
port: the contracts here are built around virtual actors, durable identities,
Orleans streams, reminders, grains, placement, and cluster lifecycle.

## Quick start

```csharp
RunnableGraph graph = Source.From(orderEvents)
    .Where(order => order.IsValid)
    .Select(OrderDocument.FromEvent)
    .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> processed);

await using RunHandle run = await new LocalDataflowHost().MaterializeAsync(graph, cancellationToken);

long count = await run.GetValueAsync(processed, cancellationToken);
```

The same pipeline in F# — an equal frontend over the same algebra, not a wrapper
around the C# one:

```fsharp
let graph, processed =
    Source.ofSeq orderEvents
    |> Source.filter (fun order -> order.IsValid)
    |> Source.map OrderDocument.ofEvent
    |> Source.toResult "processed" (Sink.aggregate 0L (fun count _ -> count + 1L))

let! run = LocalDataflowHost().MaterializeAsync(graph, cancellationToken)
let! count = run |> Run.value processed cancellationToken
```

Both build the same document, byte for byte. That is checked on every build
rather than claimed here.

## What you get

- **Bounded memory by construction.** Nothing produces until something asks, so
  a slow consumer slows the producer. Where you want more than one element in
  flight, you say how many — and that number is what the pipeline holds, whether
  the stream is ten elements or ten million.
- **A vocabulary that covers real work.** Mapping, filtering, folding,
  asynchronous calls with a concurrency bound, batching and windowing, rate
  limiting, deduplication, grouping by key with a bound on live keys, and nine
  ways to split and join streams.
- **Failure you decide in advance.** Retry ladders, fallbacks, and what to do
  with an element that has exhausted them — declared in the pipeline, not
  discovered in production.
- **Runs that survive their process.** Name a run, give it a checkpoint cadence,
  and a later process continues it from where it stopped, with the replay window
  stated rather than implied.
- **Two equal frontends.** C# and F# author the same pipelines; neither is a
  translation layer over the other.

## Documentation

**[Start here →](docs/index.md)**

- New to the library: [How it works](docs/concepts/how-it-works.md), then
  [your first pipeline](docs/start/first-pipeline.md).
- Solving something specific: the [guides](docs/index.md#guides).
- Looking up a name: [operators](docs/reference/operators.md),
  [glossary](docs/reference/glossary.md), [errors](docs/reference/errors.md).
- Running it in production: [operations](docs/index.md#operate).

## Running the samples

Eight scenarios, each written twice — once in C#, once in F# — in one console
application that fails if the two authorings disagree:

```bash
dotnet run --project samples/Orleans.Dataflow.Samples            # all of them
dotnet run --project samples/Orleans.Dataflow.Samples -- --list  # what they are
```

## Project status

Pre-1.0, with the state tracked honestly rather than optimistically: the
[capability matrix](docs/project/CAPABILITY-MATRIX.md) records what is proven
and what is deferred, the [roadmap](docs/project/ROADMAP.md) orders the work,
and [benchmarks](docs/BENCHMARKS.md) publishes what the runtime costs with the
grade of those numbers stated beside them.

Until 1.0.0, development happens through frequent reviewed commits directly to
`main`. Pull requests and package publication begin with the 1.0.0 release
process.

## License

Not yet chosen; one will be declared before the 1.0.0 release.
