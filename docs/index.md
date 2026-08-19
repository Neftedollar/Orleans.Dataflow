# Orleans.Dataflow documentation

Orleans.Dataflow is a typed dataflow library for [Microsoft Orleans](https://github.com/dotnet/orleans).
You describe a pipeline — where elements come from, what happens to them, where
they end up — and the library runs it, in your process or across a cluster,
without unbounded memory and without losing track of where it got to.

It has two frontends, C# and F#, and they are equals: the same pipeline written
in either produces the same description, byte for byte.

> **Before 1.0.** The API, the runtime model, and the storage contracts are not
> stable yet, and no package is published. Read
> [what is supported](reference/compatibility.md) before depending on anything
> here.

## Where to start

**Never seen this library.** Read [How it works](concepts/how-it-works.md) — ten
minutes, no code, and everything afterwards will make sense. Then build
something: [Your first pipeline](start/first-pipeline.md).

**Want it running now.** [Installation](start/installation.md) →
[Your first pipeline](start/first-pipeline.md) →
[Running on a silo](start/running-on-a-silo.md) →
[Surviving a crash](start/surviving-a-crash.md). About an hour end to end, and
every step is a program that runs.

**Have a specific problem.** Go to the [guides](#guides) — each one solves one
task and shows the whole program.

**Looking up a name.** The [operator reference](reference/operators.md) lists
every operator in both languages; the [glossary](reference/glossary.md) defines
every term the library uses; the [error reference](reference/errors.md) says what
each exception means and what to do about it.

**Running this in production.** Start with [Deploying](operations/deploying.md),
then the [runbooks](operations/runbooks.md).

**Prefer to read code.** [`samples/`](../samples) is a console application whose
eight scenarios are each written twice, once in C# and once in F#. It runs in
continuous integration, so nothing in it is out of date.

## Learn

Four short tutorials, in order. Each one is a complete program you can run.

| Page | What you end up with |
|---|---|
| [Installation](start/installation.md) | The packages referenced and a project that builds. |
| [Your first pipeline](start/first-pipeline.md) | A pipeline that reads a sequence, transforms it, and gives you back a number. |
| [Running on a silo](start/running-on-a-silo.md) | The same pipeline executing inside an Orleans silo instead of your process. |
| [Surviving a crash](start/surviving-a-crash.md) | A run that keeps its place across a process death and continues where it stopped. |

## Understand

Why the library is shaped the way it is. Read these when something surprises
you — the answer is usually a decision, and these pages are where the decisions
are explained.

| Page | The question it answers |
|---|---|
| [How it works](concepts/how-it-works.md) | What are the moving parts, and why are there three of them? |
| [Pull and backpressure](concepts/pull-and-backpressure.md) | Why does a fast producer never flood a slow consumer? |
| [Graphs and identity](concepts/graphs-and-identity.md) | What exactly is a pipeline, and what makes two of them "the same"? |
| [Runs and results](concepts/runs-and-results.md) | How does a running pipeline end, and how do you get a value out of it? |
| [Branching](concepts/branching.md) | What happens to ordering, completion, and memory when a pipeline stops being a line? |
| [Failure and supervision](concepts/failure-and-supervision.md) | What happens when your code throws, and what can you say about it in advance? |
| [Durability](concepts/durability.md) | What does it mean for a run to survive a crash, and what does it cost? |
| [The cluster model](concepts/cluster-model.md) | What runs where, who owns what, and who is allowed to call what? |

## Guides

One task per page, each with the whole program rather than a fragment.

| Page | Task |
|---|---|
| [Bounding memory](guides/bounding-memory.md) | Keep a fast source and a slow sink in the same pipeline without unbounded growth. |
| [Doing asynchronous work](guides/async-work.md) | Call something slow per element, with a concurrency bound you choose. |
| [Branching and joining](guides/branching-and-joining.md) | Send one stream to several places, or combine several into one. |
| [Windows and keys](guides/windows-and-keys.md) | Group elements by count, by time, or by a key, with the memory bounded. |
| [Handling failure](guides/handling-failure.md) | Retry, fall back, drop, or fail — and say which in advance. |
| [Durable runs](guides/durable-runs.md) | Give a run a name and a checkpoint cadence so it survives a restart. |
| [Orleans streams and grains](guides/orleans-integration.md) | Read from and write to Orleans streams, call grains from a pipeline. |
| [Writing a custom stage](guides/custom-stages.md) | Make your own source, flow, or sink deployable to a silo. |
| [Authoring in F#](guides/fsharp.md) | The F# frontend, and how it differs from the C# one. |
| [Testing and observability](guides/testing-and-observability.md) | Test a pipeline deterministically; see what a run is doing in production. |

## Look up

| Page | Contents |
|---|---|
| [Glossary](reference/glossary.md) | Every term, defined. |
| [Operators](reference/operators.md) | Every operator, C# and F# spellings side by side. |
| [Options](reference/options.md) | Every options type, every field, every default. |
| [Errors](reference/errors.md) | Every exception, what causes it, what to do. |
| [Run handles](reference/run-handles.md) | Every member of the local and cluster handles. |
| [Hosting](reference/hosting.md) | Registration, silo and client builders, host methods. |
| [Adapters](reference/adapters.md) | Every source and sink adapter with its delivery guarantee. |
| [Provider SDK](reference/provider-sdk.md) | The seam a custom stage plugs into. |
| [Compatibility](reference/compatibility.md) | Supported .NET, Orleans, and F# versions; what the API guarantee covers. |

## Operate

| Page | Contents |
|---|---|
| [Deploying](operations/deploying.md) | What a silo and a client need, and what the deployment owes the library. |
| [Checkpoint stores](operations/checkpoint-stores.md) | The contract a store must honor, and how to implement one. |
| [Runbooks](operations/runbooks.md) | Replacing a run, retiring a name, rolling an upgrade, recovering from a store outage. |
| [Monitoring](operations/monitoring.md) | Metrics, traces, snapshots, and what to alert on. |

## Project status

Not documentation. This project is pre-1.0 and tracks its own state in the open:
what is proven and what is not ([capability matrix](project/CAPABILITY-MATRIX.md)),
what is planned ([roadmap](project/ROADMAP.md)), what it is for
([goals](project/GOAL.md)), and what it costs
([benchmarks](BENCHMARKS.md)). The [engineering records](internal/) are the
design arguments the library was built from — useful if you are changing it,
and the wrong place to start if you are using it.
