# Orleans.Dataflow samples

One console application, eight scenarios, and every scenario written twice — once in C# and once in
F#. Both authorings run in the same process, and the runner prints their results side by side together
with both graph fingerprints and whether they are identical.

```bash
dotnet run --project samples/Orleans.Dataflow.Samples --configuration Release
```

```
  --list                     Name each scenario and what it teaches.
  --only TEXT                Run only scenarios whose name contains TEXT.
  --smoke                    Run everything at the smallest sizes that still exercise it.
  --timeout-seconds N        Give up after this long (default 900).
  --help                     Print the above.
```

An unrecognized argument, a failing scenario, and a disagreement between two authorings each exit
non-zero.

## The spine: two frontends, one document

This library's central claim is that C# and F# are **equal frontends over one graph algebra**, not a
frontend and a wrapper around it. Authoring a graph in either language produces the same immutable
document, and the fingerprint of that document — the SHA-256 of its canonical serialization — is
therefore the same 32 bytes.

Every scenario here is authored twice for exactly that reason, and the runner checks it:

```
fingerprints
graph        identical   csharp                    fsharp
main         yes         sha256:30acffef99d8…      sha256:30acffef99d8…

observations
name                     agree   csharp   fsharp
orders-in-the-feed       yes     12       12
orders-the-filter-kept   yes     9        9
```

The two authorings must agree about **both**: the fingerprint of every graph they build, and every
observation their runs produce. If any pair differs, the run says so and exits non-zero. That is what
makes this application self-verifying rather than decorative, and it is why CI runs `--smoke` on every
push: a frontend that had drifted would still compile, and nothing else in the repository would notice
as quickly.

## What each scenario teaches

| Scenario | What a reader learns |
| --- | --- |
| `first-pipeline` | The whole authoring vocabulary in four lines: a source, a filter, a map, a fold, and one typed result slot. This is literally the repository README's snippet, made runnable. |
| `backpressure` | A declared buffer bound is what bounds memory, and a declared overflow policy is what decides who is dropped. The same shape runs under `DropOldest` and `DropNewest`, and the two kept sets differ. |
| `async-work` | Asynchronous mapping runs exactly as concurrently as the graph declared — the sample would hang rather than print a wrong number — and ordered and unordered emission differ in what leaves first, not in what finishes first. |
| `junctions` | One stream broadcast into two branches, each ending in its own terminal with its own result slot, so one run answers two questions. |
| `windowing` | Grouping closed by a count or a window, whichever comes first; and a keyed operator that **refuses** a key past its declared maximum, with the bound and the offending key named in the message. The refusal is a designed outcome, not a crash. |
| `failure` | A stage that throws inside a supervision scope: once with retries and a declared ladder of waits, once with a declared fallback that ends the scope's stream successfully. Both print the run's snapshot counters afterwards, which is where a run's diagnostics live. |
| `cluster` | The same pipeline materialized on a real in-process silo through the ordinary hosting API — the generic host, `AddOrleansDataflow`, `AddOrleansDataflowClient`, `OrleansDataflowHost` — and no test facility anywhere. |
| `durable` | A durable run that dies mid-stream, a second host that continues it from the last checkpoint, and the at-least-once replay window between them, printed as the orders both attempts delivered. |

## How the two projects are arranged

- **`Orleans.Dataflow.Samples`** — the C# console application: the CLI, the scenario runner, the
  printing, the C# authoring of every scenario, the silo the cluster scenario runs on, the stage
  factory behind the registered vocabulary, and `SampleCheckpointStore`.
- **`Orleans.Dataflow.Samples.FSharp`** — an F# library: the F# authoring of every scenario, one module
  per scenario, plus the small kernel both frontends share.

The F# side is a **library rather than a second application** on purpose: the two authorings have to run
in one process, because comparing them is the point.

That reference direction — the C# application referencing the F# library — is also why the shared
kernel lives on the F# side. Anything both authorings must agree about has to be visible to both, and
only the F# project is. So `Domain.fs` holds the order domain, `Vocabulary.fs` holds the registered
stage catalog the cluster scenario deploys, `Coordination.fs` holds the small synchronizing objects the
timing scenarios need, and `Reporting.fs` holds what a scenario answers with. None of it is F#-specific:
a catalog is a published artifact rather than a language artifact, and a graph document never names a
CLR type at all.

## Public API only

Neither project references `Orleans.Dataflow.Testing`, and neither reaches into any internal. The
samples are also the clean-room check that the published surface is usable without reading the
library's source, so where a scenario needed something the library ships a test-only convenience for,
it was written here instead:

- `SampleCheckpointStore` implements the published `ICheckpointStore` in about fifty lines. Its doc
  comment states which of the store's three duties it honors — atomic per document, compare-and-swap on
  the ETag — and which it fakes for a demonstration living in one process.
- `SampleStageFactory` implements the published `IDataflowStageFactory` for the three registered stages
  the cluster scenario deploys.
- `SampleCluster` stands the silo up with the generic host rather than with the shipped test cluster.

## Two things the samples had to work around, and why

Both are library behavior rather than sample quirks, and both are worth knowing before you write your
own graph.

- **A supervision scope inside a supervision scope is refused** — which of two nested policies wins is
  a contract nobody has written yet. So "retry, and if the retries run out substitute a fallback" is not
  one scope with two answers; it is a choice between them, and the `failure` scenario is therefore two
  graphs rather than one.
- **An ordered asynchronous mapping holds a completed result until everything before it has been
  emitted.** Arranging for an element to wait on another element outside the declared concurrency
  window is a deadlock rather than a demonstration; the `async-work` scenario keeps its handshake inside
  the first concurrent batch for that reason.
