# Hosting

How the library is registered and reached: the in-process host, the silo builder
extension, the client extension, and the cluster host's methods.

There are two hosts and they take different things. `LocalDataflowHost` takes a
[`RunnableGraph`](operators.md#reusing-and-composing) and runs it in your
process. `OrleansDataflowHost` takes a `PipelineDefinition` — a graph that has
been given an identity and contains no delegates — and asks a cluster to run it.

**Materializing is the only call that starts anything.** Everything before it is
value construction.

**Examples on this page.** The silo registration is lifted verbatim from
`samples/Orleans.Dataflow.Samples/SampleCluster.cs`, which builds a real silo and
runs in continuous integration; the pipeline sequence is from
`samples/Orleans.Dataflow.Samples/CSharp/Cluster.cs`. The local-host and
client-registration snippets were compiled and executed in a scratch project
written for this page. The one illustrative block is the provider registration
under [the local builder](#the-local-builder), which names a factory you supply.

---

## The local host

`Orleans.Dataflow.LocalDataflowHost`. Four constructors, three materializing
methods. It is stateless and holds no run: a run's lifetime is its
[handle](run-handles.md)'s, and one host can materialize any number of graphs
from any number of threads.

### Constructors

| Constructor | What it gives you |
|---|---|
| `LocalDataflowHost()` | The lambda-only host. It resolves exactly the local stage catalog and no registered provider, so a graph containing a registered stage is refused *by name* rather than half-executed. Its clock is `TimeProvider.System`. |
| `LocalDataflowHost(timeProvider)` | The same, measuring time by a clock you supply. |
| `LocalDataflowHost(configure)` | Registers catalogs, factories, and .NET push adapters through [`ILocalDataflowBuilder`](#the-local-builder). |
| `LocalDataflowHost(timeProvider, configure)` | Both. They are independent: the clock reaches the local vocabulary's timing stages, and a registered stage receives the run's tokens and whatever its own provider gave it. |

**The clock is a service, not a setting.** Every operator that reads a clock —
`Delay`, `InitialDelay`, `Timeout`, `TakeWithin`, `SkipWithin`, `Throttle`, and
`Source.Tick` — reads the one this host was given and never `TimeProvider.System`
directly, which is what makes a deterministic test of them possible at all. It is
read when a graph is materialized and carried by the run. A document never
carries a clock: two runs of one graph may be measured by two different clocks
and their [fingerprints](glossary.md#fingerprint) are the same.

Registrations are checked in the constructor, so a broken one stops the host from
being built rather than surfacing at the first graph. What they resolve to is one
immutable catalog and one immutable factory registry, shared by every graph the
host materializes.

### Materializing

| Method | What it does |
|---|---|
| `ValueTask<RunHandle> MaterializeAsync(graph, ct = default)` | Validates the document against the host's catalog, binds the delegates, and starts the run. |
| `ValueTask<RunHandle> MaterializeDurableAsync(graph, durable, ct = default)` | The same, with the run writing [checkpoints](glossary.md#checkpoint) on the cadence [`DurableRunOptions`](options.md#durablerunoptions) declares. |
| `ValueTask<RunHandle> MaterializeFromCheckpointAsync(graph, durable, ct = default)` | Reads the stored checkpoint for that run identity and continues the run it describes. |

**The run is started before the call returns.** There is no separate start step,
because a materialized run that had not started would be a state with no use and
one more thing to get wrong.

**An already-cancelled token does not make the call throw.** The run starts,
observes the token before its first pull, and ends cancelled without ever
enumerating the source — so a caller always receives a handle to await and
dispose. Cancellation is an outcome of a run, not a failure of materialization.

**Validation failures name every diagnostic**, not the first, because a caller
fixing a foreign document needs the whole report.

Three things are worth knowing about the durable pair:

- **A run that declares neither an interval nor an element bound never touches
  the store.**
- **Nothing is read on a fresh durable start, and the first write presents no
  ETag.** A run started under a name that already has a checkpoint is therefore
  refused by the store at its first capture, loudly, with a
  [`CheckpointConflictException`](errors.md#checkpointconflictexception) that
  fails the run. That is what stops a fresh start from quietly overwriting a live
  run.
- **A clean end writes nothing.** A run that completes has an outcome and does
  not need a checkpoint — which is exactly why the last stored capture is what a
  resume replays from.

```csharp
LocalDataflowHost host = new(TimeProvider.System);

await using RunHandle run = await host.MaterializeAsync(graph, CancellationToken.None);
await run.Completion;

DurableRunOptions durable = new()
{
    Store = store,
    RunId = RunId.Create("nightly-2026-08-19"),
    EveryElements = 1_000,
    Interval = TimeSpan.FromSeconds(30),
};

await using RunHandle durableRun = await host.MaterializeDurableAsync(graph, durable);
await durableRun.Completion;

await using RunHandle resumed = await host.MaterializeFromCheckpointAsync(graph, durable);
await resumed.Completion;
```

```fsharp
let host = LocalDataflowHost()
use! run = host.MaterializeAsync graph
do! run.Completion
```

`LocalDataflowHost` is a C# type; F# calls it directly. There is no F# wrapper,
because a host has no algebra to give an idiomatic spelling to.

### The local builder

`Orleans.Dataflow.Hosting.ILocalDataflowBuilder`. Four members, and each one is
member-for-member the mirror of the silo builder's where the two hosts have the
same question to answer.

| Member | What it registers |
|---|---|
| `AddCatalog(catalog)` | The stage specifications this host accepts. Callable more than once; the host's catalog is the union. Registering one stage reference twice is refused. |
| `AddFactory(provider, factory)` | The [`IDataflowStageFactory`](provider-sdk.md#the-factory) that builds every stage of one provider. One factory per provider. |
| `AddDotnetStages()` | The `dotnet` vocabulary — the timer and the observable source. See [adapters](adapters.md#the-net-vocabulary). |
| `AddObservable(binding)` | Names an `IObservable<T>` this host may open, with its element contract. |

```csharp
LocalDataflowHost host = new(builder => builder
    .AddCatalog(providerCatalog)
    .AddFactory(providerId, new MyStageFactory()));
```

**The two halves are registered separately because different processes need
different halves.** A catalog is all a validator needs; only a host that will
*run* the graph needs a factory. A host with the catalog and no factory validates
a document and refuses it at materialization, naming the provider that has
nothing to build it.

The very catalog, the very factory, and the very bindings a silo is given can be
given to this host, so one declaration serves both runtimes and a graph written
against them runs in either.

---

## The silo builder extension

`Orleans.Dataflow.Hosting.OrleansDataflowSiloBuilderExtensions.AddOrleansDataflow`,
an extension on `ISiloBuilder`. One call, taking the registrations a silo needs
to accept and run pipelines.

```csharp
_ = builder.UseOrleans(silo =>
{
    // Development clustering: one silo that is its own membership table. A deployment names a real
    // clustering provider here and changes nothing else in this method.
    _ = silo.UseLocalhostClustering();

    // The coordinator keeps one register per pipeline, and which store stands behind it is a
    // deployment decision the library deliberately does not make. In memory, here, because this
    // silo lives as long as one run of the samples.
    _ = silo.AddMemoryGrainStorage(OrleansDataflowStorage.CoordinatorProviderName);

    // The whole of registering this library on a silo: the vocabulary its documents may name, and
    // the factory that builds those stages when a run is materialized.
    _ = silo.AddOrleansDataflow(dataflow => dataflow
        .AddCatalog(SampleVocabulary.Catalog())
        .AddFactory(SampleVocabulary.Provider, new SampleStageFactory()));

    // The client side, on the same services, because this process is both.
    _ = silo.Services.AddOrleansDataflowClient();
});
```

**What a silo owes the library** is exactly three things, and only the first is
this call:

1. `AddOrleansDataflow(...)` with the vocabulary the deployment's documents may
   name.
2. A **grain storage provider** registered under
   `OrleansDataflowStorage.CoordinatorProviderName` — the constant
   `"orleans-dataflow-coordinator"` — which is where each pipeline's coordinator
   keeps its register. Which store stands behind that name is a deployment
   decision the library deliberately does not make.
3. A **stream provider**, if any document names a stream adapter — such as
   `AddMemoryStreams` beside a `PubSubStore`.

**The grains need no registration and get none.** Orleans discovers them from the
generated metadata of the assemblies it has loaded, and calling this method is
what loads this one — so the registration a deployment writes and the discovery it
depends on are the same act, and a silo cannot end up configured for dataflow
without the grains that serve it.

**Registrations are checked while the silo is being built**, so a broken one stops
the host from starting rather than surfacing at the first pipeline. What they
resolve to is one immutable value built from the silo's own container, so every
activation sees the same catalog and the same factories, and a run is always
materialized against exactly the catalog the coordinator validated its document
with. `AddOrleansDataflow` throws `ArgumentException` when no catalog was
registered, or when one stage reference, one provider, or one Orleans binding
name was registered twice.

### The silo builder

`Orleans.Dataflow.Hosting.IOrleansDataflowBuilder`. Fourteen members: four shared
with the local builder, seven that name Orleans bindings, three that are silo
settings.

| Member | What it registers |
|---|---|
| `AddCatalog(catalog)` | As the local builder. |
| `AddFactory(provider, factory)` | As the local builder. |
| `AddDotnetStages()` | As the local builder. |
| `AddObservable(binding)` | The *very* binding a `LocalDataflowHost` is given, because an `IObservable<T>` is not an Orleans concept and a deployment should not declare it twice to run one document in two runtimes. |
| `AddStreamElement<T>(binding)` | The CLR type that carries one element contract over this silo's Orleans streams. |
| `AddBroadcastElement<T>(binding)` | The same, for broadcast channels. |
| `AddGrainCall<TIn, TOut>(binding)` | A named awaited grain call that transforms elements. |
| `AddGrainCallSink<TIn>(binding)` | A named awaited grain call that terminates a graph. |
| `AddKeyedGrainCall<TIn, TOut>(binding)` | A named keyed grain call and the function that partitions its elements. |
| `AddGrainEnumerable<T>(binding)` | A named grain enumeration that heads a graph. |
| `AddObserverBridge<T>(binding)` | A named observer bridge that heads a graph. |

**Why the bindings exist.** The name is what a document may carry; the delegate
is what a document may not. A document naming a call this silo does not register
is refused when the run is started, with the compiler's own diagnostics naming
the node and listing the calls this silo *does* publish. For a keyed call, every
silo that may host one of its executors registers the same binding, because a
distributed keyed stage places its executors anywhere in the cluster.

Element types must satisfy Orleans serialization, and Orleans checks that at
first use rather than here.

**The Orleans adapter vocabulary is published exactly when the silo registers at
least one Orleans binding.** A deployment that uses no adapter keeps precisely
the catalog it wrote — and precisely the catalog fingerprint it had — while a
deployment that registers one stream element or one named call gets all ten
adapter stages, because they ship as one vocabulary and a half-published one
would fail at the first element instead of at the start.

### Silo settings

| Member | Default | What it says |
|---|---|---|
| `LimitResultSize(maximumBytes)` | `OrleansDataflowResults.DefaultMaximumResultBytes` — 1 MiB (1 048 576) | The largest result this silo will send across a grain boundary, measured on the value's Orleans-serialized form. At least 1. Exceeding it raises [`ResultTooLargeException`](errors.md#resulttoolargeexception) for *that read* and nothing else. The bound is a silo's, not a pipeline's, because how much a host is willing to put on one message is a property of the deployment and its network. |
| `UsePlacement(runGrains, keyedExecutors)` | `DataflowPlacement.ClusterDefault` for both | Where a run's grain and a keyed stage's per-key executors are placed. Values: `ClusterDefault`, `Random`, `PreferLocal`, `HashBased`. It is a knob rather than an attribute on the grain classes because the cluster default is resource-optimized — the right default, and the wrong one for a deployment that has arranged its data by the same key its executors are named after. |
| `UseCheckpointStore(resolver)` | none | Where this silo's durable runs keep their checkpoints. The resolver is called once, with the silo's own container, and the value it answers serves every run on this silo. |

**There is deliberately no default checkpoint store**, for the same reason the
coordinator's storage has none: an in-memory default would let a deployment
believe its runs were durable while their positions died with the process that
wrote them. **A silo without one is a silo that runs no durable pipeline**, which
is a legal configuration; what it refuses — at the declaration, by name, before
anything has run — is a request for a run whose position must survive.

What the store has to be is stated by
[`ICheckpointStore`](../operations/checkpoint-stores.md) and is one property: an
ETag-guarded write that refuses a writer the store has moved on from.

Each of the three replaces whatever a previous call said rather than adding to
it, because a silo has one bound, one placement, and one store.

---

## The client extension

`Orleans.Dataflow.Hosting.OrleansDataflowClientExtensions.AddOrleansDataflowClient`,
an extension on `IServiceCollection`.

```csharp
services.AddOrleansDataflowClient();
services.AddOrleansDataflowClient(options => options.PollInterval = TimeSpan.FromSeconds(1));
```

One registration, for the one type a client needs. There is nothing resembling a
client builder because the cluster connection is Orleans' to configure and this
library has no opinion about it.

It resolves `IGrainFactory`, which **both a cluster client and a silo provide**,
so the same registration works inside a silo that wants to start pipelines of its
own — which is what the sample above does with
`silo.Services.AddOrleansDataflowClient()`. A deployment whose clients are
separate processes writes the same line in the client and nothing else changes.

`OrleansDataflowHost` is registered as a singleton because it is stateless and
holds no run. Its one option is
[`OrleansDataflowClientOptions.PollInterval`](options.md#orleansdataflowclientoptions).

---

## The cluster host

`Orleans.Dataflow.Hosting.OrleansDataflowHost`. Two constructors and four
methods. You normally resolve it rather than construct it.

| Constructor | |
|---|---|
| `OrleansDataflowHost(grains)` | With the default poll interval. |
| `OrleansDataflowHost(grains, options)` | With a poll interval of your own. |

| Method | What it does |
|---|---|
| `Task<OrleansRunHandle> MaterializeAsync(pipeline, ct = default)` | Starts an ordinary run. The cluster names it with a fresh identifier, because two runs of one pipeline are two runs. |
| `Task<OrleansRunHandle> MaterializeDurableAsync(pipeline, durable, ct = default)` | Starts — or *addresses* — the durable run of that name. Materializing one durable pipeline twice under one `RunId` addresses one run: the second call hands back a handle to the run that already exists, or continues it from its checkpoint if the silo hosting it has died. |
| `Task<OrleansRunHandle> ReplaceDurableRunAsync(pipeline, durable, ct = default)` | The destructive spelling. |
| `Task<bool> RetireDurableRunAsync(pipelineId, runId, ct = default)` | Gives a durable name up. |

```csharp
RunnableGraph graph = Source
    .FromRegistered(SampleVocabulary.Feed, "feed", SampleVocabulary.FeedParameters(orders))
    .Via(SampleVocabulary.Discount, "discount", SampleVocabulary.DiscountParameters(DiscountPercent))
    .To(
        SampleVocabulary.Tally,
        "tally",
        SampleVocabulary.TallyParameters("accepted-orders", MinimumAmount),
        "accepted",
        out ResultSlot<long> _);

PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create(Lineage), GraphRevision.Create(Revision));
ResultSlot<long> accepted = pipeline.ResultSlot("accepted", SampleVocabulary.TallyContract);

await using OrleansRunHandle run = await sample.Cluster.MaterializeAsync(pipeline, cancellationToken);

RunEnding ending = await run.WatchTermination;
long tally = await run.GetValueAsync(accepted, cancellationToken);
RunSnapshot snapshot = await run.SnapshotAsync(cancellationToken);
```

**A pipeline is not a graph with a name on it.** Declaring an identity re-closes
the document under that identity, so a pipeline's fingerprint differs from its
graph's by design. And every stage in it has to be a registered one: a graph
holding a delegate declares itself nondeployable, and `AsPipeline` refuses it by
name rather than shipping a document a silo could not resolve.

**The slot is recovered from the pipeline** rather than kept from the closing
call. A closed graph's slot binds to that built instance; a pipeline's binds to
the fingerprint and the lineage, which is what lets a run started by one process
be read by another.

### Replacing and retiring

`MaterializeDurableAsync` **refuses** a run identity that already holds a
different document, by name and with both fingerprints — see
[`PipelineResumeRefusedException`](errors.md#pipelineresumerefusedexception).
Migrating a checkpoint across a changed graph is not something a cluster will
guess at. The two calls below are what a deployment says when it means something
else, and both are destructive.

**`ReplaceDurableRunAsync`** clears the stored checkpoint, supersedes the previous
attempt with a fresh epoch, and runs the document from the beginning under the
name it took over. The document does not have to differ: replacing an identity
with the very document it already held is how a *finished* durable run is run
again — a run that has ended stays ended, and no poll revives it — and replacing
it with a new revision is how an identity moves forward. Both destroy the same
thing, which is why they are one call. If the stored checkpoint moves between
being read and being cleared, the call raises
[`CheckpointConflictException`](errors.md#checkpointconflictexception) and
retrying is the answer.

**`RetireDurableRunAsync`** is destructive in exactly the same way, with the one
difference that gives it its name: the declaration is *removed* rather than
rewritten. A replacement takes a name forward onto a new document; a retirement
gives the name up. It answers `true` when a declaration was removed. It takes two
identifiers rather than a pipeline, because an operator carrying out a runbook has
the names and no reason to be able to rebuild the document.

It exists because the register of durable names is a thing that grows: each record
holds the document it names, the coordinator rewrites the whole register on every
declaration, and a deployment that names durable runs after something outside its
control — a tenant, a day, a customer — otherwise grows a state document until its
storage provider will not accept it. A cap refuses the thousand-and-first name;
retiring is what makes room for it.

**Neither call stops what is running, because the cluster may not.** What ends a
replaced or retired run is its own next capture, refused by a store it no longer
holds an ETag for. A run that declared no checkpoint timing at all — and therefore
never captures — runs on until something else ends it. That is why both are an
operator's decision. See [runbooks](../operations/runbooks.md).

---

## Related

- [Run handles](run-handles.md) — what each materializing call hands you.
- [Options](options.md) — every value these calls take.
- [Provider SDK](provider-sdk.md) — what goes into `AddCatalog` and `AddFactory`.
- [Adapters](adapters.md) — what each `Add…` binding makes addressable.
- [Deploying](../operations/deploying.md) — what the deployment owes the library
  in production.
- [Running on a silo](../start/running-on-a-silo.md) — the same registration as a
  tutorial.
