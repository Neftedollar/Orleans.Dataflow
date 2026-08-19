# Deploying

What a silo needs, what a client needs, what every silo has to agree about, and
what the deployment owes the library.

## What a silo needs

Every silo that may host runs:

```csharp
silo.AddOrleansDataflow(dataflow => dataflow
    .AddCatalog(YourVocabulary.Catalog())
    .AddFactory(YourVocabulary.Provider, new YourStageFactory()));
```

Every silo that may host a **durable** run additionally:

```csharp
    .UseCheckpointStore(services => services.GetRequiredService<YourCheckpointStore>())
```

And, outside the dataflow builder, a grain storage provider under the name the
coordinator keeps its register beneath:

```csharp
silo.AddAzureTableGrainStorage("orleans-dataflow-coordinator", /* … */);
```

The name is `OrleansDataflowStorage.CoordinatorProviderName`. **There is no
default**, on purpose: an in-memory default would let a deployment believe its
runs were durable while their register died with the process.

Four rules that will fail a silo at startup rather than at run time:

- **At least one `AddCatalog` call**, even when every stage in your pipelines is
  a shipped adapter. Without one a silo can resolve no stage reference, so every
  document it is handed is refused.
- **One factory per provider**, and one specification per stage reference.
  Registering either twice is refused, because two answers to one question is not
  a merge. `AddCatalog` itself is callable many times and the silo's catalog is
  the union — a deployment composes vocabularies from several packages.
- **A silo that registers a catalog without the matching factory** accepts a
  document at the coordinator and refuses it at materialization, naming the
  missing provider. That is legal and sometimes what you want; it is not usually
  what you meant.
- **A silo with no checkpoint store runs no durable pipeline.** Its ordinary runs
  are unaffected; what it refuses — at the declaration, by name, before anything
  has run — is a request for a run whose position must survive. Proven by
  `RollingUpgradeTests.ASiloWithNoCheckpointStoreRefusesADurableDeclarationRatherThanRunningWithoutOne`.

Anything an adapter needs is the deployment's to register too: the Orleans
streaming provider and its `PubSubStore`, the reminder service, the broadcast
channel providers. The adapters *name* a provider and never configure one.

## What a client needs

One line on its services:

```csharp
client.Services.AddOrleansDataflowClient(options => options.PollInterval = TimeSpan.FromMilliseconds(20));
```

That materialises `OrleansDataflowHost`, which is what you resolve to start,
watch, replace, and retire runs. `PollInterval` defaults to 20 ms and is what a
`Completion` wait costs in grain calls — a snapshot is one call per reading and
neither starts nor joins the poll loop, so a monitor sampling on its own schedule
costs exactly the calls it makes.

A silo can be its own client. `AddOrleansDataflowClient` resolves `IGrainFactory`,
which a silo provides as readily as a cluster client does, so a silo that wants
to start pipelines of its own needs no second process — that is what
[`samples/Orleans.Dataflow.Samples/SampleCluster.cs`](../../samples/Orleans.Dataflow.Samples/SampleCluster.cs)
does.

## What every silo must agree about

Three things. Each disagreement has a different failure mode, and knowing which
is which is most of a rolling upgrade.

| They must agree about | What a disagreement does |
|---|---|
| **The registered stages and their versions** | The coordinator refuses a document a silo cannot resolve. A resume landing on a silo without the stage is refused **by name**, and the run continues when a capable silo picks it up. Nothing is lost — a refusal does not touch the checkpoint. |
| **The checkpoint store** | A cluster whose silos disagree accepts a durable declaration on one host and cannot honour it on another. The refusal a run then gets names exactly this. |
| **The coordinator's storage provider** | Registered under one name; a silo without it cannot activate a coordinator at all. |

They need **not** agree about the result-size cap or about placement. Both are
deliberately a silo's rather than a pipeline's: how much a host is willing to put
on one message is a property of the deployment and its network, and putting it in
a document would make two silos accept the same graph and disagree about what it
may return.

A rolling upgrade is exactly the window in which silos disagree about their
catalogs, and it has its own procedure — see
[Rolling an upgrade](runbooks.md#rolling-an-upgrade-across-silos-that-disagree).

## The trust boundary

**Everything connected to the cluster is inside it.**

This library adds no per-call authorization, and Orleans hands a grain no caller
identity, so there is nothing here that *could* authorize one. State that plainly
before anything else: a client on the cluster's wire can address any run's
grains, and durable run identities are author-chosen names rather than secrets.

### What a connected caller can do

- **Stop, cancel, replace, or retire any run whose name it can guess.**
- **Read the results of any run.**
- **Read back the canonical document of any declared durable run** — which is the
  whole pipeline's shape and every parameter in it.

Treat a pipeline document as readable by anything that can reach the cluster. If
a parameter is a secret, it does not belong in a document; make it a
registration on the silo and let the document name it.

### What the protocol does defend, and why none of it is authorization

- An ownership [epoch](../reference/glossary.md#epoch) is refused unless the
  coordinator issued it (`TrustBoundaryTests.AStartCarryingAnEpochNoCoordinatorIssuedIsRefusedAndLeavesTheRunStartable`).
- Ownership is taken by the activation that is about to host the run and by
  nothing else, so a bystander reading a declaration fences nobody
  (`TrustBoundaryTests.ReadingADeclarationOfALiveRunFencesNobodyAndItsEndingIsStillRecorded`).
- Documents are bounded **before** they are decoded.
- The register a declaration grows is bounded.

These stop a confused caller and a runaway script. They do not stop a hostile
one, and are not meant to.

### The obligations that follow

They are the deployment's, and there are four:

1. **Do not expose the Orleans gateway to untrusted clients.**
2. **Use Orleans' own connection-level authentication and TLS.**
3. **Isolate the cluster's network.**
4. **If one cluster hosts more than one tenant, put an `IIncomingGrainCallFilter`
   in front of these grains.** That seam is Orleans', it is where per-call
   identity belongs, and this library deliberately does not occupy it.

## Limits a coordinator enforces

Each of these refuses work rather than slowing it, and each exists because the
coordinator's turn is a shared resource: `StartRunAsync` is not interleaved, so
anything it does is time the whole coordinator spends — status polls of its other
runs included.

| Bound | Value | Why it exists |
|---|---:|---|
| Document bytes, measured **before** decoding | 4 MiB | Decoding happens on the coordinator's own turn. A pipeline that legitimately approaches this is a generated one; split it, or run its parts as separate pipelines. |
| Nodes in a document, counted after decoding | 10,000 | The same turn, bounded by shape rather than by size. Validating, resolving every stage against the catalog, and compiling the plan are all linear or worse in the node count. |
| Durable run identities per pipeline | 1,000 | A record holds the document it names, and the whole register is rewritten on every declaration — so an unbounded register eventually exceeds the storage provider's per-document limit, after which the coordinator cannot write at all and **every** start of that pipeline stops with it. The refusal names the cap and points at retirement. |
| Canonical payload value | 256 KiB, from inputs of at most 4 MiB | The value cap is the contract; the input ceiling is a memory floor, so refusing an absurd input costs no more than reading it. |
| Diagnostics in a refusal message | 20, then "and N more" | A refusal travels as text across a wire. An uncapped one can be larger than the document that earned it — a 200,000-node document once produced a 23-million-character exception message. |
| Result bytes across a grain boundary | 1 MiB, per silo | A `Collect` over a cluster produces a result whose size nothing in the document bounds; without a cap the caller meets a codec error, a transport failure, or a poll that never answers. Raise it with `LimitResultSize` and do so on purpose. |

A pipeline a person wrote never meets any of these. They are here so that a
generated or hostile one is refused rather than absorbed. All six are proven by
`CoordinatorLimitsTests` and `ResultSizeTests`.

## Placement

Two grain types have a placement worth deciding, and both default to
`DataflowPlacement.ClusterDefault` — which *defers* rather than naming a
strategy, so a deployment that configured its own default keeps it.

```csharp
silo.AddOrleansDataflow(dataflow => dataflow
    .UsePlacement(runGrains: DataflowPlacement.ClusterDefault, keyedExecutors: DataflowPlacement.HashBased));
```

`Random` spreads without regard to load; `PreferLocal` removes the network hop
and gives no spread; `HashBased` makes a key's placement a property of the key,
which is what you want when your data is already partitioned by that key.

**For a durable run this is a performance choice and never a correctness one.**
The checkpoint travels through the store rather than through the silo, so a
resumed activation lands wherever Orleans places it and continues identically.

## Turning on telemetry

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics.AddMeter("Orleans.Dataflow"))
    .WithTracing(tracing => tracing.AddSource("Orleans.Dataflow"));
```

Do this before you need it. See [Monitoring](monitoring.md) for the instruments
and what to alert on.

## A checklist

Before a deployment carries production traffic:

- [ ] Every silo registers the same catalogs at the same versions.
- [ ] Every silo that may host a durable run registers the **same** checkpoint store.
- [ ] A grain storage provider exists under `orleans-dataflow-coordinator`, and it is not in-memory.
- [ ] The checkpoint store honours all three duties — see [Checkpoint stores](checkpoint-stores.md).
- [ ] The gateway is not reachable from untrusted networks; TLS and connection-level authentication are on.
- [ ] A call filter is in place if one cluster serves more than one tenant.
- [ ] Durable run names are enumerable and someone owns retiring them.
- [ ] The meter and the activity source are wired to your collector.
- [ ] Someone has read [what user data can reach a failure message](monitoring.md#what-user-data-can-reach-a-failure-message) and checked their `GroupBy` keys against it.

## Next

- [Checkpoint stores](checkpoint-stores.md) — the contract, in full.
- [Runbooks](runbooks.md) — the procedures, each as steps.
- [Monitoring](monitoring.md) — instruments, traces, and alerting starting points.
- [The cluster model](../concepts/cluster-model.md) — why the pieces are arranged this way.
