# Registered stages and pipeline definitions

- Status: M1 design for the deployable authoring surface, extended by M4.5 with
  multi-port registered stages and the public runtime-factory seam; signatures
  settle with the implementation checkpoint
- Depends on: [ADR 0001](../architecture/0001-definition-runtime-authoring-planes.md),
  [ADR 0004](../architecture/0004-csharp-api-baseline.md) §6,
  [DEFINITION-MODEL.md](DEFINITION-MODEL.md)

The lambda surface authors nondeployable local graphs. The registered
surface authors graphs whose behavior is resolved from a stage catalog by
stable identity — the path to `PipelineDefinition` and, in M3, to durable
Orleans execution. Both surfaces compose in one chain; deployability is a
property the closed document either has or does not.

## Typed contracts are the bridge

The definition plane forbids CLR type names as contract identity, so the
authoring layer needs an explicit, process-local association between a
contract and a CLR type:

```csharp
ElementContract<OrderCreated> orderCreated = ElementContract.For<OrderCreated>("order-created", 1);
```

`ElementContract<T>` carries a `ContractReference` and the compile-time
type; declaring it is deployment code's assertion "in this process, contract
`order-created@v1` is `OrderCreated`". The document stores only the
reference. Two processes agreeing on the reference but binding different
CLR types is a deployment error the definition plane cannot see — stated,
not hidden; cross-silo enforcement is the M3 catalog-fingerprint check plus
serializer contracts.

## Typed stage handles

A registered stage becomes a typed authoring value by pairing its
specification with element contracts:

```csharp
RegisteredFlow<OrderCreated, OrderDocument> normalize =
    RegisteredStage.Flow(catalog, normalizeRef, input: orderCreated, output: orderDocument);
```

Construction validates against the catalog immediately: the stage must
exist, must have the linear shape the handle claims (one input, one output
for a flow; the corresponding shapes for sources and sinks), and the
element contracts must equal the specification's port contracts. A mismatch
is an `ArgumentException` at handle creation, not a compiler diagnostic at
close.

Sources and sinks follow the same pattern (`RegisteredStage.Source`,
`RegisteredStage.Sink`, result-bearing
`RegisteredStage.SinkWithResult<TIn, TResult>` pairing the result port's
contract with a `ResultContract<TResult>` declaration).

## Multi-port handles: registered junctions

A registered stage may declare several input ports or several output ports,
and since M4.5 the authoring surface has handles for both. They are the same
handles with one word added — *every*: the port counts have to be exactly what
the handle claims, and **every port** has to carry the contract the handle
declares for it.

```csharp
RegisteredFanOut<OrderDocument, OrderDocument> split =
    RegisteredStage.FanOut(catalog, splitRef, OrderDocumentContract, OrderDocumentContract);

RegisteredFanIn<OrderDocument, OrderDocument> join =
    RegisteredStage.FanIn(catalog, joinRef, OrderDocumentContract, OrderDocumentContract);
```

Four factories, two of them for junctions whose ports carry unlike contracts —
the unzip shape and the zip shape, which are separate handles rather than
options because two legs with two element types cannot be described by one type
argument:

| Factory | Shape | Ports checked |
|---|---|---|
| `RegisteredStage.FanOut<TIn, TOut>` | one in, *n* legs of one contract | 1 input, ≥ 2 outputs, 0 results |
| `RegisteredStage.FanOut<TIn, TLeft, TRight>` | one in, two unlike legs | 1 input, exactly 2 outputs, 0 results |
| `RegisteredStage.FanIn<TIn, TOut>` | *n* inputs of one contract, one out | ≥ 2 inputs, 1 output, 0 results |
| `RegisteredStage.FanIn<TFirst, TSecond, TOut>` | two unlike inputs, one out | exactly 2 inputs, 1 output, 0 results |

**The arity is read from the specification rather than asked for.** How many
legs a junction has is a fact about the stage a provider registered; a handle
that let an author restate it would let the two disagree. What a call has to
match is `Legs` or `Inputs` on the handle, and a call with the wrong number is
refused naming both numbers.

**Position is the specification's own canonical port order**, ordinal by port
name. A specification sorts its ports at construction, so that order is the
same in every process that resolves it, and it is the order the authoring side
wires branches in, the order the planner allocates legs in, and the order a
provider's own router or combiner answers in. One statement, read by three
places.

**A junction declares no result port.** A result is read from a terminal and a
junction is not one; requiring none rather than ignoring them keeps a stage
from quietly declaring a result nothing in a graph could ever expose.

The fluent attachment reuses ADR 0006's shapes exactly — a fan-out is a
terminal call taking branches, a fan-in is a combinator on sources — and adds
the occurrence name and payload every registered attachment carries:

```csharp
RunnableGraph graph = Source.FromRegistered(orderSource, "orders-in", sourceParameters)
    .Via(normalize, "normalize", normalizeParameters)
    .FanOutTo(
        split,
        "split",
        splitParameters,
        Flow.For<OrderDocument>().To(countSink, "count-left", sinkParameters, "left", out ResultSlot<long> left),
        Flow.For<OrderDocument>().To(countSink, "count-right", sinkParameters, "right", out ResultSlot<long> right));
```

**What that graph declares is nothing at all.** Every occurrence is registered,
so no stage requires `nondeployable`; every occurrence is named, so nothing
requires `ephemeral-identity`; every port carries a real contract, so the graph
compiler finds no seam. `AsPipeline` accepts it, and it is the first branching
document that a cluster can be handed. The M4.2 limit — "a fan-out pipeline
built entirely from registered stages is not expressible until a provider can
register a junction" (ADR 0006's implementation notes) — is closed by exactly
this, and the M4.2 test that asserts a local junction costs a graph both tokens
now has a sibling asserting their absence.

**The mixing rule is unchanged.** A local junction's ports still declare
`local-opaque@v1`, so a graph that puts one between registered stages still
reports one `element-contract-mismatch` per seam. What changed is that a fully
registered graph no longer has such an edge, not that a mixed one stopped
having them.

**The compiler needed nothing.** Port lists have existed on
`StageSpecification` since M0 and `GraphCompiler` reads them: it finds an edge's
port by name on the specification, compares the two ends' element contracts, and
requires every non-optional input and every non-ignorable output to carry an
edge. Multi-port registered nodes were validated correctly before any of them
could be authored, which is asserted rather than assumed — the same authored
document is reported against a catalog whose junction declares other port names
(two `unknown-output-port` and two `unconnected-output-port`) and against one
whose second leg declares another contract (one `element-contract-mismatch`, on
that leg alone).

## Occurrence names at attachment

Registered stages attach with an explicit occurrence name, mirroring the
slot-name rule and for the same reason — durable identities are
author-stable:

```csharp
Source<OrderCreated> source = Source.FromRegistered(streamSource, "orders-in", streamParameters);

RunnableGraph graph = source
    .Via(normalize, "normalize", normalizeParameters)
    .To(indexSink, "index-out", sinkParameters);
```

Parameters are `CanonicalJsonValue` payloads validated against the
specification's parameter contract by the graph compiler (and by the
specification's validator when it has one). Typed parameter builders are
provider-SDK sugar (M4); the M1 surface is honest raw payloads.

Mixing is legal at authoring: a chain may hold registered and lambda
stages, and closure works. But the implementation proved a limit worth
stating precisely: every local port declares the opaque `local-opaque@v1`
contract while a registered port declares a real one, so every
lambda-to-registered seam edge is a correct `element-contract-mismatch`
under the graph compiler — against any catalog, merged or not. Weakening
the contract rule to treat the opaque contract as a wildcard was considered
and rejected: it would blunt contract checking for every document to buy an
authoring convenience. Mixing is therefore an authoring and (future)
materialization affordance, not a definition-plane one; the
lambda-harness-around-a-registered-stage scenario becomes real when the
runtime-factory seam lets the local host execute registered stages.

Capability tokens are conditional and causal: `nondeployable` appears
exactly when a local-provider stage is present (local stages as a class are
nondeployable — a buffer carries no delegate, but `local/buffer@v1`
resolves in this process's provider and nowhere else), `ephemeral-identity`
exactly when an occurrence is auto-named, and the document's capabilities
are the union of every occurrence's declared requirements — so a registered
stage requiring `durable-state` closes into a document that declares it.
Through this API the two local tokens co-occur (lambdas cannot be named,
registered stages must be); they remain orthogonal in the model, where a
document carrying only one is hand-writable.

## PipelineDefinition

```csharp
PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create("orders"), GraphRevision.Create(3));
```

`AsPipeline` re-closes the graph's content under the real identity and
revision (the anonymous document's placeholder identity is replaced, so the
pipeline's fingerprint differs from the anonymous graph's — the fingerprint
is of the deployable document). It rejects, listing every violation at
once: any `nondeployable` token (a delegate has no durable behavior), any
`ephemeral-identity` token (machine names anchor nothing), and any
capability the target catalog does not know.

`PipelineDefinition` is a sealed value: `GraphId Id`, `GraphRevision
Revision`, `GraphDocument Document`, `GraphFingerprint Fingerprint`. Slots
of a pipeline bind by fingerprint and lineage, without an instance nonce
(ADR 0004 §4): registered behavior makes content identity meaningful.
Materialization of pipelines is the M3 Orleans host's concern; the local
host executes a registered graph when every stage resolves in a catalog it
was given and every provider has a factory it was given — which is exactly
the seam below, and is reachable since M4.5.

## Settled by the implementation

1. **Handles carry the specification, not the catalog.** Everything an
   attachment needs is on the specification; adjacency compatibility is
   contract equality already pinned by the `ElementContract<T>` values, and
   validation is the compiler's question against the host's catalog — a
   question no handle could answer.
2. **The runtime-factory seam was deliberately not invented here, and M4.5 is
   where it was invented publicly.** M1 shipped a registered occurrence that
   carries no binding, and a local host that refuses a registered graph with
   `unknown-stage` before planning. M3 built the engine-internal seam and the
   Orleans package published a mirror of it for silos. M4.5 promoted that
   mirror into the core package, where it is one seam serving both hosts —
   see the section below.
3. **Same chain, one vocabulary** — the registered overloads live on
   `Source`/`Flow`/`Sink`, separated from the lambda forms by arity.
4. **A junction's ports are the catalog's, never the factory's.** A provider
   states what its junction *does*; which ports it is wired at comes from the
   specification the catalog published, so a factory cannot disagree with its
   own catalog entry about its own shape.

## The runtime-factory seam, in the core package

What M1 declined to invent is now public API of `Orleans.Dataflow`, in the
`Orleans.Dataflow.Hosting` namespace, and it is one seam rather than two:

- **`IDataflowStageFactory`** — one factory per `ProviderId`, asked for every
  node of that provider. `Create(DataflowStageRequest)` receives the node as
  the document declares it and the specification it resolved to in the host's
  catalog, and nothing else: no document, no sibling node, no run identity, no
  services beyond what it was constructed with.
- **`DataflowStageRuntime`** — the executable form, in the shapes the engine
  runs and no others. Four linear (`Source`, `Element`, `ElementAsync`,
  `Terminal`) and, since M4.5, nine junctions: `Broadcast`, `Balance`,
  `Partition`, `Unzip`, `Merge`, `Concat`, `Interleave`, `Zip`,
  `CombineLatest`. A provider that wants a shape this type does not have is
  asking for a new engine primitive rather than a new stage.
- **`DataflowRunTokens`** — the run token, the stop token, and the run's
  identity, handed to a source opener once per run. A factory still receives no
  run identity: a stage request says what a stage *is*, and these say which run
  is opening it.
- **`ILocalDataflowBuilder`** — the in-process host's registration surface,
  member for member the mirror of `IOrleansDataflowBuilder` where the two hosts
  have the same question to answer: `AddCatalog`, `AddFactory`,
  `AddDotnetStages`, `AddObservable`. `new LocalDataflowHost(builder => …)`
  takes it.

The engine's own executor vocabulary stays internal, and one internal adapter
in the core package unwraps the public mirror for both hosts — so a silo and an
in-process host accept the same factory value and unwrap it identically. The
practical consequence is the one the design has claimed since phase 3 and can
now be checked for a provider's own vocabulary rather than only for the .NET
adapters: **a provider writes its stages once and they run in either runtime.**

```csharp
LocalDataflowHost host = new(builder => builder
    .AddCatalog(providerCatalog)
    .AddFactory(providerId, new MyStageFactory()));
```

The two halves are registered separately because different processes need
different halves (ADR 0001): a catalog is all a validator needs, and only a
host that will run the graph needs a factory. A host with the catalog and no
factory validates a document and refuses it at materialization, naming the
provider that has nothing to build it.

## What the multi-port half does not claim

Stated here rather than discovered later, in the style the runtime design uses
for its phases:

- **A junction's semantics are inherited rather than re-proved.** A registered
  junction is planned into the same segment, with the same `LocalFanOut` or
  `LocalFanIn` strategy value, as the local junction of that kind — so the
  memory bounds, the pause discipline, the drain-versus-abandon split, and the
  completion rules are literally the same code. What the tests prove is that a
  provider reaches it and that the elements come out right; they do not re-run
  the engine's junction suite through the seam, because there would be nothing
  different to measure.
- **A registered junction in a cycle is supported by construction and untested.**
  The planner's feedback machinery asks whether a node is a fan-in, and that
  question now has an answer for a registered node; no test closes a loop
  through one.
- **The public-surface claim is a discipline plus two checks.** The provider
  fixture is written against public API only, the seam is asserted to be public
  (including the junction factories, which a friend assembly would otherwise
  reach either way), and nothing the provider's own signatures name is internal.
  What is not checked is the inside of its method bodies: the test assembly is a
  friend of the core package and nothing in the language could stop one from
  reaching an internal.
- **Result-bearing junctions are refused rather than designed.** A junction
  declaring a result port is rejected at handle creation. Whether a junction
  should ever expose one is a question nothing has asked yet.
