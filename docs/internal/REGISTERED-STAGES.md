# Registered stages and pipeline definitions

- Status: M1 design for the deployable authoring surface, extended by M4.5 with
  multi-port registered stages and the public runtime-factory seam, and by
  M4.5b with the conformance kit and the typed-parameter-builder pattern;
  signatures settle with the implementation checkpoint
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
specification's validator when it has one). The M1 surface is honest raw
payloads, and M4.5 keeps that promise rather than replacing it: the typed
parameter builders below write those very bytes — see *Typed parameter
builders* — so a provider that adopts them changes no document and no
fingerprint.

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

## Typed parameter builders

The M1 note promised sugar for payloads in M4. What ships is a **pattern
rather than a framework**, and the reason is that both real providers in this
repository had already grown it: `OrleansStages` has ten typed writers
(`StreamSourceParameters`, `GrainCallParameters`, `ReminderTriggerParameters`,
…) and `DotnetStages` has two. There was nothing left to invent, only something
to name and to check.

**A provider's payload lives in exactly three places, and the pattern is that
they are three views of one statement:**

| Place | What it is | Example |
|---|---|---|
| The member names and one reader | `internal static class XxxPayload` with `const string` members, `Write`, and `TryRead` | `OrleansStagePayloads.cs`, `DotnetStagePayloads.cs` |
| A typed writer per stage | `public static CanonicalJsonValue XxxParameters(…)` on the vocabulary type | `OrleansStages.ReminderTriggerParameters(period, ingress)` |
| A validator over the reader | `IStageParameterValidator` that runs `TryRead` and answers its violations | `OrleansStageValidator`, `DotnetStageValidator` |

and the factory that executes the stage reads the node's payload through **the
same `TryRead`**, so a member renamed in one place stops compiling in the other
three.

```csharp
public static CanonicalJsonValue ReminderTriggerParameters(TimeSpan period, BufferOptions ingress)
{
    ArgumentNullException.ThrowIfNull(ingress);
    ArgumentOutOfRangeException.ThrowIfLessThan(period.TotalMilliseconds, 1, nameof(period));

    if (ingress.OverflowPolicy is OverflowPolicy.Backpressure)
    {
        throw new ArgumentException("A reminder trigger cannot backpressure a cluster reminder…", nameof(ingress));
    }

    return ReminderTriggerPayload.Write(period, ingress);
}
```

**What the writer buys is a refusal at the line the author wrote.** A period of
zero, a backpressuring ingress for a clock, a mode this vocabulary does not
have — each is an `ArgumentException` at the call rather than a diagnostic when
the graph closes, and none of them can be spelled at all when the value is an
enumeration. **What it cannot buy is the check**, and that is why the validator
is not optional: a document reaching a silo was not necessarily written through
the builder. It may be hand-authored, from another version, or from another
provider entirely, and the reader is the only thing standing between it and the
factory.

**The builders are sugar over the raw payload and nothing more**, which is what
makes them safe to adopt: `SplitParameters(SplitMode.Broadcast)` writes
`{"mode":"broadcast"}`, byte for byte what the literal wrote, so documents and
fingerprints are unchanged (`JunctionParameterTests`). The definition plane
never learns that a builder exists.

**A generic builder framework was considered and rejected.** A fluent
`PayloadBuilder` with typed member descriptors would have to describe what each
reader already states in twenty lines of ordinary C#, and it would buy one
thing — deriving the reader from the writer — at the price of a second way to
spell a payload, a reflection or source-generation step in a package that has
neither, and a shape fixed before three providers exist. The repository has
twice preferred an honest pattern to a framework (the `Local*Parameters` types,
the probe stages), and this is the third time. The smallest complete instance
is `JunctionModePayload` in the test provider: one member, one closed set of
values, a writer, a reader, a validator, and a factory reading through it.

## Conformance: `ProviderConformance`

A provider ships a catalog and a factory, and everything that can go wrong
between them goes wrong quietly. `Orleans.Dataflow.Testing.ProviderConformance`
is the mechanical half of the provider SDK: a provider author points it at
their own registration plus one accepted payload per stage, and gets nine
checks that were previously nine hand-written tests per provider.

```csharp
public static TheoryData<string> Checks => [.. ProviderConformance.Checks];

[Theory]
[MemberData(nameof(Checks))]
public void TheProviderConforms(string check) => Kit().Check(check);

private static ProviderConformance Kit() =>
    ProviderConformance.Create(
        MyStages.Provider,
        MyStages.Catalog,
        new MyStageFactory(registry),
        [ProviderStageSample.Create(MyStages.ReadStage, MyStages.ReadParameters(…))]);
```

One theory over `Checks` is the whole of what an author writes, and a check
added to the kit becomes a test in every provider's suite without that file
changing. Nothing in the kit names a test framework: a failure is a
`ProviderConformanceException` carrying every violation the check found, in the
numbered form this project uses for every other report.

The nine checks, and where each one came from:

| Check | What it asserts | Extracted from |
|---|---|---|
| `EveryPortCarriesADeclaredContractInCanonicalOrder` | Every port declares a created contract, names are unique across the stage, each port list is in ordinal order of its names, and a stage declares at least one port | The canonical-order rule the junction handles, the planner, and a provider's own router all read |
| `EveryStagesPayloadIsReadByAValidatorThatRefusesWhatItDoesNotDeclare` | The stage has a reader; it accepts the sample; it refuses an added member, each removed required member, each retyped member, and a payload that is not an object — naming the member in single quotes each time; and it accepts a removed *optional* member | The unknown-member refusal every adapter payload performs |
| `TheCatalogFingerprintIsTheSameForEveryRegistrationOfTheSameStages` | Registration order does not change the fingerprint, two reads of the catalog do not, and a changed parameter contract does | The catalog fingerprint a cluster negotiates on |
| `TheFactoryAnswersForEveryStageTheCatalogDeclares` | The factory builds a non-null runtime for every declared stage | One registration per vocabulary: half a vocabulary fails at the first element |
| `TheFactoryRefusesAStageTheCatalogDoesNotDeclare` | An unknown stage id and an unregistered major version are refused by throwing, naming the stage, and not by a null reference, an index, a missing key, or a cast | The explicit lookup every factory here writes instead of dereferencing |
| `EveryRuntimeHasTheShapeItsSpecificationDeclares` | Port counts imply a shape and the built runtime is that shape; a terminal produces a result exactly when the stage declares a result port; an unzip's projections match the leg count | The M4.5a negative tests — `enrich-miscast` and the three-legged fan-out that split a row into two — generalized |
| `EveryStageHasATypedHandleThatRefusesTheWrongShape` | The handle the specification implies is creatable, a handle of another shape is refused, and a contract no port declares is refused | Handle-creation validation, which turns a catalog mismatch into an `ArgumentException` at the author's own line |
| `NoParameterPayloadNamesAClrType` | No string in a payload resolves to a `Type`, and none is assembly-qualified | ADR 0001: a document causes no code loading |
| `NoCoreOptionTypeNamesAnythingOfThisProvider` | No public `*Options` type of the core packages names a type of the provider's assembly or namespace | The M4 exit criterion "provider packages do not leak their configuration into core option types" |

**The kit refuses to measure nothing.** A catalog declaring no stage of the
named provider, a declared stage with no sample, and a sample naming a stage
the catalog does not declare are all refused at `Create`, because a green suite
that measured nothing reads exactly like a green suite that measured
everything.

**Its first consumers are the two vocabularies this repository ships.** The
.NET adapters run it in the core suite against `DotnetStages.Publish` and
`DotnetStageFactory`; the Orleans adapters run it inside the cluster collection
against `OrleansStages.Publish` and `OrleansStageFactory`, because building a
stream stage resolves a stream provider and building a reminder trigger reads
the cluster's own minimum period. Both pass every check. **One change was
needed to make that possible and it is a real one**: `DotnetStageFactory`
implemented the engine's internal factory interface, one unwrap closer to the
planner than the seam this milestone published, so the SDK's own vocabulary
could not be pointed at the SDK's own kit. It now implements
`IDataflowStageFactory` and is unwrapped through `DataflowStageFactoryAdapter`
exactly as the Orleans adapter factory already was, which also makes "a
provider writes its vocabulary once and both hosts take it" true of the
vocabulary shipped inside the core package.

**The kit is checked as an instrument before anything is asserted through it.**
`ProviderConformanceTests` points each check at a provider broken in the one
way that check is about — a stage with no port, a reader that lets an unknown
member through, a reader whose refusals name nothing, a catalog that publishes
a different vocabulary on every read, a factory that does not implement a stage
its own catalog declares, a factory that builds a stranger, one that fails on a
stranger by accident, one that refuses without naming, a factory that builds a
junction where its catalog declares a chain, a terminal that produces no result
for a stage declaring one, an unzip that splits a row into fewer parts than it
has legs, a junction no typed handle can author, and a payload naming a CLR
type — with a correct control that passes all nine.

**What the kit does not check, stated rather than discovered:**

- **Semantics.** Whether a source really ends its sequence on a stop token,
  whether a terminal's fold is associative, whether an adapter's acknowledgement
  boundary is where its documentation says it is: none of that is derivable from
  a catalog and a factory. ADAPTERS.md is where those answers live and a
  provider's own tests are what prove them.
- **The runtime it builds is never run.** The factory is asked to build and the
  shape of what it built is read; nothing is opened, pulled, folded, or
  disposed. A source that throws on its first `MoveNextAsync` passes every check
  here.
- **Two checks cannot fail from a test assembly, and it is a property of what
  they assert.** The canonical-order and unique-name clauses re-derive
  invariants `StageSpecification.Create` already enforces, so a specification
  breaking them cannot be constructed through the public factory at all — what
  is falsifiable there is the clause that factory does *not* enforce, a stage
  with no port, and that one has a test. And
  `NoCoreOptionTypeNamesAnythingOfThisProvider` fails only when a type shipped in
  the core package names a type of the provider's, which no test can arrange: it
  guards a future change to `Orleans.Dataflow` rather than reporting a present
  state.
- **The samples are the provider's own claim.** The kit mutates one payload per
  stage, so a member the sample omits is a member nothing is checked about. The
  sample should be the fullest payload the stage accepts, with its genuinely
  optional members named as such.

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
