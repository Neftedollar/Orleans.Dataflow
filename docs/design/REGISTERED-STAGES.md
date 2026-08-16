# Registered stages and pipeline definitions

- Status: M1 design for the deployable authoring surface; signatures settle
  with the implementation checkpoint
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

Mixing is legal: a chain may hold registered and lambda stages. The closed
document then carries `nondeployable` (and `ephemeral-identity` if anything
is unnamed) and stays local — useful for testing a registered stage inside
a lambda harness.

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
host may execute a pipeline's document when every stage resolves in a local
catalog with runtime factories — which is exactly the M2/M3 seam.

## Open questions for the implementation checkpoint

1. Whether `RegisteredStage.*` handles carry the catalog or only the
   specification (carrying the catalog enables cross-stage checks at
   authoring; carrying the specification keeps handles serializable-ish and
   host-agnostic).
2. The runtime-factory seam: M2's local runtime binds lambdas through the
   internal binding table; registered stages need
   `IStageRuntimeFactory`-shaped contracts (planned by DEFINITION-MODEL as
   the M2 addition) before a registered pipeline can execute locally.
3. Whether `Source.FromRegistered`/`Via(handle, ...)` overloads live on the
   existing types (keeping one chain) or a parallel `Pipeline`-flavored
   builder — current lean: same chain, one vocabulary, per the mixing rule
   above.
