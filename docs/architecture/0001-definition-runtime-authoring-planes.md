# ADR 0001: Separate authoring, definition, and runtime planes

- Status: Accepted for M0
- Date: 2026-08-16
- Amended by: [ADR 0002](0002-result-slots.md) (materialized-value shape)

## Context

Orleans.Dataflow needs an ergonomic typed C# API, a future idiomatic F# API, and a graph which can be validated, stored, transferred, upgraded, and executed across an Orleans cluster.

A graph which captures C# delegates or runtime objects is convenient locally but cannot be a stable distributed contract. A graph made only from untyped configuration is portable but loses compile-time composition safety. The architecture needs both without treating one representation as the other.

## Decision

The system has three distinct planes.

### Authoring plane

Typed immutable values such as `Source`, `Flow`, `Sink`, junctions, and `RunnableGraph` provide compile-time port compatibility and language-specific ergonomics. C# fluent methods and future F# modules are facades over the same graph algebra.

Authoring values do not execute work.

### Definition plane

The durable definition is a nongeneric immutable graph document containing only stable identifiers, versioned data contracts, node parameters, topology, declared capabilities, and named result slots.

The initial conceptual shape is:

```text
GraphDocument
  FormatVersion
  GraphId
  Revision
  Nodes[]
  Edges[]
  ResultSlots[]

StageNode
  NodeId
  StageRef { ProviderId, StageId, MajorVersion }
  ParameterContract
  Parameters
  ExecutionPolicy

Edge
  From { NodeId, PortId }
  To   { NodeId, PortId }
```

The definition plane never stores:

- delegates, closures, expression trees, or language-specific functions;
- tasks, channels, streams, grain references, service providers, or implementation instances;
- assembly-qualified CLR type names as contract identity;
- transient run handles or infrastructure credentials.

Port and parameter compatibility are resolved through explicit stable contract IDs and a trusted stage catalog registered by deployment code.

### Runtime plane

Materialization compiles a validated graph document against a stage catalog and selected host capabilities. The runtime creates executors, grain references, demand links, channels, storage sessions, resource leases, metrics, cancellation, and control handles.

Runtime values are created anew for each materialization. Durable identity is explicit and never inferred from object identity.

## Identity

These identities are distinct:

| Identity | Meaning |
|---|---|
| `StageRef` | Registered implementation family and compatibility major version. |
| `GraphId` + `Revision` | User-controlled deployable graph definition. |
| `NodeId` | Logical occurrence of a stage inside one graph lineage. |
| `RunId` | One materialized run. |
| `AttemptId` | A retry or failover incarnation within a run. |

Reusable graph fragments carry local node IDs. Importing a fragment requires a stable scope; composition deterministically rebases its internal IDs below that scope. Importing the same fragment twice produces two independent logical node sets.

Auto-generated identities may be used only for explicitly ephemeral local graphs. A graph cannot become durable or distributed until every durable identity is stable and validated.

## Registered behavior and local delegates

Durable and distributed stages reference trusted registered behavior through stable descriptors.

An optional ephemeral authoring surface may later accept delegates for local execution. Such a graph must carry an explicit nondeployable capability and the compiler must reject persistence, remote placement, resume, or distributed materialization. There is no silent attempt to serialize a closure.

## Options boundaries

Configuration remains separated by responsibility:

- source options: cursor, partitions, replay, source ordering, ingress credit;
- flow options: concurrency, ordering, state, checkpoint, supervision;
- sink options: batching, flush, acknowledgement, commit, idempotency;
- run options: placement, resource limits, cancellation, observability overrides;
- host options: catalogs, executors, persistence, cluster policy, defaults.

A single generic `DataflowOptions` bag is rejected because it would couple provider semantics and make future language APIs ambiguous.

## Materialized values

The definition plane represents materialized outputs as named, versioned result or control slots. The runtime plane creates their concrete values or proxies per materialization.

The exact public C# generic shape remains an M0 prototype decision. The graph IR must be able to support source, flow, sink, and lifecycle materialized outputs without persisting runtime objects. Durable graphs initially allow structural selection and composition of named slots; arbitrary result-combining delegates cannot be part of a persisted graph.

This comparison is resolved by [ADR 0002](0002-result-slots.md): stream shapes carry element types only (`Source<TOut>`, `Flow<TIn,TOut>`, `Sink<TIn>`), and materialized results are typed named result slots resolved through a run handle.

## Provider boundary

Deployment code registers a closed set of stage catalogs. Each catalog provides stable provider identity, stage specifications, parameter/result contracts, validators, planning capabilities, and runtime factories.

Graph data cannot request arbitrary assembly loading or instantiate a CLR type name. Before a run starts, eligible silos must prove compatible catalog capabilities. Heterogeneous placement is deferred until the homogeneous rule is correct and tested.

## Consequences

Positive consequences:

- C# and F# can be equal frontends;
- graph documents remain deterministic and reviewable;
- durable identity, checkpoint, and upgrade behavior have stable anchors;
- providers cannot silently execute arbitrary code named by untrusted graph data;
- local convenience cannot accidentally become a false durability promise.

Costs:

- distributed user-defined mapping requires explicit registration;
- graph compilation and catalog compatibility are first-class subsystems;
- materialized-result mapping is more constrained for durable graphs;
- the library must clearly distinguish ephemeral and deployable capabilities in diagnostics and docs.

## Rejected alternatives

### Serialize delegates or expression trees

Rejected because captured state, assembly identity, security, versioning, AOT, and cross-language behavior are not stable distributed contracts.

### Make `IAsyncEnumerable<T>` the graph model

Rejected because it does not retain topology, source ownership, partitioning, replay, checkpoint, placement, stable stage identity, or provider delivery semantics.

### Make a mutable C# builder the canonical graph

Rejected because it weakens reuse and thread safety and forces future F# through C# mutation and overload resolution.

### Dynamically load stage implementation types from graph data

Rejected because it makes the graph an executable payload rather than validated data and prevents closed deployment capability checks.
