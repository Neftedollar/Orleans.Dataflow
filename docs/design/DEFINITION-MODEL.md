# Definition-plane model

- Status: M0 implementation specification
- Depends on: [ADR 0001](../architecture/0001-definition-runtime-authoring-planes.md), [ADR 0002](../architecture/0002-result-slots.md), [ADR 0003](../architecture/0003-canonical-graph-serialization.md)

This document specifies the immutable graph document, the stage catalog
contracts, and the validation rules that M0 implements. It is the contract for
implementation checkpoints; deviations require updating this document in the
same checkpoint.

## Identifier primitives

All identifiers share one segment grammar: `[a-z0-9]+(-[a-z0-9]+)*`, length
1..64, ASCII only, ordinal comparison. `NodeId` is a `/`-joined path of
segments (max depth 16, max canonical length 256); every other identifier is a
single segment.

| Type | Meaning |
|---|---|
| `ProviderId` | Registered stage provider family. |
| `StageId` | Stage family within one provider. |
| `StageRef` | `ProviderId` + `StageId` + major version: one registered implementation family. |
| `GraphId` | User-controlled deployable graph identity. |
| `GraphRevision` | Positive integer revision of one graph identity; first revision is 1. |
| `NodeId` | Logical occurrence of a stage inside one graph lineage; hierarchical for import scoping. |
| `PortId` | Port name on a stage specification. |
| `ResultSlotId` | Named result or control slot of a graph. |
| `ContractId` | Stable identity of a data, parameter, policy, or result contract. |
| `RunId` | One materialized run (runtime plane; never stored in a document). |
| `AttemptId` | One retry/failover incarnation within a run (runtime plane; never stored in a document). |

Identity rebasing: importing a fragment under scope segment `s` maps every
internal `NodeId` `p` to `s/p`. Rebasing is pure prefixing, so it is
deterministic, composable, and collision-free across distinct scopes.

## Contract references

A `ContractReference` is `ContractId` plus a positive integer major version.
Contract identity is never a CLR type name. Two contract references are
compatible only when their `ContractId` and major version are equal;
finer-grained compatibility (additive minor versions) is a post-M0 extension
and is deliberately absent from the M0 model.

Element types flowing between ports are identified by contract references
declared in stage specifications, not stored per edge. Validation resolves
them through the catalog.

## Graph document

The graph document is the only durable representation of a graph. It is
nongeneric, immutable, and contains no behavior.

```text
GraphDocument
  FormatVersion        int, currently 1
  GraphId              GraphId
  Revision             GraphRevision
  DeclaredCapabilities sorted set of capability tokens (segment grammar)
  Nodes                sorted list of StageNode (by NodeId)
  Edges                sorted list of Edge (by From.Node, From.Port, To.Node, To.Port)
  ResultSlots          sorted list of ResultSlotDefinition (by ResultSlotId)

StageNode
  NodeId               NodeId
  StageRef             StageRef
  ParameterContract    ContractReference
  ParameterPayload     canonical JSON value (ADR 0003 payload rules)
  ExecutionPolicyContract ContractReference (optional: absent means provider default)
  ExecutionPolicyPayload  canonical JSON value (present iff the contract is present)

Edge
  From                 PortAddress { NodeId, PortId }   an output port
  To                   PortAddress { NodeId, PortId }   an input port

ResultSlotDefinition
  ResultSlotId         ResultSlotId
  ResultContract       ContractReference
  Producer             PortAddress { NodeId, ResultPortId }
```

Capability tokens mark facts validation and hosts must honor. The initial
vocabulary: `nondeployable` (contains locally registered behavior and must
never be persisted, resumed, or placed remotely). Further tokens are added
with the features that need them.

### Structural invariants (enforced at document construction)

1. `NodeId` values are unique.
2. Every `Edge` endpoint and every `ResultSlotDefinition.Producer` references
   an existing node.
3. No self-loop edge (`From.Node == To.Node`) in M0; cycles arrive with an
   explicit boundary contract in a later milestone.
4. `ResultSlotId` values are unique.
5. At most one edge terminates at any given input `PortAddress` (fan-in is a
   junction stage, not edge multiplicity).
6. At most one edge originates at any given output `PortAddress` (fan-out is
   a junction stage, not edge multiplicity).
7. Execution policy contract and payload are present together or absent
   together on a node.
8. The document is closed under its own references; nothing points outside.

Structural invariants do not require a catalog. A structurally valid document
can still be semantically invalid against a catalog.

### Catalog invariants (enforced by the graph compiler)

1. Every `StageRef` resolves to a registered stage specification.
2. Every edge connects an output port to an input port that both exist on the
   resolved specifications, with equal element contract references.
3. Every input port of every node is connected exactly once unless the
   specification marks it optional; every output port is connected or the
   specification marks it ignorable.
4. Every `ParameterPayload` validates against its declared parameter
   contract, and the declared `ParameterContract` matches the specification.
5. Every result slot's `Producer` port exists on the resolved specification
   as a result port with a matching result contract.
6. Declared capabilities are consistent with the stages used (a document
   using a nondeployable-only stage must declare `nondeployable`).
7. Unknown `FormatVersion` fails before any other rule runs.

Diagnostics carry the failing rule, the offending identity, and no CLR type
names. Validation reports all violations it can find, not only the first.

## Canonical envelope layout (format version 1)

The fixed schema property order that ADR 0003 requires, with camelCase JSON
names. Every property is always written in this order; an absent optional
value is an explicit JSON `null`, never an omitted property.

| Object | Property order |
|---|---|
| `GraphDocument` | `formatVersion`, `graphId`, `revision`, `capabilities`, `nodes`, `edges`, `resultSlots` |
| `StageNode` | `nodeId`, `stageRef`, `parameterContract`, `parameters`, `executionPolicyContract`, `executionPolicy` |
| `StageRef` | `providerId`, `stageId`, `majorVersion` |
| `ContractReference` | `contractId`, `majorVersion` |
| `PortAddress` | `nodeId`, `portId` |
| `Edge` | `from`, `to` |
| `ResultSlotDefinition` | `resultSlotId`, `resultContract`, `producer` |

Identifiers serialize as their canonical text; revisions and major versions
as integers; capabilities as an array of token strings in ordinal order;
`parameters` and `executionPolicy` as embedded canonical JSON values.
`executionPolicyContract` and `executionPolicy` are `null` together or
present together.

## Stage catalog contracts

Deployment code registers a closed set of catalogs at startup; graph data can
never cause code loading.

```text
StageSpecification
  StageRef
  InputPorts    list of PortSpecification { PortId, ElementContract, Optional }
  OutputPorts   list of PortSpecification { PortId, ElementContract, Ignorable }
  ResultPorts   list of ResultPortSpecification { PortId, ResultContract }
  ParameterContract   ContractReference
  RequiredCapabilities  set of capability tokens
  Validator     parameter-payload validation hook
```

A catalog exposes lookup by `StageRef` and enumeration for fingerprinting.
The catalog fingerprint (SHA-256 over the canonical serialization of all
specifications) supports the later cross-silo compatibility checks; M0 only
defines and tests the fingerprint's determinism.

Runtime factories are part of the runtime plane and are intentionally absent
from the M0 catalog contract; the local runtime milestone (M2) adds them
without changing the definition contracts above.

## Not in M0

- minor-version contract compatibility;
- cycle liveness rules;
- checkpoint/durability metadata in the document;
- heterogeneous catalog placement;
- any runtime execution.

## Implementation checkpoints

1. Identifier primitives with grammar tests.
2. Document model records with structural invariants and tests.
3. Canonical writer/reader, `GraphFingerprint`, golden fixtures.
4. Catalog contracts, catalog fingerprint, graph compiler with catalog
   invariants and diagnostics tests.
5. Language-neutral composition/rebasing helpers over the document model,
   bridging into the M1 C# authoring API.
