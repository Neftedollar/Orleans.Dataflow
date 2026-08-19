# Compatibility

What Orleans.Dataflow runs on, what its public API is, and which shapes are
deliberate decisions rather than accidents.

> **Before 1.0.** The API, the runtime model, and the storage contracts are not
> stable yet, and no package is published. Everything below describes what is
> supported today; the forward promise at the bottom of this page starts at the
> 1.0 tag.

---

## Supported platforms

| Axis | Supported | Notes |
|---|---|---|
| .NET | `net10.0` | A single target by decision, not accident: .NET 10 is the current long-term-support release, and a narrower matrix is one the test suites actually cover. Widening to additional targets is additive and needs no breaking change. |
| SDK | `10.0.400`, pinned with `rollForward: disable` | The build refuses to roll forward, so every machine and the continuous-integration runner compile with the same compiler. |
| C# | 14 | The C# frontend compiles consumers on any compiler that targets `net10.0`. |
| F# | `FSharp.Core` 10.1.201 or later | The F# frontend declares an explicit floor and this repository pins it exactly. F# nullness checking is on, and the compiled surface carries honest nullable annotations — a C# consumer passing `null` where the F# surface forbids it gets a compiler warning. |
| Orleans | `10.2.2` | The version built and tested against, exactly. See the coupling table below for why only one assembly cares. |
| OS and architecture under test | Linux x64 on every push; macOS Arm64 in development | Nothing OS-specific is used. Windows is expected to work and is not in the tested set — that is a statement of test coverage, not of support refusal. |

---

## The five assemblies

| Assembly | Contains | References |
|---|---|---|
| `Orleans.Dataflow.Abstractions` | the definition plane: documents, identities, canonical serialization, the graph compiler | nothing but the framework |
| `Orleans.Dataflow` | the authoring surface, the local runtime, the provider seam, the .NET adapters | `Orleans.Dataflow.Abstractions` |
| `Orleans.Dataflow.FSharp` | the F# frontend | `Orleans.Dataflow`, `FSharp.Core` |
| `Orleans.Dataflow.Testing` | probes, a fault point, a marking sink, an in-memory checkpoint store, a test clock, the provider conformance kit | `Orleans.Dataflow` |
| `Orleans.Dataflow.Cluster` | the grains, the cluster host, the Orleans adapters | `Orleans.Dataflow` plus Orleans |

### Where Orleans can couple, and where it cannot

Orleans package references are isolated to one assembly by construction:

| Assembly | Orleans packages referenced |
|---|---|
| `Orleans.Dataflow.Abstractions` | none |
| `Orleans.Dataflow` | none |
| `Orleans.Dataflow.Testing` | none |
| `Orleans.Dataflow.FSharp` | none (`FSharp.Core` only) |
| `Orleans.Dataflow.Cluster` | `Microsoft.Orleans.Sdk`, `.Server`, `.Streaming`, `.BroadcastChannel`, `.Reminders` |

**The definition plane, the local runtime, the provider kit, and the F# frontend
cannot break with an Orleans release, because they cannot see one.** A deployment
that pins a different Orleans 10.x patch than 10.2.2 is expected to work under
Orleans' own compatibility discipline; it is not in the tested set until this
table says so.

`Orleans.Dataflow.Cluster` additionally carries a **generated** public surface:
the Orleans code generator emits public codec and proxy types (`OrleansCodeGen.*`)
into the assembly, and their signatures name Orleans runtime types directly. That
surface belongs to the Orleans SDK that emitted it, is coupled to its version, and
is explicitly outside this library's API guarantee.

---

## Namespaces and the packages they come from

A namespace here does not name its package, which is a decision rather than an
accident — but it means the namespace on a type is not enough to tell you what to
reference. This table is:

| Namespace | Assembly holding its public types |
|---|---|
| `Orleans.Dataflow` | `Orleans.Dataflow` |
| `Orleans.Dataflow.Authoring` | `Orleans.Dataflow` |
| `Orleans.Dataflow.Adapters` | `Orleans.Dataflow` **and** `Orleans.Dataflow.Cluster` |
| `Orleans.Dataflow.Hosting` | `Orleans.Dataflow` **and** `Orleans.Dataflow.Cluster` |
| `Orleans.Dataflow.Definition` | `Orleans.Dataflow.Abstractions` |
| `Orleans.Dataflow.Identity` | `Orleans.Dataflow.Abstractions` |
| `Orleans.Dataflow.Serialization` | `Orleans.Dataflow.Abstractions` |
| `Orleans.Dataflow.Compilation` | `Orleans.Dataflow.Abstractions` |
| `Orleans.Dataflow.Grains` | `Orleans.Dataflow.Cluster` |
| `Orleans.Dataflow.Testing` | `Orleans.Dataflow.Testing` |
| `Orleans.Dataflow.FSharp` | `Orleans.Dataflow.FSharp` |

**Two rows carry the practical consequence.**

`Orleans.Dataflow.Hosting` holds `ILocalDataflowBuilder`, `ICheckpointStore`, and
the stage-factory seam (`IDataflowStageFactory`, `DataflowStageRequest`,
`DataflowStageRuntime`, `DataflowRunTokens`) from the core package, and
`OrleansDataflowHost`, `OrleansRunHandle`, `DurablePipelineOptions`, and the silo
and client builders from the Orleans package.

`Orleans.Dataflow.Adapters` holds `DotnetStages` and `ObservableBinding` from the
core package, and the grain, stream, and broadcast bindings from the Orleans
package.

In both cases the namespace is shared because **the seam is shared**: one seam
serves a silo and a `LocalDataflowHost` alike, and a provider that never
references Orleans can still write a factory. The cost is this table, and it is
the cheaper half of the trade.

`Orleans.Dataflow.Runtime` and `Orleans.Dataflow.Diagnostics` are absent from the
table because they contain no public types at all; they are internal to
`Orleans.Dataflow`.

---

## What the public API is

The guaranteed surface is the **hand-written public API of the five assemblies**,
and it is guarded twice:

- the Roslyn **PublicAPI analyzer** on every C# project, which fails the build at
  the declaration site when the surface changes without the accompanying text-file
  edit — the incremental, review-friendly guard;
- a **reflection surface snapshot** test per assembly, which records what the
  analyzer is blind to — generic-parameter variance, base types, implemented
  interfaces, and attributes including `[Id(n)]` numbering — and which is the only
  guard the F# assembly has, since the analyzer does not run under the F# compiler.

### Outside the guarantee, by decision

- **`OrleansCodeGen.*` generated types** — the Orleans SDK's contract, not this
  library's.
- **Grain interfaces and the types they exchange.** The interfaces are
  `IPipelineCoordinatorGrain`, `IPipelineRunGrain`, `IObserverBridgeGrain`,
  `IReminderTriggerGrain`, and `IDataflowPushReceiver`; the payloads they pass are
  `RunStatusSnapshot`, `ResultEnvelope`, `DurableRunClaim`,
  `DurableRunDeclaration`, `DataflowPushOutcome`, and `RunPhase`. All of them are
  public because Orleans proxies require it, and all of them are infrastructure by
  intent — the wire protocol between a client and a silo. **They are not
  documented in this reference** and no page here describes them, deliberately:
  the supported way to reach a run is
  [`OrleansDataflowHost`](hosting.md#the-cluster-host) and the
  [handle](run-handles.md) it returns, whose `RunSnapshot`, `RunEnding`, and
  `PipelineRunTicket` are the guaranteed readings of the same facts. A caller
  invoking grain methods directly is on the wire protocol, not the API.
- **Internals reached through friend grants.** The `InternalsVisibleTo` list
  exists so the F# frontend binds to the shared core and the test suites see the
  seams: `Orleans.Dataflow` grants to `Orleans.Dataflow.Cluster`,
  `Orleans.Dataflow.Testing`, `Orleans.Dataflow.FSharp`, and the four test
  assemblies. Internals are not a compatibility surface and not a security
  boundary. The grants are unkeyed because the assemblies are not strong-named —
  the modern .NET norm for `net10.0`-only libraries with no .NET Framework
  consumers.

---

## Wire and store contracts

- **`[Id(n)]` numbering** on the types that cross grain boundaries is a wire
  contract. Round-trip tests prove a serializer exists; the surface snapshot is
  what pins the numbers, because a round trip within one build always agrees with
  itself.
- **Graph fingerprints** — canonical bytes, SHA-256 — are pinned by golden
  compatibility tests. The same inputs must produce the same bytes across
  versions, or a stored checkpoint could refuse its own document.
- **Checkpoint documents carry no cross-revision migration.** A resume requires
  the same fingerprint and the same revision, or it is refused with
  [`PipelineResumeRefusedException`](errors.md#pipelineresumerefusedexception).
  Migrating a checkpoint across a changed document is not something a cluster will
  guess at; the deliberate alternative is
  [`ReplaceDurableRunAsync`](hosting.md#replacing-and-retiring), which destroys
  the stored position on purpose.

### Coordinator limits, which are part of what a document may be

| Limit | Value |
|---|---|
| canonical document bytes a coordinator decodes | 4 MiB |
| nodes per document | 10 000 |
| declared durable run identities per pipeline | 1 000 |
| result bytes a silo will send, by default | 1 MiB, [configurable](hosting.md#silo-settings) |
| legs on a local junction | 2 to 8 |

The first three are refusals on the coordinator's own turn, where nothing else
about the pipeline is answered while the work runs. See
[errors](errors.md#pipelinerejectedexception).

---

## Shapes that are decisions, not defects

**The local and cluster run handles are deliberately not polymorphic.**
`RunHandle` and `OrleansRunHandle` share verbs where the semantics are the same —
`Completion`, `WatchTermination`, `ShutdownAsync`, result reads, `DisposeAsync` —
and diverge where the capabilities do: pause, resume, and the synchronous
`Snapshot()` are local affordances; the epoch, the ticket, the run identity, and
the asynchronous `SnapshotAsync` exist because the cluster handle reads a run that
lives elsewhere. A shared base type would either flatten those to the smallest set
or throw `NotSupportedException` from the rest, and both are worse than two honest
types. See [run handles](run-handles.md#the-two-handles-at-a-glance).

**Neither handle exposes a `CancelAsync`.** Cancelling is `DisposeAsync`, or the
token you passed at materialization. There is one verb for the abrupt stop rather
than two spellings of it.

**Cancellation-token absences are stated at the member.** Where a public
asynchronous member takes no token, its documentation says why — `ResumeAsync` and
`ShutdownAsync` are requests that cannot be unsent. Properties (`Completion`,
`WatchTermination`) compose with `WaitAsync`. Members accepting an asynchronous
enumerable hand the run's token to `GetAsyncEnumerator`, so the enumeration is
cancelable even though the accepting signature carries no token.

**Mixed-language consumers can hit simple-name collisions.**
`Orleans.Dataflow.FSharp.{Source, Flow, Sink}` mirror the C# names by design — the
two frontends are equal spellings of one algebra. A C# file that opens both
namespaces disambiguates with a `using` alias; an F# file shadows by `open` order,
which is why the F# samples do not `open Orleans.Dataflow` at all. This is a
papercut, priced and accepted, in exchange for neither frontend carrying a mangled
name.

**Two types are externally derivable** — `DataflowSinkMark` and
`DataflowSourceCursor`, the [provider SDK's](provider-sdk.md#cursors-and-marks)
two extension points. Every other hand-written public class is sealed or static.
The 1.x policy for them: additions arrive as virtual members with working
defaults, never as new abstract members, because an abstract addition breaks every
provider compiled before it.

**Four public methods cannot be called.** The
[compile-error guards](operators.md#closing-a-graph) on `Source<T>.To` and
`Flow<TIn, TOut>.To` exist so that closing a graph with a result-bearing sink and
no name for the result is a compiler error with a useful message rather than a
cast that silently drops the result.

---

## Trim and Native AOT

**No trim or AOT claim is made.** The honest state, assembly by assembly,
established with the trim and AOT analyzers:

| Assembly | State |
|---|---|
| `Orleans.Dataflow.Abstractions` | analyzer-clean. |
| `Orleans.Dataflow` | one genuine reflection site remains: the delegate adapter closes generic templates over types recovered at run time, which is inherent to a delegate-based authoring surface, not an oversight. |
| `Orleans.Dataflow.Cluster` | cannot claim compatibility regardless of this repository's code — Orleans 10.2.2 itself carries no `IsTrimmable` or `IsAotCompatible` annotation on any of the assemblies this library references, so reference-compatibility checks warn on every one of them. |
| `Orleans.Dataflow.Testing` | the provider conformance kit reflects over provider assemblies by design; it is a diagnostic tool. |
| `Orleans.Dataflow.FSharp` | the IL analyzers do not run under the F# compiler, so a claim here would be unverified by tooling; none is made. |

A deployment that trims anyway is running ahead of both this library's claims and
Orleans' own.

---

## The forward promise

Within 1.x the hand-written public surface only widens. The PublicAPI baselines
move from unshipped to shipped at the 1.0 tag, and every later change to a shipped
entry or a surface snapshot is a reviewed, deliberate diff.

Three things are explicitly not covered by that promise and are listed again
because they are the ones a consumer is likeliest to reach for: the generated
Orleans codec types, the grain interfaces, and anything behind a friend grant.

---

## Related

- [Hosting](hosting.md) — what a silo and a client have to register.
- [Errors](errors.md) — what each refusal above looks like at run time.
- [Deploying](../operations/deploying.md) — what a deployment owes the library.
- [Checkpoint stores](../operations/checkpoint-stores.md) — the one storage
  contract a deployment implements.
