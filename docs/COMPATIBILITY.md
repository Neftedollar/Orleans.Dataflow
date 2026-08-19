# Compatibility

What Orleans.Dataflow runs on, what its public API is, and which shapes are
deliberate decisions rather than accidents. Everything here was established by
the M8.3 API review — the claims below are the reviewed ones, with their
boundaries named.

## Supported platforms

| Axis | Supported at 1.0 | Notes |
|---|---|---|
| .NET | `net10.0` (LTS) | Single target by decision, not accident: .NET 10 is the current long-term-support release, and a narrower matrix is one the test suites actually cover. Widening to additional targets after 1.0 is additive and needs no breaking change. |
| C# | 14 | The C# frontend compiles consumers on any compiler that targets `net10.0`. |
| F# | FSharp.Core `10.1.201` or later | The F# frontend declares an explicit floor; central pinning in this repo holds it exactly. F# nullness checking is on (`--checknulls+`), and the compiled surface carries honest nullable annotations — a C# consumer passing `null` where the F# surface forbids it gets a compiler warning. |
| Orleans | `10.2.2` | The version built and tested against, exactly. See the coupling table below for why only one assembly cares. |
| OS / arch under test | Linux x64 (CI, every push), macOS Arm64 (development, every suite) | Nothing OS-specific is used; Windows is expected to work and is not in the tested set — that is a statement of test coverage, not of support refusal. |

## Where Orleans can couple, and where it cannot

Orleans package references are isolated to one assembly by construction:

| Assembly | Orleans packages referenced |
|---|---|
| `Orleans.Dataflow.Abstractions` | none |
| `Orleans.Dataflow` | none |
| `Orleans.Dataflow.Testing` | none |
| `Orleans.Dataflow.FSharp` | none (`FSharp.Core` only) |
| `Orleans.Dataflow.Orleans` | `Microsoft.Orleans.Sdk`, `.Server`, `.Streaming`, `.BroadcastChannel`, `.Reminders` |

The definition plane, the local runtime, the provider kit, and the F#
frontend cannot break with an Orleans release, because they cannot see one.
A deployment that pins a different Orleans 10.x patch than `10.2.2` is
expected to work under Orleans' own compatibility discipline; it is not in
the tested set until this table says so.

`Orleans.Dataflow.Orleans` additionally carries a **generated** public
surface: the Orleans code generator emits public codec and proxy types
(`OrleansCodeGen.*`) into the assembly, and their signatures name Orleans
runtime types directly. That surface belongs to the Orleans SDK that emitted
it, is coupled to its version, and is explicitly outside this library's API
guarantee below.

## Namespaces and the packages they come from

A namespace here does not name its package, and that is a decision rather than
an accident — but it means the namespace on a type is not enough to tell you
what to reference. This table is:

| Namespace | Assembly holding its public types |
|---|---|
| `Orleans.Dataflow` | `Orleans.Dataflow` |
| `Orleans.Dataflow.Authoring` | `Orleans.Dataflow` |
| `Orleans.Dataflow.Adapters` | `Orleans.Dataflow` **and** `Orleans.Dataflow.Orleans` |
| `Orleans.Dataflow.Hosting` | `Orleans.Dataflow` **and** `Orleans.Dataflow.Orleans` |
| `Orleans.Dataflow.Definition` | `Orleans.Dataflow.Abstractions` |
| `Orleans.Dataflow.Identity` | `Orleans.Dataflow.Abstractions` |
| `Orleans.Dataflow.Serialization` | `Orleans.Dataflow.Abstractions` |
| `Orleans.Dataflow.Compilation` | `Orleans.Dataflow.Abstractions` |
| `Orleans.Dataflow.Grains` | `Orleans.Dataflow.Orleans` |
| `Orleans.Dataflow.Testing` | `Orleans.Dataflow.Testing` |
| `Orleans.Dataflow.FSharp` | `Orleans.Dataflow.FSharp` |

Two rows carry the practical consequence. **`Orleans.Dataflow.Hosting`** holds
`ILocalDataflowBuilder`, `ICheckpointStore`, and the stage-factory seam
(`IDataflowStageFactory`, `DataflowStageRequest`, `DataflowStageRuntime`,
`DataflowRunTokens`) from the core package, and `OrleansDataflowHost`,
`OrleansRunHandle`, `DurablePipelineOptions`, and the silo and client builders
from the Orleans package. **`Orleans.Dataflow.Adapters`** holds `DotnetStages`
and `ObservableBinding` from the core package, and the grain, stream, and
broadcast bindings from the Orleans package. In both cases the namespace is
shared because the *seam* is shared: the factory mirror moved into the core
package in M4.5 under the same names in the same namespace precisely so that one
seam serves a silo and a `LocalDataflowHost` alike, and a provider that never
references Orleans can still write a factory. The cost is this table, and it is
the cheaper half of the trade.

`Orleans.Dataflow.Runtime` and `Orleans.Dataflow.Diagnostics` are absent because
they contain no public types at all; they are internal to `Orleans.Dataflow` and
are reached only through the friend grants described below.

## What the public API is

The guaranteed surface is the **hand-written public API of the five
assemblies**, and it is guarded twice:

- the Roslyn **PublicAPI analyzer** on every C# project, which fails the
  build at the declaration site when the surface changes without the
  accompanying text-file edit — the incremental, review-friendly guard;
- a **reflection surface snapshot** test per assembly, which records what
  the analyzer is blind to — generic-parameter variance, base types,
  implemented interfaces, and attributes including `[Id(n)]` numbering —
  and which is the only guard the F# assembly has (the analyzer does not
  run under `fsc`).

Outside the guarantee, by decision:

- **`OrleansCodeGen.*` generated types** — the Orleans SDK's contract, not
  this library's.
- **Grain interfaces** (`IPipelineCoordinatorGrain`, `IPipelineRunGrain`,
  `IObserverBridgeGrain`, `IReminderTriggerGrain`, `IDataflowPushReceiver`)
  — public because Orleans proxies require it, infrastructure by intent.
  The supported way to reach a run is `OrleansDataflowHost` and the handle
  it returns; a caller invoking grain methods directly is on the wire
  protocol, not the API.
- **Internals reached through friend grants.** The `InternalsVisibleTo`
  list exists so the F# frontend binds to the shared core and the test
  suites see the seams; internals are not a compatibility surface and not a
  security boundary. The grants are unkeyed because the assemblies are not
  strong-named — the modern .NET norm for `net10.0`-only libraries with no
  .NET Framework consumers.

## Wire and store contracts

- **`[Id(n)]` numbering** on the types that cross grain boundaries is a wire
  contract. The round-trip tests prove a serializer exists; the surface
  snapshot is what pins the numbers, because a round-trip within one build
  always agrees with itself.
- **Graph fingerprints** (canonical bytes, SHA-256) are pinned by golden
  compatibility tests — the same inputs must produce the same bytes across
  versions, or a stored checkpoint could refuse its own document.
- **Checkpoint documents** follow ADR 0007: no cross-revision migration in
  v1; same fingerprint and same revision, or refuse.

## Shapes that are decisions, not defects

- **The local and cluster run handles are deliberately not polymorphic.**
  `RunHandle` and `OrleansRunHandle` share verbs where the semantics are the
  same (`Completion`, `WatchTermination`, `ShutdownAsync`, `CancelAsync`,
  result reads, `DisposeAsync`) and diverge where the capabilities do:
  pause/resume and the synchronous `Snapshot()` are local affordances; the
  epoch, ticket, and asynchronous `SnapshotAsync` exist because the cluster
  handle reads a run that lives elsewhere. A shared base type would either
  flatten those to the smallest set or throw `NotSupportedException` from
  the rest, and both are worse than two honest types.
- **Cancellation-token absences are stated at the member.** Where a public
  async member takes no token, its documentation says why (`ResumeAsync`,
  `ShutdownAsync`); properties (`Completion`, `WatchTermination`) compose
  with `WaitAsync`. Async-enumerable-accepting members hand the run's token
  to `GetAsyncEnumerator` — the enumeration is cancelable even though the
  accepting signature carries no token.
- **Mixed-language consumers can hit simple-name collisions.**
  `Orleans.Dataflow.FSharp.{Source, Flow, Sink}` mirror the C# names by
  design — the two frontends are equal spellings of one algebra. A C# file
  that opens both namespaces disambiguates with a `using` alias; an F# file
  shadows by `open` order. This is a papercut, priced and accepted, in
  exchange for neither frontend carrying a mangled name.
- **Two types are externally derivable** — `DataflowSinkMark` and
  `DataflowSourceCursor`, the provider SDK's two extension points; every
  other hand-written public class is sealed or static. The 1.x policy for
  them: additions arrive as virtual members with working defaults, never as
  new abstract members, because an abstract addition breaks every provider
  compiled before it.

## Trim and Native AOT

**No trim or AOT claim is made at 1.0.** The honest state, assembly by
assembly, established with the trim/AOT analyzers:

- `Orleans.Dataflow.Abstractions` — analyzer-clean today.
- `Orleans.Dataflow` — one genuine reflection site remains: the delegate
  adapter closes generic templates over types recovered at runtime, which
  is inherent to a delegate-based authoring surface, not an oversight.
- `Orleans.Dataflow.Orleans` — cannot claim compatibility regardless of
  this repo's code: Orleans `10.2.2` itself ships no `IsTrimmable` or AOT
  annotation on any of its twelve assemblies (verified with reference
  compatibility checks, which warn on every one).
- `Orleans.Dataflow.Testing` — the provider conformance kit reflects over
  provider assemblies by design; it is a diagnostic tool.
- `Orleans.Dataflow.FSharp` — the IL analyzers do not run under the F#
  compiler, so a claim here would be unverified by tooling; none is made.

A deployment that trims anyway is running ahead of both this library's
claims and Orleans' own.

## After 1.0

The forward promise is the standard one: within 1.x the hand-written public
surface only widens — the PublicAPI baselines move from Unshipped to Shipped
at the 1.0 tag, and every later change to a shipped entry or a surface
snapshot is a reviewed, deliberate diff. The full versioning and migration
policy (what moves a major, how deprecations are staged) is stated in the
release documentation rather than here.
