# Orleans research notes for M3

- Status: research snapshot, 2026-08-16; verify package versions before scaffolding
- Source: Microsoft Learn (orleans-10-0 pivot) and NuGet.org; full citations in the research run

Facts the M3 design depends on, distilled. This is a snapshot of external
reality, not a contract of ours; re-verify the version-sensitive lines when
M3 starts.

## Versions and packages

Current stable: **Orleans 10.x**, latest patch 10.2.2 (2026-07-21); targets
.NET 8/9/10. Package namespace is `Microsoft.Orleans.*` (post-7.0 layout).
Proposed test-project set: `TestingHost`, `Sdk`, `Reminders`, `Streaming`
(includes the memory stream provider), `Persistence.Memory`,
`BroadcastChannel` — all pinned to one patch version. Whether `TestingHost`
transitively carries the silo host or needs `Server` explicitly is
unverified: check `dotnet list package --include-transitive` at scaffold
time.

## API facts that shape the design

- **Codegen**: automatic via the `Sdk` source generator; no
  `ConfigureApplicationParts`. Every cross-boundary type needs
  `[GenerateSerializer]` and per-member `[Id(n)]` (missing `[Id]` is a hard
  documented failure; missing `[GenerateSerializer]` fails at first runtime
  use — different blast radii).
- **Testing**: `InProcessTestCluster` is the recommended API (9.0+);
  `TestCluster` remains for multi-process simulation. Failover pattern:
  `KillSiloAsync` (crash) / `StopSiloAsync` (semi-graceful) /
  `RestartSiloAsync`, then `WaitForLivenessToStabilizeAsync(didKill)` before
  asserting. Membership tuning via `ClusterMembershipOptions` (the 3.x-era
  "ShortGossipInterval" name no longer exists).
- **Streams**: `IAsyncStream<T>` is both observer and observable; explicit
  subscriptions survive deactivation but the grain must `ResumeAsync` each
  handle on reactivation — even when it implements `IAsyncObserver<T>`
  itself. Implicit subscriptions via `[ImplicitStreamSubscription(ns)]`.
  Memory provider: `AddMemoryStreams(name)` + `AddMemoryGrainStorage("PubSubStore")`;
  non-durable by design. Rewind is per-provider via `IsRewindable` (Azure
  Queues no, Event Hubs yes; **memory provider's value undocumented — probe
  it before promising rewind tests on it**).
- **Broadcast Channel**: implicit-only, best-effort, no history —
  matches our capability-matrix contract for the bridge row verbatim.
- **Timers/reminders**: `RegisterGrainTimer` (8.2+) with
  `GrainTimerCreationOptions` replaces obsoleted `RegisterTimer`; the
  default flipped to non-interleaving — a rename-only migration silently
  changes concurrency. Reminders: definitions survive restarts, missed
  ticks are not replayed (matches our matrix contract);
  `ReminderOptions.MinimumReminderPeriod` exists but its default and
  enforcement mode are undocumented — **probe before writing fast-ticking
  reminder tests**.
- **Pipelined calls arrive reordered** (probed 2026-08-17, Orleans 10.2.2):
  two hundred sequenced calls from one caller to one non-reentrant callee
  arrived badly out of order *within a single in-process silo* — the first
  arrival was the fourteenth sent. No pairwise message ordering between
  activations is documented, and `[Unordered]` is a no-op, so ordering can
  be neither requested nor refused. A design that needs per-key order must
  hold one call in flight per key and let the reply be the grant; the keyed
  grain-call stage does exactly that, and the probe lives in the suite as
  `KeyedOrderingProbeTests` so a future Orleans that changes this answers
  the question again.
- **A subscription opened at a sequence token receives the element that token
  names** (probed 2026-08-18, Orleans 10.2.2, memory provider): three
  publications recorded with their tokens, then a second subscription opened at
  the *second* element's token, which received `[order-2, order-3]`. Rewind is
  therefore **inclusive**, so a cursor that stores the token of the last element
  it delivered replays that element on resume — the stream source's window is
  one element wider than an index cursor's, and no "token plus one" operation
  exists to narrow it. `StreamCursorTests` keeps the probe so a future Orleans
  answers it again.
- **The memory provider's queue cache is purged when its last consumer
  unsubscribes** (probed by the same test, after the first shape of it read
  nothing): with the only subscriber gone, a subscription opened afterwards at a
  token that was in the cache a moment earlier receives *nothing at all*.
  `IsRewindable` is therefore a statement about the provider's ability, not
  about what it still holds; how far back a resume can reach is the provider's
  cache configuration, which is a deployment decision this package does not
  make. The probe keeps the first subscription alive for exactly this reason.
- **Observers are weakly referenced** (learned from a flake, 2026-08-17): the
  client-side table behind `CreateObjectReference` does not root the observer
  object. An implementation nothing else references is garbage-collected, the
  runtime logs `LogObserverGarbageCollected` and silently unregisters it, and
  every later call to the reference reports a dead target. Whoever creates an
  observer must keep the object alive for the reference's whole life —
  `GC.KeepAlive` after the unsubscribe, not a local the compiler may not hoist.
  The adapter sources (`Ticks`, `Pushes`) do this; any new observer must too.
- **Grain streaming**: `IAsyncEnumerable<T>` grain methods are first-class
  (7.2+), batched (`WithBatchSize`, default 100), cooperative cancellation
  end-to-end — the natural transport for our grain async-enumerable source.
  10.0 gotcha: `MessagingOptions.CancelRequestOnTimeout` now defaults to
  false, so a timeout no longer auto-cancels the grain-side enumeration.
- **Coordinator-relevant**: grains non-reentrant by default;
  `[AlwaysInterleave]`/`[ReadOnly]` for selective interleaving. Default
  placement changed to `ResourceOptimizedPlacement` (9.2+) — pin
  `RandomPlacement` where tests assume spread. `OnDeactivateAsync` is not
  guaranteed (crash) — never park durable state there.
- **Fencing**: `IPersistentState<T>.WriteStateAsync` with a stale ETag
  throws `InconsistentStateException` and the documented consequence is the
  current activation being killed — this is the primitive our
  coordinator/run-ownership fencing builds on.
- **`InProcessTestCluster` placement helpers (measured in M5.4, both the hard
  way)**: `MigrateAsync(grain, target)` waits for the grain's **current
  activation to deactivate**, so asking a grain to migrate to the silo it is
  already on never returns — a fixture that aims a migration without first
  reading `GetActivationAddress` hangs on a coin flip. And a silo started to
  replace a killed one is handed **the killed one's name back** (`Silo_1` dies,
  `Silo_1` returns), so a target chosen by `SiloName` off the handle list can be
  a dead address; membership (`IManagementGrain.GetHosts(onlyActive: true)`) is
  what decides which handle is live. Both are properties of the test host rather
  than of the runtime, and both belong to any fixture that kills and restores.
- **Breaking-change watchlist for 7.x-era knowledge**:
  `AddGrainCallFilter` removed (use `AddIncomingGrainCallFilter`);
  failure-detection default dropped 10 min → 90 s (9.0); `[Unordered]` is a
  no-op; SMS removed in favor of Broadcast Channel; no rolling upgrade
  across the 7.x→10 boundary.

## Named unknowns (probe, do not guess)

1. ~~Memory stream provider rewindability~~ **Resolved by probe
   (phase 2)**: `IsRewindable = true` for `AddMemoryStreams` (Orleans
   10.2.2, `PersistentStreamProvider`), on both silo and client. Phase 2
   exposed no rewind API, because a cursor with no checkpoint owner is a
   foot-gun; **M5.3 gave it one** and the stream source now subscribes with the
   token a resume restored. The two follow-up facts that decision needed —
   rewind is inclusive of the named element, and the memory provider's cache is
   purged when its last consumer leaves — are probed above.
2. ~~`ReminderOptions.MinimumReminderPeriod`~~ **Resolved by probe
   (phase 3)**: the type lives in `Orleans.Hosting` (not
   `Orleans.Configuration`); default is one minute; enforcement is a THROW
   (`ArgumentException` naming both periods) with no reminder registered
   afterwards. The reminder-trigger adapter therefore validates the period
   against the silo's configured floor at materialization. The probe test
   re-asks the question of every Orleans version this repo builds against.
3. ~~`TestingHost` package dependency closure~~ **Resolved by probe
   (phase 1)**: TestingHost 10.2.2 transitively carries the whole silo host
   (`Runtime`, plus `Persistence.Memory`, `Reminders`, `Streaming`), so a
   test project references only TestingHost and the test framework. Two
   side facts: TestingHost drags `DurableJobs` and `Journaling` at
   `10.2.2-alpha.1` (prerelease in a test-only path; restores clean), and
   `Server` = Runtime + Persistence.Memory + Sdk + Core with no
   Reminders/Streaming — which is why the src package references `Server`
   and `Sdk` explicitly.
