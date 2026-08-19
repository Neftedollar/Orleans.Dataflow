# Orleans streams and grains

You want a pipeline that reads what your grains produce, calls your grains per
element, and writes where your grains can see it — without the pipeline's
description carrying a grain reference, a stream provider, or a lambda.

This is the page that makes this an *Orleans* library rather than a generic one.
Everything below is a [registered stage](../reference/glossary.md#registered-stage):
the document names a binding, the [silo](../reference/glossary.md#silo) registers
the code behind that name, and the two halves meet at materialization.

## The whole program

A stream of orders in, a [grain](../reference/glossary.md#grain) call per order,
a grain call as the sink.

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Dataflow;
using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streams;

// Elements crossing a stream or a grain boundary are your types, and Orleans must know them.
[GenerateSerializer]
public sealed record OrderPlaced([property: Id(0)] string Id, [property: Id(1)] long Amount);

[GenerateSerializer]
public sealed record OrderPriced([property: Id(0)] string Id, [property: Id(1)] long Total);

public interface IPricingGrain : IGrainWithStringKey
{
    Task<OrderPriced> PriceAsync(OrderPlaced order, CancellationToken cancellationToken);
}

public interface ILedgerGrain : IGrainWithStringKey
{
    Task RecordAsync(OrderPriced priced, CancellationToken cancellationToken);

    Task<IReadOnlyList<string>> ReadAsync();
}

// The bindings: written once, handed to the silo and to the authoring side.
public static class OrleansVocabulary
{
    public const string StreamProvider = "orders-streams";

    public static ElementContract<OrderPlaced> PlacedContract { get; } =
        ElementContract.For<OrderPlaced>("guide-order-placed", 1);

    public static ElementContract<OrderPriced> PricedContract { get; } =
        ElementContract.For<OrderPriced>("guide-order-priced", 1);

    public static StreamElementBinding<OrderPlaced> PlacedElement { get; } =
        StreamElementBinding.Create(PlacedContract);

    public static GrainCallBinding<OrderPlaced, OrderPriced> Pricing { get; } =
        GrainCallBinding.Create(
            "price-order",
            PlacedContract,
            PricedContract,
            static (grains, order, token) =>
                grains.GetGrain<IPricingGrain>("pricing").PriceAsync(order, token));

    public static GrainCallSinkBinding<OrderPriced> Recording { get; } =
        GrainCallSinkBinding.Create(
            "record-price",
            PricedContract,
            static (grains, priced, token) =>
                grains.GetGrain<ILedgerGrain>("ledger").RecordAsync(priced, token));
}

// This deployment's own vocabulary. A silo registers at least one catalog even when its pipelines are
// built entirely from the shipped adapters, so this is also the smallest legal one: a pass-through flow
// whose ports declare the adapters' own opaque contract.
public static class AuditVocabulary
{
    public static ProviderId Provider { get; } = ProviderId.Create("orders");

    public static StageRef Audit { get; } =
        StageRef.Create(Provider, StageId.Create("audit"), StageRef.FirstMajorVersion);

    public static ContractReference NoParameters { get; } =
        ContractReference.Create(ContractId.Create("orders-no-parameters"), 1);

    public static StageCatalog Catalog() =>
        StageCatalog.Create(
        [
            StageSpecification.Create(
                Audit,
                [InputPortSpecification.Create(PortId.Create("in"), OrleansStages.ElementContract)],
                [OutputPortSpecification.Create(PortId.Create("out"), OrleansStages.ElementContract)],
                [],
                NoParameters,
                []),
        ]);
}

public sealed class AuditStageFactory : IDataflowStageFactory
{
    public DataflowStageRuntime Create(DataflowStageRequest request) =>
        request.Node.Stage == AuditVocabulary.Audit
            ? DataflowStageRuntime.Element(element => element)
            : throw new InvalidOperationException(
                $"The node '{request.Node.Id}' names '{request.Node.Stage}', which this provider does not implement.");
}

IHost silo = Host.CreateApplicationBuilder().UseOrleans(silo =>
{
    silo.UseLocalhostClustering();
    silo.AddMemoryGrainStorage(OrleansDataflowStorage.CoordinatorProviderName);
    silo.AddMemoryGrainStorage("PubSubStore");
    silo.AddMemoryStreams(OrleansVocabulary.StreamProvider);

    silo.AddOrleansDataflow(dataflow => dataflow
        .AddCatalog(AuditVocabulary.Catalog())
        .AddFactory(AuditVocabulary.Provider, new AuditStageFactory())
        .AddStreamElement(OrleansVocabulary.PlacedElement)
        .AddGrainCall(OrleansVocabulary.Pricing)
        .AddGrainCallSink(OrleansVocabulary.Recording));

    silo.Services.AddOrleansDataflowClient();
}).Build();

await silo.StartAsync();

OrleansDataflowHost host = silo.Services.GetRequiredService<OrleansDataflowHost>();

OrleansStreamAddress orders = OrleansStreamAddress.Create(
    OrleansVocabulary.StreamProvider,
    "orders",
    "today");

RunnableGraph graph = Source
    .FromRegistered(
        OrleansStages.StreamSource(OrleansVocabulary.PlacedElement),
        "orders",
        OrleansStages.StreamSourceParameters(
            OrleansVocabulary.PlacedElement,
            orders,
            new BufferOptions { Capacity = 32 }))
    .Via(
        OrleansStages.GrainCall(OrleansVocabulary.Pricing),
        "priced",
        OrleansStages.GrainCallParameters(OrleansVocabulary.Pricing, maxInFlight: 4))
    .Via(
        RegisteredStage.Flow(
            AuditVocabulary.Catalog(),
            AuditVocabulary.Audit,
            OrleansStages.Element<OrderPriced>(),
            OrleansStages.Element<OrderPriced>()),
        "audited",
        CanonicalJsonValue.Parse("{}"))
    .To(
        OrleansStages.GrainCallSink(OrleansVocabulary.Recording),
        "recorded",
        OrleansStages.GrainCallSinkParameters(OrleansVocabulary.Recording, maxInFlight: 1));

PipelineDefinition pipeline = graph.AsPipeline(
    GraphId.Create("orders-to-ledger"),
    GraphRevision.Create(1));

await using (OrleansRunHandle run = await host.MaterializeAsync(pipeline))
{
    // … publish four orders into the stream from a grain's own context, then:
    await run.ShutdownAsync();

    RunEnding ending = await run.WatchTermination;
    RunSnapshot snapshot = await run.SnapshotAsync();

    Console.WriteLine($"ending      {ending.Kind}");
    Console.WriteLine($"status      {snapshot.Status}");
    Console.WriteLine($"dropped     {snapshot.DroppedElements}");
}

await silo.StopAsync();
```

What it prints:

```text
pipeline    sha256:0c40d8cdf4af53ea166d1cd97ae1412cb1a3cb2832074e68c1c2c10119df9e47
ledger      order-1=20 order-2=40 order-3=60 order-4=80
ending      Completed
status      Completed
dropped     0
```

Three things in that listing are the whole idea.

**A binding is declared once and used twice.** `OrleansVocabulary.Pricing` is
handed to `AddGrainCall` on the silo and to `OrleansStages.GrainCall` on the
authoring side. The document that results carries the *name* `price-order` and
never the delegate, which is what lets a silo in another process run it.

**A silo registers at least one catalog**, even when everything in the pipeline
is a shipped adapter. `AuditVocabulary` above is the deployment's own vocabulary
— a single pass-through flow whose ports declare `OrleansStages.ElementContract`,
which is how one of your stages stands between two adapters. Building a silo
without an `AddCatalog` call fails at startup, by name.

**Your element types must satisfy Orleans serialization.** They cross a stream
and a grain boundary, so `[GenerateSerializer]` with `[Id]` on every member, or a
registered serializer. That is checked by Orleans at first use, not at
registration.

## Reading from a stream

```csharp
Source.FromRegistered(
    OrleansStages.StreamSource(binding),
    "orders",
    OrleansStages.StreamSourceParameters(binding, address, new BufferOptions { Capacity = 32 }));
```

**Guarantees.** The acknowledgement is delivery into the run's bounded ingress
and never end-to-end processing: an element this adapter accepted may still be
lost by a run that fails afterwards. Delivery and ordering are the *named
provider's*, reported rather than generalised — Orleans orders one stream from
one producer and nothing across producers, and the memory provider is
non-durable by design.

**What it replays.** A [durable run](../reference/glossary.md#durable-run)
subscribes at the sequence token its previous attempt stored, and an ordinary run
subscribes without one and reads what arrives after it subscribed. **The replay
window includes the element the cursor names** — a subscription opened at a token
receives that element again — so a stream source's window is one element wider
than an index cursor's. Two ways it degrades, both stated rather than promised
around: a provider whose `IsRewindable` is false refuses the token and the
resumed run fails on its subscription rather than silently reading from now; and
a rewindable provider that has *purged* the token has nothing to replay, so how
far back a resume can reach is the provider's cache configuration.

**What it costs.** One subscription per run per occurrence, one ingress of the
declared capacity, nothing persisted. Under the backpressuring overflow policy a
full ingress delays the provider's own pulling agent, **which serves a whole
queue** — so a run that stops draining delays delivery to every consumer of that
queue and not only to itself. Under a dropping policy the delivery is answered at
once and the drop is counted; under the failing policy the run faults.

## Writing to a stream

```csharp
.To(OrleansStages.StreamSink(binding), "published", OrleansStages.StreamSinkParameters(binding, address));
```

One awaited `OnNextAsync` per element — publication, never end-to-end delivery.
The awaited publication *is* the backpressure, so a slow provider slows the run
rather than filling a queue. One element in flight; elements are published in the
order the run produced them, which is the strongest order this adapter can offer.
No replay, no cursor, no retry, and a run that ends signals nothing on the stream
— a stream has no end this publisher owns.

## Calling a grain

Three shapes, and the difference between them is what they promise about order
and about effects.

### Awaited, transforming

```csharp
.Via(
    OrleansStages.GrainCall(binding),
    "priced",
    OrleansStages.GrainCallParameters(binding, maxInFlight: 4, timeout: TimeSpan.FromSeconds(5)));
```

The acknowledgement is the awaited reply, which acknowledges *that method
invocation* and nothing the grain may have started behind it. `maxInFlight` is
both the concurrency bound and the credit: a call in flight is credit spent and
its reply is the grant, and elements reach the stage through a bounded channel —
nothing on the wire carries credit. Emission is in input order; the calls
themselves overlap up to the bound, so the grains see them concurrently and only
what leaves the stage is ordered.

At-most-once from this adapter: it never retries, and **a call that fails faults
the run**. A deployment that wants a retry writes it inside the registered call,
where the duplicate window it opens is the deployment's own to state — or wraps
the stage in a [supervision scope](handling-failure.md), which is the declared
way to say how many attempts and what happens when they run out.

### Awaited, keyed

```csharp
.Via(
    OrleansStages.KeyedGrainCall(binding),
    "priced",
    OrleansStages.KeyedGrainCallParameters(binding, maxInFlight: 4, distributed: true));
```

A `KeyedGrainCallBinding` carries two functions: the one that routes an element
to a key, and the one that makes the call. Both are registered, because deciding
which partition an element belongs to is code and a document names things.

**Ordering per key, in the run's order**, because in-flight per key is one;
`maxInFlight` bounds the total across keys. Emission across keys is in input
order. Per-key ordering here is a probed fact rather than an inherited one:
Orleans documents no pairwise ordering between activations and was measured
reordering pipelined calls within a single silo, so the adapter holds one call
per key rather than relying on the platform.

`distributed: true` moves the work onto per-key executor grains, addressed
`{graph}/{run}/{node}/{key}` — per run, stateless, and left to activation
collection. Credit accounting stays bounded by the declared bound and never by
the key space: there is one credit entry per key with work in flight.

**Every silo that may host one of those executors registers the same binding**,
because a distributed keyed stage places its executors anywhere in the cluster
and each one resolves the name on the silo it landed on.

### Awaited, terminating

```csharp
.To(
    OrleansStages.GrainCallSink(binding),
    "recorded",
    OrleansStages.GrainCallSinkParameters(binding, maxInFlight: 1));
```

The sink form, and the only Orleans adapter that declares a
[mark](../reference/glossary.md#mark): `{"acknowledged":n}`, how many of its calls
have been answered, advanced *after* the reply is awaited and never on a throw.
The mark can lag the truth by up to `maxInFlight` — a reply is counted when the
window's queue reaches it, not when it lands — which widens a resume's replay and
never narrows it. **At a bound of one the mark is exact**, and that is the
arrangement to choose when a resume must not re-deliver.

What a mark means is exactly the acknowledgement above and no more: an answered
invocation, not anything the grain did behind it.

The terminating form orders its effects only at a bound of one. Cancellation is
observed *between* elements, because a terminal is a synchronous fold handed no
token — a call already in flight runs to its own end or to Orleans' call timeout.

### Reading a grain's enumeration

```csharp
Source.FromRegistered(
    OrleansStages.GrainEnumerable(binding),
    "feed",
    OrleansStages.GrainEnumerableParameters(binding));
```

The one Orleans source that needs no ingress buffer: the enumeration's own
backpressure is the pipeline's, so a run that stops pulling stops the grain from
producing. At-most-once within one call, the grain's own order preserved end to
end, no replay and no cursor — resuming where a previous run stopped needs an
application cursor the grain owns.

Cancellation is cooperative and carried by the run's own token. Orleans defaults
`MessagingOptions.CancelRequestOnTimeout` to false, so a response timeout does
not cancel the grain-side enumeration, and a grain that ignores the token delays
the run's stop until it next yields.

## The two push bridges

Both are best-effort by construction, and both make that observable rather than
implying otherwise.

### The observer bridge

The direction is the opposite of a subscription: the *run* publishes a receiver,
and grain code anywhere in the cluster pushes at it for as long as the run is
listening.

```csharp
Source.FromRegistered(
    OrleansStages.ObserverBridge(binding),
    "pushed",
    OrleansStages.ObserverBridgeParameters(binding, ingress));
```

The address is `{graph}/{run}/{binding}`, composable by any caller holding the
run's ticket — `OrleansStages.ObserverBridgeKey(ticket, "orders-bridge")` spells
it. A push is answered `Accepted`, `Dropped`, `Closed`, or `Failed`, so a caller
*learns* that a run stopped listening rather than guessing.

One pusher's elements arrive in the order it sent them, because the bridge
serialises pushes; nothing is ordered across pushers. Under the backpressuring
policy the push waits for room, and because the bridge grain is not reentrant
every other pusher waits behind it. No history, so no delivery to a run that has
not attached yet or has already ended. A receiver whose process is gone hangs the
push until Orleans' response timeout, is then reported `Closed`, and is forgotten
— so the cost is paid once per lost run rather than once per push.

### The broadcast bridge

```csharp
Source.FromRegistered(
    OrleansStages.BroadcastSource(binding),
    "published",
    OrleansStages.BroadcastSourceParameters(binding, provider, channelKey, ingress));
```

Consumption is confined to one package-owned namespace and the document names a
channel *key* inside it. That is the platform's shape rather than a decision:
Broadcast Channel subscription is implicit only — a grain *type* declares in an
attribute which namespaces it receives — so the subscriber has to be a grain this
package compiled. `OrleansStages.BroadcastSourceChannel(provider, key)` composes
the address a publisher writes to. The **sink** is unaffected and addresses any
namespace, because publishing needs no subscription.

The ingress overflow policy may **not** be `backpressure`: the relay grain
forwards to every run listening on one non-reentrant turn, so a run waiting for
room would stop the channel for all of them. Two named ways to lose an element: a
publication that arrives with nothing attached is dropped silently, and a
publication that finds a full ingress is dropped or fails by the declared policy.
No history, ever — a run that attaches a moment late is not caught up.

On the publishing side, what `Publish` awaits depends on the provider. With
`FireAndForgetDelivery` off it completes when every implicit subscriber has
handled the element; with it on, when the deliveries have been dispatched and a
subscriber that throws is never reported. **The declared mode is checked against
the silo's provider at materialization**, because a channel's mode belongs to the
provider and cannot be chosen per publication.

## Timers and reminders

The two triggers differ in exactly one way, and it is the way that matters.

**A timer is the run's**, and it is not an Orleans concept at all — it ships in
the runtime-neutral `dotnet` vocabulary as `dotnet/timer@v1`, so one registration
(`AddDotnetStages()`) serves a silo and an in-process host alike.

```csharp
Source.FromRegistered(DotnetStages.Timer(), "ticks", DotnetStages.TimerParameters(TimeSpan.FromSeconds(5)));
```

The tick is awaited on the run's own source thread, so there is no queue anywhere
and a run slower than the period simply ticks later. Ticks do not accumulate and
none is dropped, because there is no buffer for them to accumulate in. The tick
index is a `long` counting from zero, and a resumed run's clock starts again from
zero — a timer has no cursor, so a schedule that must survive a restart is a
reminder.

**A reminder is the cluster's.**

```csharp
Source.FromRegistered(
    OrleansStages.ReminderTrigger(),
    "ticks",
    OrleansStages.ReminderTriggerParameters(period, ingress));
```

The sharpest fact on this adapter: **the reminder definition survives a restart
and the run does not.** A reminder that should have fired while nothing was
running fires once when a silo picks it up again, and the ticks in between are
gone. The durable half of this stage is a schedule and never a stream.

The period is whole milliseconds and at least the cluster's
`ReminderOptions.MinimumReminderPeriod`, which Orleans enforces by throwing
rather than clamping — so a document below it is refused at materialization,
naming the configured minimum. The ingress overflow policy may not be
`backpressure`: a clock cannot be slowed, and a tick parked in a full queue would
hold the grain turn that owns the cluster's reminder for this run. The reminder is
unregistered on every terminal path the run can reach, and a tick that finds no
live run unregisters it from the tick side.

## What is Orleans-specific about all of this

### Placement

Two grain types have a placement a deployment may care about, and both default to
`DataflowPlacement.ClusterDefault` — which *defers* rather than naming a strategy,
so a deployment that configured its own default keeps it.

```csharp
silo.AddOrleansDataflow(dataflow => dataflow
    .UsePlacement(runGrains: DataflowPlacement.ClusterDefault, keyedExecutors: DataflowPlacement.HashBased));
```

`Random` spreads without regard to load. `PreferLocal` removes the network hop
for the common call and gives no spread at all — reasonable for keyed executors
whose work is cheap and whose caller is one run. `HashBased` makes a key's
placement a property of the key, which is what a deployment wants when it has
arranged its own data by that same key.

The knob exists rather than an attribute on the grain classes because Orleans
changed its own cluster default to resource-optimised placement, which is the
right default for most work and the wrong one for a deployment that has
partitioned its data by hand.

For a durable run, placement is a performance choice and **never** a correctness
one: the checkpoint travels through the store rather than through the silo, so a
resumed activation lands wherever Orleans places it and continues identically.

### Activation lifecycle

Orleans creates and recycles [activations](../reference/glossary.md#activation) as
it sees fit, and what that means for a run depends on whether the run is durable.

- **An ordinary run is exactly as durable as its activation.** A recycled
  activation is a lost run, reported as `PipelineRunLostException` — which is the
  honest report rather than a wait that never ends.
- **A durable run is continued.** Polling one that lost its activation resumes
  it, exactly as any other call to it does; a resumed attempt claims a fresh
  [epoch](../reference/glossary.md#epoch), and a handle from before it adopts the
  current epoch from the fencing refusal that names it and carries on.

One activation per run grain is what makes a replacement work: the activation a
replacement asks to start is the very one hosting the old attempt, and it
disposes that engine before starting the replacement.

### What a grain call's failure does to the run

No adapter here retries. A grain call that throws faults the run, with the
callee's exception as the cause — and that is deliberate, because a retry this
adapter performed would open a duplicate window nobody declared.

Three consequences worth holding:

- **A failure is the run's, not the element's**, unless you put the stage inside
  a supervision scope. Inside one, the scope decides: retry with the declared
  ladder, fall back, drop, or fail.
- **A distributed keyed call's failure reports the executor grain's address**,
  which is `{graph}/{run}/{node}/{key}` and carries the routing key **in full**.
  It is an address rather than a diagnostic, and truncating it would make two
  partitions collide — so if your keys are account numbers, that value reaches
  durable failure text. See
  [what user data can reach a failure message](../operations/monitoring.md#what-user-data-can-reach-a-failure-message).
- **A per-call timeout is optional and per occurrence.** Without one, the call is
  bounded by Orleans' own response timeout; with one, a `GrainCallTimeoutException`
  faults the run, naming the occurrence and the bound.

## Next

- [Running on a silo](../start/running-on-a-silo.md) — the tutorial version of the registration above.
- [The cluster model](../concepts/cluster-model.md) — what runs where, and who owns what.
- [Adapters](../reference/adapters.md) — every adapter's row, with its delivery guarantee and its replay window.
- [Writing a custom stage](custom-stages.md) — when the shipped adapters are not the seam you need.
- [Deploying](../operations/deploying.md) — what every silo must agree about.
