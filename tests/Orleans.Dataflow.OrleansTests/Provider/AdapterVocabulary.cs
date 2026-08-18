using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.OrleansTests.Provider;

/// <summary>
/// The author types, contracts, and named bindings the adapter tests run.
/// </summary>
/// <remarks>
/// <para>
/// The element types are the author's own records with <c>[GenerateSerializer]</c>, which is the point:
/// phase 1 proved the path with <see cref="long"/> and left the serializer unproven, and every element here
/// crosses a stream and a grain boundary as a type Orleans had to be told about.
/// </para>
/// <para>
/// The bindings are declared once and handed to two places — the silo's registration and the authoring
/// helpers — which is the shape the adapters are built around. A test that authored against one declaration
/// and registered another would be testing the mismatch check rather than the adapter, and there is a test
/// that does exactly that on purpose.
/// </para>
/// </remarks>
internal static class AdapterVocabulary
{
    /// <summary>The name the memory stream provider is registered under in the test cluster.</summary>
    internal const string StreamProvider = "dataflow-test-streams";

    /// <summary>The contract of the orders these tests carry.</summary>
    internal static ElementContract<AdapterOrder> OrderContract { get; } =
        ElementContract.For<AdapterOrder>("adapter-order", 1);

    /// <summary>The contract of the prices these tests carry.</summary>
    internal static ElementContract<AdapterPrice> PriceContract { get; } =
        ElementContract.For<AdapterPrice>("adapter-price", 1);

    /// <summary>The stream element binding for orders.</summary>
    internal static StreamElementBinding<AdapterOrder> OrderElement { get; } =
        StreamElementBinding.Create(OrderContract);

    /// <summary>The stream element binding for prices.</summary>
    internal static StreamElementBinding<AdapterPrice> PriceElement { get; } =
        StreamElementBinding.Create(PriceContract);

    /// <summary>The call that prices an order.</summary>
    internal static GrainCallBinding<AdapterOrder, AdapterPrice> Pricing { get; } =
        GrainCallBinding.Create(
            "price-order",
            OrderContract,
            PriceContract,
            static (grains, order, cancellationToken) =>
                grains.GetGrain<IAdapterPricingGrain>("pricing").PriceAsync(order, cancellationToken));

    /// <summary>The call that holds every order until the test releases it, counting how many it holds.</summary>
    internal static GrainCallBinding<AdapterOrder, AdapterPrice> GatedPricing { get; } =
        GrainCallBinding.Create(
            "gated-price-order",
            OrderContract,
            PriceContract,
            static (grains, order, cancellationToken) =>
                grains.GetGrain<IAdapterPricingGrain>("gated").PriceGatedAsync(order, cancellationToken));

    /// <summary>The call that holds each order until its own signal is raised.</summary>
    internal static GrainCallBinding<AdapterOrder, AdapterPrice> SignalledPricing { get; } =
        GrainCallBinding.Create(
            "signalled-price-order",
            OrderContract,
            PriceContract,
            static (grains, order, cancellationToken) =>
                grains.GetGrain<IAdapterPricingGrain>("signalled").PriceOnSignalAsync(order, cancellationToken));

    /// <summary>The call that throws.</summary>
    internal static GrainCallBinding<AdapterOrder, AdapterPrice> FailingPricing { get; } =
        GrainCallBinding.Create(
            "failing-price-order",
            OrderContract,
            PriceContract,
            static (grains, order, cancellationToken) =>
                grains.GetGrain<IAdapterPricingGrain>("failing").PriceRefusedAsync(order, cancellationToken));

    /// <summary>The call that never answers until the test releases it.</summary>
    internal static GrainCallBinding<AdapterOrder, AdapterPrice> HangingPricing { get; } =
        GrainCallBinding.Create(
            "hanging-price-order",
            OrderContract,
            PriceContract,
            static (grains, order, cancellationToken) =>
                grains.GetGrain<IAdapterPricingGrain>("hanging").PriceHeldAsync(order, cancellationToken));

    /// <summary>Reads the partition one order belongs to.</summary>
    /// <param name="order">The order.</param>
    /// <returns>One of three keys.</returns>
    /// <remarks>
    /// Written once and handed to both halves of the keyed binding — the routing function and the call —
    /// because they have to agree: the key decides which executor the element goes to and which grain the
    /// call reaches, and two spellings of "the key" would be a stage that ordered one partition while
    /// talking to another. Three keys over twelve elements gives every key four elements, which is enough
    /// for an ordering claim to have something to be wrong about.
    /// </remarks>
    internal static string KeyOf(AdapterOrder order) =>
        string.Create(System.Globalization.CultureInfo.InvariantCulture, $"key-{order.Amount % 3}");

    /// <summary>The keyed call that prices an order on the grain its key names.</summary>
    internal static KeyedGrainCallBinding<AdapterOrder, AdapterPrice> KeyedPricing { get; } =
        KeyedGrainCallBinding.Create(
            "keyed-price-order",
            OrderContract,
            PriceContract,
            KeyOf,
            static (grains, order, cancellationToken) =>
                grains.GetGrain<IAdapterKeyedGrain>(KeyOf(order)).PriceAsync(order, cancellationToken));

    /// <summary>The keyed call that holds every order until the test releases it.</summary>
    internal static KeyedGrainCallBinding<AdapterOrder, AdapterPrice> GatedKeyedPricing { get; } =
        KeyedGrainCallBinding.Create(
            "gated-keyed-price-order",
            OrderContract,
            PriceContract,
            KeyOf,
            static (grains, order, cancellationToken) =>
                grains.GetGrain<IAdapterKeyedGrain>(KeyOf(order)).PriceGatedAsync(order, cancellationToken));

    /// <summary>The keyed call that throws.</summary>
    internal static KeyedGrainCallBinding<AdapterOrder, AdapterPrice> FailingKeyedPricing { get; } =
        KeyedGrainCallBinding.Create(
            "failing-keyed-price-order",
            OrderContract,
            PriceContract,
            KeyOf,
            static (grains, order, cancellationToken) =>
                grains.GetGrain<IAdapterKeyedGrain>(KeyOf(order)).PriceRefusedAsync(order, cancellationToken));

    /// <summary>The terminating call that records a price.</summary>
    internal static GrainCallSinkBinding<AdapterPrice> Recording { get; } =
        GrainCallSinkBinding.Create(
            "record-price",
            PriceContract,
            static (grains, price, cancellationToken) =>
                grains.GetGrain<IAdapterLedgerGrain>("ledger").RecordAsync(price, cancellationToken));

    /// <summary>The terminating call that writes every price into a log the test process keeps.</summary>
    /// <remarks>
    /// Beside <see cref="Recording"/> rather than instead of it, and the difference is where the evidence
    /// lives. The recording ledger writes into the shared observations, which one collection's tests reset
    /// between them; this one writes into a named log, so a test in a different collection — the crash
    /// suite, which runs against its own cluster and possibly at the same time — has a record nobody else
    /// touches.
    /// </remarks>
    internal static GrainCallSinkBinding<AdapterPrice> Logging { get; } =
        GrainCallSinkBinding.Create(
            "log-price",
            PriceContract,
            static (grains, price, cancellationToken) =>
                grains.GetGrain<IAdapterLedgerGrain>("logging").LogAsync(price, cancellationToken));

    /// <summary>The terminating call that records a price once the test releases the ledger.</summary>
    internal static GrainCallSinkBinding<AdapterPrice> GatedRecording { get; } =
        GrainCallSinkBinding.Create(
            "gated-record-price",
            PriceContract,
            static (grains, price, cancellationToken) =>
                grains.GetGrain<IAdapterLedgerGrain>("ledger").RecordGatedAsync(price, cancellationToken));

    /// <summary>The enumeration that yields four orders and ends.</summary>
    internal static GrainEnumerableBinding<AdapterOrder> Feed { get; } =
        GrainEnumerableBinding.Create(
            "orders-feed",
            OrderContract,
            static (grains, cancellationToken) =>
                grains.GetGrain<IAdapterFeedGrain>("feed").EnumerateAsync(4, cancellationToken));

    /// <summary>The enumeration that yields twelve orders and ends, four to each of three keys.</summary>
    internal static GrainEnumerableBinding<AdapterOrder> KeyedFeed { get; } =
        GrainEnumerableBinding.Create(
            "orders-keyed-feed",
            OrderContract,
            static (grains, cancellationToken) =>
                grains.GetGrain<IAdapterFeedGrain>("keyed-feed").EnumerateAsync(12, cancellationToken));

    /// <summary>The enumeration that never ends and stops only when its token is cancelled.</summary>
    internal static GrainEnumerableBinding<AdapterOrder> EndlessFeed { get; } =
        GrainEnumerableBinding.Create(
            "orders-endless",
            OrderContract,
            static (grains, cancellationToken) =>
                grains.GetGrain<IAdapterFeedGrain>("endless").EnumerateAsync(0, cancellationToken));

    /// <summary>The name the awaiting broadcast channel provider is registered under.</summary>
    internal const string BroadcastProvider = "dataflow-test-broadcast";

    /// <summary>The name the fire-and-forget broadcast channel provider is registered under.</summary>
    internal const string FireAndForgetBroadcastProvider = "dataflow-test-broadcast-ff";

    /// <summary>The bridge external grain code pushes orders at.</summary>
    internal static ObserverBridgeBinding<AdapterOrder> OrderBridge { get; } =
        ObserverBridgeBinding.Create("orders-bridge", OrderContract);

    /// <summary>A second bridge, so that two bindings in one run address two grains.</summary>
    internal static ObserverBridgeBinding<AdapterOrder> NarrowBridge { get; } =
        ObserverBridgeBinding.Create("orders-bridge-narrow", OrderContract);

    /// <summary>The element contract this cluster's broadcast channels carry.</summary>
    internal static BroadcastElementBinding<AdapterOrder> BroadcastOrder { get; } =
        BroadcastElementBinding.Create(OrderContract);

    /// <summary>The observable a silo publishes, proving one declaration serves both hosts.</summary>
    /// <remarks>
    /// The very shape a deployment writes: one <see cref="ObservableBinding{T}"/> handed to a silo and to
    /// the authoring helpers, with nothing Orleans-specific anywhere in it. It replays a fixed sequence and
    /// ends, so a cluster test can assert what a run of it produced without driving anything by hand.
    /// </remarks>
    internal static ObservableBinding<AdapterOrder> SharedOrders { get; } =
        ObservableBinding.Create(
            "shared-orders",
            OrderContract,
            static () => new ReplayObservable<AdapterOrder>(
                new AdapterOrder("replay-1", 1),
                new AdapterOrder("replay-2", 2),
                new AdapterOrder("replay-3", 3)));

    /// <summary>The provider of the two stages that stand beside an adapter in these tests.</summary>
    internal static ProviderId Provider { get; } = ProviderId.Create("adapter-test");

    /// <summary>The sink that counts what reached it and can say when it has seen enough.</summary>
    internal static StageRef Count { get; } =
        StageRef.Create(Provider, StageId.Create("count"), StageRef.FirstMajorVersion);

    /// <summary>The flow that holds the run until a signal is raised.</summary>
    internal static StageRef Gate { get; } =
        StageRef.Create(Provider, StageId.Create("gate"), StageRef.FirstMajorVersion);

    /// <summary>The flow that turns a test number into a price.</summary>
    /// <remarks>
    /// The joint between the test vocabulary and the Orleans adapters, and the only reason it exists: the
    /// range source is the one source in this suite that declares a cursor, and the terminating grain call
    /// is the one sink that declares a commit mark, so a test about what a crash does to the pair needs a
    /// stage that stands between them. Its input port declares the test vocabulary's own contract and its
    /// output port declares <see cref="OrleansStages.ElementContract"/>, which is the documented way a
    /// deployment's stage joins an adapter, exercised rather than described.
    /// </remarks>
    internal static StageRef Priced { get; } =
        StageRef.Create(Provider, StageId.Create("priced"), StageRef.FirstMajorVersion);

    /// <summary>The same counting sink, on the port contract the .NET push adapters declare.</summary>
    /// <remarks>
    /// One stage per element contract is the documented cost of a specification declaring one contract per
    /// port. A deployment's own stage that wants to consume a push source declares
    /// <see cref="DotnetStages.ElementContract"/> exactly as one that wants to consume an Orleans adapter
    /// declares <see cref="OrleansStages.ElementContract"/>, and this is that escape hatch exercised on the
    /// other provider.
    /// </remarks>
    internal static StageRef DotnetCount { get; } =
        StageRef.Create(Provider, StageId.Create("dotnet-count"), StageRef.FirstMajorVersion);

    /// <summary>The contract of the total the counting sink yields.</summary>
    internal static ResultContract<long> Total { get; } = ResultContract.For<long>("adapter-total", 1);

    /// <summary>The contract of the counting sink's payload.</summary>
    internal static ContractReference CountParameters { get; } =
        ContractReference.Create(ContractId.Create("adapter-count-parameters"), 1);

    /// <summary>The contract of the gate flow's payload.</summary>
    internal static ContractReference GateParameters { get; } =
        ContractReference.Create(ContractId.Create("adapter-gate-parameters"), 1);

    /// <summary>Gets the catalog of the two stages that stand beside an adapter.</summary>
    /// <returns>The catalog.</returns>
    /// <remarks>
    /// Both declare <see cref="OrleansStages.ElementContract"/> on the port that faces an adapter, which is
    /// the documented way a deployment's own stage joins two adapters: an adapter's ports carry one opaque
    /// contract because one specification cannot declare a per-occurrence one, so a neighbour that wants to
    /// sit beside one declares the same contract. This catalog is that escape hatch exercised rather than
    /// described.
    /// </remarks>
    internal static StageCatalog Catalog() =>
        StageCatalog.Create(
        [
            StageSpecification.Create(
                Count,
                [InputPortSpecification.Create(PortId.Create("in"), OrleansStages.ElementContract)],
                [],
                [ResultPortSpecification.Create(PortId.Create("total"), Total.Reference)],
                CountParameters,
                []),
            StageSpecification.Create(
                Gate,
                [InputPortSpecification.Create(PortId.Create("in"), OrleansStages.ElementContract)],
                [OutputPortSpecification.Create(PortId.Create("out"), OrleansStages.ElementContract)],
                [],
                GateParameters,
                []),
            StageSpecification.Create(
                DotnetCount,
                [InputPortSpecification.Create(PortId.Create("in"), DotnetStages.ElementContract)],
                [],
                [ResultPortSpecification.Create(PortId.Create("total"), Total.Reference)],
                CountParameters,
                []),
            StageSpecification.Create(
                Priced,
                [InputPortSpecification.Create(PortId.Create("in"), TestVocabulary.Number.Reference)],
                [OutputPortSpecification.Create(PortId.Create("out"), OrleansStages.ElementContract)],
                [],
                TestVocabulary.NoParameters,
                []),
        ]);

    /// <summary>Writes the counting sink's payload.</summary>
    /// <param name="signal">The signal raised once the sink has seen enough elements.</param>
    /// <param name="signalAt">How many elements are enough.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue CountPayload(string signal, int signalAt) =>
        CanonicalJsonValue.Parse(
            $"{{\"signal\":\"{signal}\",\"signalAt\":{signalAt.ToString(System.Globalization.CultureInfo.InvariantCulture)}}}");

    /// <summary>Writes the gate flow's payload.</summary>
    /// <param name="entered">The signal the gate raises when its first element reaches it.</param>
    /// <param name="release">The signal that releases it.</param>
    /// <returns>The canonical payload.</returns>
    internal static CanonicalJsonValue GatePayload(string entered, string release) =>
        CanonicalJsonValue.Parse($"{{\"entered\":\"{entered}\",\"release\":\"{release}\"}}");
}

/// <summary>An order, as an author's own Orleans-serializable type.</summary>
/// <param name="Id">The order's identity.</param>
/// <param name="Amount">The amount ordered.</param>
[GenerateSerializer]
public sealed record AdapterOrder([property: Id(0)] string Id, [property: Id(1)] long Amount);

/// <summary>A price, as an author's own Orleans-serializable type.</summary>
/// <param name="Id">The order's identity.</param>
/// <param name="Total">The total price.</param>
[GenerateSerializer]
public sealed record AdapterPrice([property: Id(0)] string Id, [property: Id(1)] long Total);

/// <summary>An observable that hands every subscriber a fixed sequence and then ends.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="items">The sequence.</param>
/// <remarks>
/// Cold and synchronous: each subscription replays the sequence on the subscribing thread and completes,
/// so a run of it produces exactly what the fixture declares and a cluster test needs nothing to drive. The
/// push happens inside <see cref="Subscribe"/>, which is legal and worth exercising — a producer that has
/// already finished by the time the subscription is returned is the sharpest version of the lifetime rule.
/// </remarks>
internal sealed class ReplayObservable<T>(params T[] items) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentNullException.ThrowIfNull(observer);

        foreach (T item in items)
        {
            observer.OnNext(item);
        }

        observer.OnCompleted();

        return new Subscription();
    }

    /// <summary>The subscription to a producer that has already finished.</summary>
    private sealed class Subscription : IDisposable
    {
        /// <inheritdoc/>
        public void Dispose()
        {
        }
    }
}
