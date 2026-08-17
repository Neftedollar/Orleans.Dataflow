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

    /// <summary>The terminating call that records a price.</summary>
    internal static GrainCallSinkBinding<AdapterPrice> Recording { get; } =
        GrainCallSinkBinding.Create(
            "record-price",
            PriceContract,
            static (grains, price, cancellationToken) =>
                grains.GetGrain<IAdapterLedgerGrain>("ledger").RecordAsync(price, cancellationToken));

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

    /// <summary>The enumeration that never ends and stops only when its token is cancelled.</summary>
    internal static GrainEnumerableBinding<AdapterOrder> EndlessFeed { get; } =
        GrainEnumerableBinding.Create(
            "orders-endless",
            OrderContract,
            static (grains, cancellationToken) =>
                grains.GetGrain<IAdapterFeedGrain>("endless").EnumerateAsync(0, cancellationToken));

    /// <summary>The provider of the two stages that stand beside an adapter in these tests.</summary>
    internal static ProviderId Provider { get; } = ProviderId.Create("adapter-test");

    /// <summary>The sink that counts what reached it and can say when it has seen enough.</summary>
    internal static StageRef Count { get; } =
        StageRef.Create(Provider, StageId.Create("count"), StageRef.FirstMajorVersion);

    /// <summary>The flow that holds the run until a signal is raised.</summary>
    internal static StageRef Gate { get; } =
        StageRef.Create(Provider, StageId.Create("gate"), StageRef.FirstMajorVersion);

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
