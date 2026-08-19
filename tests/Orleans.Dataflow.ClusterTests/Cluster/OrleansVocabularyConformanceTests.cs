using Microsoft.Extensions.DependencyInjection;
using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.ClusterTests.Provider;
using Orleans.Dataflow.Testing;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// The Orleans adapter vocabulary, run through the conformance kit the provider SDK publishes.
/// </summary>
/// <param name="cluster">The deployed cluster, whose silo container the factory is constructed from.</param>
/// <remarks>
/// <para>
/// The second of the kit's two shipped consumers, and the one that shows what it costs a provider whose
/// stages need a host: the catalog half and the payload half are answerable anywhere, and the factory half
/// is answerable only where the silo is, because building a stream stage resolves a stream provider and
/// building a reminder trigger reads the cluster's own minimum period. So the kit runs inside the cluster
/// collection, against the very container <c>AddOrleansDataflow</c> would hand its factory.
/// </para>
/// <para>
/// Ten stages, ten samples, and every sample is written by the vocabulary's own typed parameter builder —
/// <c>OrleansStages.StreamSourceParameters</c> and its nine siblings — rather than by hand. That is not a
/// convenience: a sample written as literal JSON would be a second spelling of the payload maintained
/// beside the first, and the first thing to drift.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class OrleansVocabularyConformanceTests(DataflowCluster cluster)
{
    /// <summary>The channel key and stream key the samples address.</summary>
    /// <remarks>
    /// Nothing is published to either. The kit builds a stage runtime and never opens it, so what these
    /// name has to exist as a registration and need not exist as a stream.
    /// </remarks>
    private const string ConformanceKey = "conformance";

    /// <summary>Gets the kit's checks as this theory's data.</summary>
    /// <value>One row per check, so a failure names the rule that stopped being true.</value>
    public static TheoryData<string> Checks => [.. ProviderConformance.Checks];

    [Theory]
    [MemberData(nameof(Checks))]
    public void TheOrleansVocabularyKeepsTheProviderContract(string check) => Kit().Check(check);

    /// <summary>Points the kit at the Orleans vocabulary as a silo registers it.</summary>
    /// <returns>The kit.</returns>
    /// <remarks>
    /// The registry is built from the very bindings <see cref="DataflowCluster"/> registers, so the names
    /// the samples carry are names this silo really publishes and the factory really resolves. A registry
    /// built from other bindings would make the factory checks a test of the mismatch refusal instead of a
    /// test of the vocabulary.
    /// </remarks>
    private ProviderConformance Kit()
    {
        IServiceProvider services = cluster.Cluster.Silos[0].ServiceProvider;
        OrleansAdapterRegistry.Builder registrations = new();

        registrations.Add((IStreamElementEntry)AdapterVocabulary.OrderElement);
        registrations.Add((IStreamElementEntry)AdapterVocabulary.PriceElement);
        registrations.Add((IGrainCallEntry)AdapterVocabulary.Pricing);
        registrations.Add((IKeyedGrainCallEntry)AdapterVocabulary.KeyedPricing);
        registrations.Add((IGrainCallSinkEntry)AdapterVocabulary.Recording);
        registrations.Add((IGrainEnumerableEntry)AdapterVocabulary.Feed);
        registrations.Add((IObserverBridgeEntry)AdapterVocabulary.OrderBridge);
        registrations.Add((IBroadcastElementEntry)AdapterVocabulary.BroadcastOrder);

        OrleansAdapterRegistry registry = registrations.Build();

        return ProviderConformance.Create(
            OrleansStages.Provider,
            OrleansStages.Publish(registry),
            new OrleansStageFactory(services, services.GetRequiredService<IGrainFactory>(), registry),
            Samples());
    }

    /// <summary>Writes one accepted payload per stage, through the vocabulary's own builders.</summary>
    /// <returns>The samples.</returns>
    private static IEnumerable<ProviderStageSample> Samples()
    {
        BufferOptions dropping = new() { Capacity = 8, OverflowPolicy = OverflowPolicy.DropOldest };
        OrleansStreamAddress stream = OrleansStreamAddress.Create(
            AdapterVocabulary.StreamProvider,
            "conformance-namespace",
            ConformanceKey);

        yield return ProviderStageSample.Create(
            OrleansStages.StreamSourceStage,
            OrleansStages.StreamSourceParameters(
                AdapterVocabulary.OrderElement,
                stream,
                new BufferOptions { Capacity = 8 }));

        yield return ProviderStageSample.Create(
            OrleansStages.StreamSinkStage,
            OrleansStages.StreamSinkParameters(AdapterVocabulary.PriceElement, stream));

        // The timeout is the one optional member this vocabulary has, and naming it optional is what makes
        // the kit check both halves of the claim: a payload without it is accepted, and a payload carrying
        // it as a string is not.
        yield return ProviderStageSample.Create(
            OrleansStages.GrainCallStage,
            OrleansStages.GrainCallParameters(
                AdapterVocabulary.Pricing,
                maxInFlight: 2,
                TimeSpan.FromSeconds(5)),
            ["timeoutMilliseconds"]);

        yield return ProviderStageSample.Create(
            OrleansStages.KeyedGrainCallStage,
            OrleansStages.KeyedGrainCallParameters(
                AdapterVocabulary.KeyedPricing,
                maxInFlight: 2,
                distributed: false,
                TimeSpan.FromSeconds(5)),
            ["timeoutMilliseconds"]);

        yield return ProviderStageSample.Create(
            OrleansStages.GrainCallSinkStage,
            OrleansStages.GrainCallSinkParameters(
                AdapterVocabulary.Recording,
                maxInFlight: 2,
                TimeSpan.FromSeconds(5)),
            ["timeoutMilliseconds"]);

        yield return ProviderStageSample.Create(
            OrleansStages.GrainEnumerableStage,
            OrleansStages.GrainEnumerableParameters(AdapterVocabulary.Feed));

        // Two seconds, because this cluster's floor is one and the factory refuses a period below it: what a
        // sample has to be is a payload the provider really accepts, not one that looks plausible.
        yield return ProviderStageSample.Create(
            OrleansStages.ReminderTriggerStage,
            OrleansStages.ReminderTriggerParameters(TimeSpan.FromSeconds(2), dropping));

        yield return ProviderStageSample.Create(
            OrleansStages.ObserverBridgeStage,
            OrleansStages.ObserverBridgeParameters(AdapterVocabulary.OrderBridge, dropping));

        yield return ProviderStageSample.Create(
            OrleansStages.BroadcastSinkStage,
            OrleansStages.BroadcastSinkParameters(
                AdapterVocabulary.BroadcastOrder,
                OrleansStreamAddress.Create(
                    AdapterVocabulary.BroadcastProvider,
                    OrleansStages.BroadcastSourceNamespace,
                    ConformanceKey),
                fireAndForgetDelivery: false));

        yield return ProviderStageSample.Create(
            OrleansStages.BroadcastSourceStage,
            OrleansStages.BroadcastSourceParameters(
                AdapterVocabulary.BroadcastOrder,
                AdapterVocabulary.BroadcastProvider,
                ConformanceKey,
                dropping));
    }
}
