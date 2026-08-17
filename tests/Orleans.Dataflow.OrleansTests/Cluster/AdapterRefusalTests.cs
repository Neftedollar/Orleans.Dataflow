using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.OrleansTests.Provider;
using Orleans.Dataflow.Serialization;
using Orleans.Hosting;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// What a silo refuses about an Orleans adapter, and whether the refusal says enough to act on.
/// </summary>
/// <remarks>
/// A document names a call, a source, or an element contract; a silo says whether it publishes that name.
/// Every refusal here happens before a run identity exists, which is what makes "this deployment cannot run
/// this graph" a different outcome from "this graph ran and went wrong".
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class AdapterRefusalTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ADocumentNamingAnUnregisteredCallIsRefusedListingTheOnesThisSiloPublishes()
    {
        PipelineDefinition pipeline = AdapterPipelines.HandWrittenCall(
            "adapter-unknown-call",
            CanonicalJsonValue.Parse(
                "{\"call\":\"no-such-call\",\"input\":\"adapter-order@v1\",\"maxInFlight\":1,\"output\":\"adapter-price@v1\"}"));

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("invalid-parameters", refused.Message, StringComparison.Ordinal);
        Assert.Contains("'no-such-call' is not registered in this silo", refused.Message, StringComparison.Ordinal);
        Assert.Contains("'price-order'", refused.Message, StringComparison.Ordinal);
        Assert.Contains("priced", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADocumentDeclaringADifferentSignatureForARegisteredCallIsRefusedNamingBoth()
    {
        PipelineDefinition pipeline = AdapterPipelines.HandWrittenCall(
            "adapter-wrong-signature",
            CanonicalJsonValue.Parse(
                "{\"call\":\"price-order\",\"input\":\"adapter-order@v1\",\"maxInFlight\":1,\"output\":\"adapter-order@v1\"}"));

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("different signature", refused.Message, StringComparison.Ordinal);
        Assert.Contains("adapter-price@v1", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADocumentNamingAnUnregisteredStreamElementContractIsRefused()
    {
        PipelineDefinition pipeline = AdapterPipelines.HandWrittenStreamSource(
            "adapter-unknown-element",
            CanonicalJsonValue.Parse(
                "{\"capacity\":1,\"element\":\"nobody-registered-this@v1\",\"key\":\"k\",\"namespace\":\"n\",\"overflowPolicy\":\"backpressure\",\"provider\":\"" +
                AdapterVocabulary.StreamProvider +
                "\"}"));

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains(
            "'nobody-registered-this@v1' is not registered in this silo",
            refused.Message,
            StringComparison.Ordinal);
        Assert.Contains("adapter-order@v1", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADocumentNamingAStreamProviderThisSiloDoesNotHostIsRefusedAtMaterialization()
    {
        PipelineDefinition pipeline = AdapterPipelines.HandWrittenStreamSource(
            "adapter-unknown-provider",
            CanonicalJsonValue.Parse(
                "{\"capacity\":1,\"element\":\"adapter-order@v1\",\"key\":\"k\",\"namespace\":\"n\",\"overflowPolicy\":\"backpressure\",\"provider\":\"no-such-provider\"}"));

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        // The one thing a parameter validator cannot see: which providers a silo hosts is not a property of
        // the payload, so this refusal comes from the build rather than from the catalog.
        Assert.Contains("no-such-provider", refused.Message, StringComparison.Ordinal);
        Assert.Contains("AddMemoryStreams", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADocumentWithAMalformedAdapterPayloadIsRefusedWithEveryViolation()
    {
        PipelineDefinition pipeline = AdapterPipelines.HandWrittenCall(
            "adapter-malformed",
            CanonicalJsonValue.Parse("{\"call\":\"price-order\",\"maxInFlight\":0}"));

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("the member 'input' is missing", refused.Message, StringComparison.Ordinal);
        Assert.Contains("the member 'output' is missing", refused.Message, StringComparison.Ordinal);
        Assert.Contains("the member 'maxInFlight' is 0", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OneCallNameRegisteredTwiceIsRefusedWhenTheSiloStarts()
    {
        string reported = await RefusedSilo(dataflow => dataflow
            .AddCatalog(TestVocabulary.Catalog())
            .AddFactory(TestVocabulary.Provider, new TestStageFactory())
            .AddGrainCall(AdapterVocabulary.Pricing)
            .AddGrainCall(GrainCallBinding.Create(
                AdapterVocabulary.Pricing.Name,
                AdapterVocabulary.OrderContract,
                AdapterVocabulary.PriceContract,
                static (grains, order, cancellationToken) => Task.FromResult(new AdapterPrice(order.Id, 0L)))));

        Assert.Contains("registered more than once", reported, StringComparison.Ordinal);
        Assert.Contains("price-order", reported, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OneStreamElementContractRegisteredTwiceIsRefusedWhenTheSiloStarts()
    {
        string reported = await RefusedSilo(dataflow => dataflow
            .AddCatalog(TestVocabulary.Catalog())
            .AddFactory(TestVocabulary.Provider, new TestStageFactory())
            .AddStreamElement(AdapterVocabulary.OrderElement)
            .AddStreamElement(StreamElementBinding.Create(AdapterVocabulary.OrderContract)));

        Assert.Contains("one contract is carried by one CLR type in one silo", reported, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADocumentNamingAnUnregisteredCallSinkIsRefused()
    {
        PipelineDefinition pipeline = AdapterPipelines.HandWrittenCallSink(
            "adapter-unknown-call-sink",
            CanonicalJsonValue.Parse(
                "{\"call\":\"no-such-sink\",\"input\":\"adapter-price@v1\",\"maxInFlight\":1}"));

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains(
            "'no-such-sink' is not registered in this silo",
            refused.Message,
            StringComparison.Ordinal);
        Assert.Contains("'record-price'", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASiloThatRegistersNoAdapterBindingPublishesNoAdapterStageAtAll()
    {
        // The catalog a silo publishes is exactly what it registered. The adapters ship as one vocabulary
        // and arrive with the first binding, so a deployment that uses none keeps the vocabulary — and the
        // catalog fingerprint — it wrote, and a document naming an adapter meets the ordinary unknown-stage
        // refusal rather than a half-configured adapter.
        Orleans.TestingHost.InProcessTestClusterBuilder builder = new(initialSilosCount: 1);

        builder.ConfigureSilo((siloOptions, silo) =>
        {
            _ = silo.AddMemoryGrainStorage(OrleansDataflowStorage.CoordinatorProviderName);
            _ = silo.AddOrleansDataflow(dataflow => dataflow
                .AddCatalog(TestVocabulary.Catalog())
                .AddFactory(TestVocabulary.Provider, new TestStageFactory()));
        });

        await using Orleans.TestingHost.InProcessTestCluster plain = builder.Build();

        await plain.DeployAsync();

        (PipelineDefinition pipeline, ResultSlot<long> _) = AdapterPipelines.CountingFeed(
            "adapter-not-published",
            AdapterVocabulary.Feed,
            "unused",
            int.MaxValue);

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => new OrleansDataflowHost(plain.Client).MaterializeAsync(pipeline, Token));

        Assert.Contains("unknown-stage", refused.Message, StringComparison.Ordinal);
        Assert.Contains("orleans/grain-enumerable@v1", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheAdapterCatalogPublishesTheNineAdapterStagesAndNothingElse()
    {
        Assert.Equal(9, OrleansStages.Catalog.Specifications.Count);
        Assert.True(OrleansStages.Catalog.TryGetSpecification(OrleansStages.StreamSourceStage, out _));
        Assert.True(OrleansStages.Catalog.TryGetSpecification(OrleansStages.StreamSinkStage, out _));
        Assert.True(OrleansStages.Catalog.TryGetSpecification(OrleansStages.GrainCallStage, out _));
        Assert.True(OrleansStages.Catalog.TryGetSpecification(OrleansStages.KeyedGrainCallStage, out _));
        Assert.True(OrleansStages.Catalog.TryGetSpecification(OrleansStages.GrainCallSinkStage, out _));
        Assert.True(OrleansStages.Catalog.TryGetSpecification(OrleansStages.GrainEnumerableStage, out _));
        Assert.True(OrleansStages.Catalog.TryGetSpecification(OrleansStages.ReminderTriggerStage, out _));
        Assert.True(OrleansStages.Catalog.TryGetSpecification(OrleansStages.ObserverBridgeStage, out _));
        Assert.True(OrleansStages.Catalog.TryGetSpecification(OrleansStages.BroadcastSinkStage, out _));
        Assert.False(OrleansStages.Catalog.TryGetSpecification(TestVocabulary.Range, out _));
    }

    /// <summary>Deploys a silo with a broken registration and reports what stopped it.</summary>
    /// <param name="configure">The registration to make.</param>
    /// <returns>The text of everything the failure said, inner exceptions included.</returns>
    private static async Task<string> RefusedSilo(Action<IOrleansDataflowBuilder> configure)
    {
        Orleans.TestingHost.InProcessTestClusterBuilder builder = new(initialSilosCount: 1);

        builder.ConfigureSilo((siloOptions, silo) => _ = silo.AddOrleansDataflow(configure));

        await using Orleans.TestingHost.InProcessTestCluster refused = builder.Build();

        Exception failure = await Assert.ThrowsAnyAsync<Exception>(refused.DeployAsync);

        return failure.ToString();
    }
}
