using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.ClusterTests.Provider;
using Orleans.Dataflow.Serialization;
using Orleans.Hosting;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// What a silo refuses, and whether the refusal says enough to act on.
/// </summary>
/// <remarks>
/// A refusal is a start that produced nothing, which is a different outcome from a run that failed and has
/// to read differently. Each of these asserts on the message as well as on the type, because a message
/// that named only the first of ten problems would pass a test about the type and still be useless.
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class ClusterRefusalTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    /// <value>The ambient test's own cancellation token.</value>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ADocumentNamingAnUnregisteredStageIsRefusedWithTheCompilerDiagnostics()
    {
        PipelineDefinition pipeline = TestPipelines.Unknown("unknown-stage");

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("unknown-stage", refused.Message, StringComparison.Ordinal);
        Assert.Contains("test/nowhere@v1", refused.Message, StringComparison.Ordinal);
        Assert.Contains("does not validate", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BytesThatAreNotACanonicalDocumentAreRefused()
    {
        IPipelineCoordinatorGrain coordinator = cluster.Cluster.Client
            .GetGrain<IPipelineCoordinatorGrain>("not-a-document");

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => coordinator.StartRunAsync([0x7B, 0x7D]));

        Assert.Contains("canonical serialization", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADocumentAddressedToTheWrongCoordinatorIsRefusedNamingBoth()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("right-pipeline", 2);

        IPipelineCoordinatorGrain wrong = cluster.Cluster.Client
            .GetGrain<IPipelineCoordinatorGrain>("wrong-pipeline");

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => wrong.StartRunAsync(GraphDocumentSerializer.Serialize(pipeline.Document)));

        Assert.Contains("right-pipeline", refused.Message, StringComparison.Ordinal);
        Assert.Contains("wrong-pipeline", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadingAResultUnderAnotherDocumentsFingerprintIsRefusedNamingBoth()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("fingerprint-guard", 2);
        (PipelineDefinition other, ResultSlot<long> _) = TestPipelines.Doubling("fingerprint-guard-other", 3);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        IPipelineRunGrain run = cluster.Cluster.Client
            .GetGrain<IPipelineRunGrain>($"{handle.Ticket.GraphId}/{handle.RunId}");

        ArgumentException refused = await Assert.ThrowsAsync<ArgumentException>(
            () => run.GetResultAsync(handle.Epoch, TestPipelines.TotalSlot, other.Fingerprint.ToString()));

        Assert.Contains(other.Fingerprint.ToString(), refused.Message, StringComparison.Ordinal);
        Assert.Contains(pipeline.Fingerprint.ToString(), refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadingAResultTheDocumentDoesNotDeclareIsRefusedListingTheOnesItDoes()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("unknown-slot", 2);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        IPipelineRunGrain run = cluster.Cluster.Client
            .GetGrain<IPipelineRunGrain>($"{handle.Ticket.GraphId}/{handle.RunId}");

        ArgumentException refused = await Assert.ThrowsAsync<ArgumentException>(
            () => run.GetResultAsync(handle.Epoch, "no-such-slot", pipeline.Fingerprint.ToString()));

        Assert.Contains("no-such-slot", refused.Message, StringComparison.Ordinal);
        Assert.Contains($"'{TestPipelines.TotalSlot}'", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnInvalidRunIdentityIsRefusedByTheCoordinator()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("bad-run-id", 2);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        IPipelineCoordinatorGrain coordinator = cluster.Cluster.Client
            .GetGrain<IPipelineCoordinatorGrain>(pipeline.Id.Value);

        ArgumentException refused = await Assert.ThrowsAsync<ArgumentException>(
            () => coordinator.GetStatusAsync("Not A Run Id", handle.Epoch));

        Assert.Contains("not a valid run identifier", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASiloWithNoFactoryForAProviderRefusesTheDocumentItCanStillValidate()
    {
        // A silo of its own, registering the catalog and deliberately no factory: the state a rolling
        // upgrade produces when a package's stages are published before its runtime is deployed.
        Orleans.TestingHost.InProcessTestClusterBuilder builder = new(initialSilosCount: 1);

        builder.ConfigureSilo((siloOptions, silo) =>
        {
            _ = silo.AddMemoryGrainStorage(OrleansDataflowStorage.CoordinatorProviderName);
            _ = silo.AddOrleansDataflow(dataflow => dataflow.AddCatalog(TestVocabulary.Catalog()));
        });

        await using Orleans.TestingHost.InProcessTestCluster validating = builder.Build();

        await validating.DeployAsync();

        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("no-factory", 2);

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => new OrleansDataflowHost(validating.Client).MaterializeAsync(pipeline, Token));

        Assert.Contains("registers no runtime factory", refused.Message, StringComparison.Ordinal);
        Assert.Contains("test/range@v1", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASiloPublishingNoVocabularyAtAllIsRefusedWhenItStarts()
    {
        string reported = await RefusedSilo(dataflow => dataflow.AddFactory(
            TestVocabulary.Provider,
            new TestStageFactory()));

        Assert.Contains("at least one vocabulary", reported, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASiloThatPublishesOnlyAShippedVocabularyStarts()
    {
        // What the vocabulary check is actually about is a silo that can resolve nothing, and a silo holding
        // the ten Orleans adapter stages and their factory is not that silo. Requiring an AddCatalog call of
        // it as well would have asked for a token, and the only tokens available were a shipped catalog
        // registered without its factory — which is exactly the "accepts a document and refuses it at
        // materialization" state the check exists to prevent.
        await using Orleans.TestingHost.InProcessTestCluster cluster = Cluster(dataflow => dataflow
            .AddStreamElement(StreamElementBinding.Create(ElementContract.For<long>("cluster-refusal-tick", 1))));

        await cluster.DeployAsync();

        Assert.NotNull(cluster.Client);
    }

    [Fact]
    public async Task ASiloThatPublishesOnlyTheDotnetVocabularyStarts()
    {
        await using Orleans.TestingHost.InProcessTestCluster cluster = Cluster(
            dataflow => dataflow.AddDotnetStages());

        await cluster.DeployAsync();

        Assert.NotNull(cluster.Client);
    }

    [Fact]
    public async Task AProviderRegisteredTwiceIsRefusedWhenTheSiloStarts()
    {
        string reported = await RefusedSilo(dataflow => dataflow
            .AddCatalog(TestVocabulary.Catalog())
            .AddFactory(TestVocabulary.Provider, new TestStageFactory())
            .AddFactory(TestVocabulary.Provider, new TestStageFactory()));

        Assert.Contains("more than one runtime factory", reported, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OneStageRegisteredByTwoCatalogsIsRefusedWhenTheSiloStarts()
    {
        // The shape a deployment reaches by composing two packages that both publish a shared vocabulary.
        // Merging them silently would mean choosing one of two specifications for one reference without
        // anything to choose by, so the union refuses instead and says which reference collided.
        string reported = await RefusedSilo(dataflow => dataflow
            .AddCatalog(TestVocabulary.Catalog())
            .AddCatalog(TestVocabulary.Catalog())
            .AddFactory(TestVocabulary.Provider, new TestStageFactory()));

        Assert.Contains("repeats the stage reference", reported, StringComparison.Ordinal);
        Assert.Contains("test/range@v1", reported, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADefaultProviderCannotBeGivenAFactory()
    {
        string reported = await RefusedSilo(dataflow => dataflow
            .AddCatalog(TestVocabulary.Catalog())
            .AddFactory(default(ProviderId), new TestStageFactory()));

        Assert.Contains("names no provider", reported, StringComparison.Ordinal);
    }

    /// <summary>Deploys a silo with a broken registration and reports what stopped it.</summary>
    /// <param name="configure">The registration to make.</param>
    /// <returns>The text of everything the failure said, inner exceptions included.</returns>
    /// <remarks>
    /// A silo builder's configuration delegates run when the host starts rather than when the builder is
    /// built, so a bad registration is a failure to start and not a failure to build. The host wraps what
    /// it caught, which is why the whole chain is rendered rather than one message: the assertion is about
    /// what a deployment is told, and a deployment reads the log.
    /// </remarks>
    private static async Task<string> RefusedSilo(Action<IOrleansDataflowBuilder> configure)
    {
        await using Orleans.TestingHost.InProcessTestCluster refused = Cluster(configure);

        Exception failure = await Assert.ThrowsAnyAsync<Exception>(refused.DeployAsync);

        return failure.ToString();
    }

    /// <summary>Builds a one-silo cluster carrying one dataflow registration and nothing else.</summary>
    /// <param name="configure">The registration to make.</param>
    /// <returns>The undeployed cluster.</returns>
    /// <remarks>
    /// Shared by the refusals and by the two registrations that are meant to succeed, so that "this starts"
    /// and "this does not" are the same silo differing only in what was registered.
    /// </remarks>
    private static Orleans.TestingHost.InProcessTestCluster Cluster(Action<IOrleansDataflowBuilder> configure)
    {
        Orleans.TestingHost.InProcessTestClusterBuilder builder = new(initialSilosCount: 1);

        builder.ConfigureSilo((siloOptions, silo) => _ = silo.AddOrleansDataflow(configure));

        return builder.Build();
    }
}
