using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Orleans.Hosting;
using Orleans.TestingHost;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// A silo whose whole vocabulary is one <see cref="StageProvider"/>, registered in one call.
/// </summary>
/// <remarks>
/// The in-process tests say that a vocabulary written in one place publishes the same catalog and builds the
/// same runtime as one written in three. This says the remaining thing they cannot: that
/// <c>AddProvider</c> is a real silo registration — it satisfies the vocabulary requirement, it puts the
/// specifications where the coordinator validates documents against them, and it puts the factory where the
/// run grain reaches for it. A silo is the only place all three are true at once.
/// </remarks>
public sealed class StageProviderClusterTests : IAsyncLifetime
{
    /// <summary>The provider this silo publishes.</summary>
    private const string ProviderName = "one-call";

    /// <summary>The payload member both stages read.</summary>
    private const string CountMember = "n";

    /// <summary>The reference of the counting source.</summary>
    private static readonly StageRef NumbersStage = StageRef.For(ProviderName, "numbers");

    /// <summary>The reference of the summing terminal.</summary>
    private static readonly StageRef SumStage = StageRef.For(ProviderName, "sum");

    /// <summary>The contract of the elements both stages carry.</summary>
    private static readonly ElementContract<int> Number = ElementContract.For<int>("one-call-number", 1);

    /// <summary>The contract of the total the terminal answers with.</summary>
    private static readonly ResultContract<long> Total = ResultContract.For<long>("one-call-total", 1);

    /// <summary>The contract of both stages' payloads.</summary>
    private static readonly ContractReference Parameters = ContractReference.For("one-call-parameters");

    /// <summary>Gets the deployed cluster.</summary>
    private InProcessTestCluster Cluster { get; set; } = null!;

    /// <inheritdoc/>
    public async ValueTask InitializeAsync()
    {
        InProcessTestClusterBuilder builder = new(initialSilosCount: 1);

        builder.ConfigureSilo((siloOptions, silo) =>
        {
            _ = silo.AddMemoryGrainStorage(OrleansDataflowStorage.CoordinatorProviderName);

            // The whole deployment story: one line, and the catalog and the factory cannot have drifted.
            _ = silo.AddOrleansDataflow(dataflow => dataflow.AddProvider(Vocabulary()));
        });

        builder.ConfigureClientHost(client =>
            client.Services.AddOrleansDataflowClient(options =>
                options.PollInterval = TimeSpan.FromMilliseconds(10)));

        Cluster = builder.Build();

        await Cluster.DeployAsync();
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.DisposeAsync();
        }
    }

    [Fact]
    public async Task AVocabularyRegisteredInOneCallRunsOnASilo()
    {
        OrleansDataflowHost host = Cluster.Client.ServiceProvider.GetRequiredService<OrleansDataflowHost>();
        IStageCatalog catalog = Vocabulary().Catalog;

        RunnableGraph graph = Source
            .FromRegistered(RegisteredStage.Source(catalog, NumbersStage, Number), "numbers", Payload(10))
            .To(
                RegisteredStage.SinkWithResult(catalog, SumStage, Number, Total),
                "total",
                Payload(5),
                "total",
                out ResultSlot<long> _);

        PipelineDefinition pipeline = graph.AsPipeline(GraphId.Create("one-call"), GraphRevision.Create(1));
        ResultSlot<long> counted = pipeline.ResultSlot("total", Total);

        await using OrleansRunHandle run = await host.MaterializeAsync(
            pipeline,
            TestContext.Current.CancellationToken);

        RunEnding ending = await run.WatchTermination;

        Assert.Equal(RunEndingKind.Completed, ending.Kind);
        Assert.Equal(6L, await run.GetValueAsync(counted, TestContext.Current.CancellationToken));
    }

    /// <summary>Declares the two-stage vocabulary this silo publishes.</summary>
    /// <returns>The vocabulary.</returns>
    /// <remarks>
    /// A fresh vocabulary per call for the reason every catalog in this suite is fresh per call: two
    /// registrations should not quietly share one value. The catalogs the two calls produce are equal
    /// values, which is what lets the authoring side resolve handles against one and the silo run documents
    /// validated against the other.
    /// </remarks>
    private static StageProvider Vocabulary() =>
        StageProvider
            .Create(ProviderName)
            .Source(NumbersStage, Parameters, Port.Out("out", Number), request =>
            {
                int count = Count(request);

                return DataflowStageRuntime.Source(_ => Numbers(count));
            })
            .Sink(SumStage, Parameters, Port.In("in", Number), Port.Result("total", Total), request =>
            {
                int least = Count(request);

                return DataflowStageRuntime.Terminal(
                    static () => 0L,
                    (state, element) => (int)element! >= least ? (long)state! + 1L : state,
                    finish: null,
                    producesResult: true);
            });

    /// <summary>Reads the one number both stages carry.</summary>
    /// <param name="request">The node and its specification.</param>
    /// <returns>The number.</returns>
    private static int Count(DataflowStageRequest request) =>
        request.Node.Parameters.ToElement().GetProperty(CountMember).GetInt32();

    /// <summary>Writes the payload both stages carry.</summary>
    /// <param name="value">The number.</param>
    /// <returns>The canonical payload.</returns>
    private static CanonicalJsonValue Payload(int value) =>
        StageParameters.Create().Add(CountMember, value).Build();

    /// <summary>Emits the first numbers of the run.</summary>
    /// <param name="count">How many to emit.</param>
    /// <param name="cancellationToken">The enumeration's own token.</param>
    /// <returns>The numbers.</returns>
    private static async IAsyncEnumerable<object?> Numbers(
        int count,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (int index = 1; index <= count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            yield return index;

            await Task.Yield();
        }
    }
}
