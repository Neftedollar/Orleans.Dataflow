using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.ClusterTests.Provider;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// Who owns a run, and what happens to a caller who thinks they do and does not.
/// </summary>
/// <remarks>
/// An epoch orders claims to a run, and the only property worth asserting about it is that a wrong claim
/// fails loudly rather than quietly succeeding. Everything here is that one property seen from the three
/// control calls that carry an epoch.
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class ClusterFencingTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    /// <value>The ambient test's own cancellation token.</value>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ASecondRunOfOnePipelineIsIssuedAHigherEpoch()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("epoch-order", 2);

        await using OrleansRunHandle first = await cluster.Host.MaterializeAsync(pipeline, Token);
        await using OrleansRunHandle second = await cluster.Host.MaterializeAsync(pipeline, Token);

        Assert.True(second.Epoch > first.Epoch);
    }

    [Fact]
    public async Task AControlCallCarryingAnotherRunsEpochIsRejected()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) =
            TestPipelines.Doubling("stale-epoch", 3, halt: "stale-epoch");

        await using OrleansRunHandle first = await cluster.Host.MaterializeAsync(pipeline, Token);
        await using OrleansRunHandle second = await cluster.Host.MaterializeAsync(pipeline, Token);

        await TestSignals.Reached("stale-epoch");

        IPipelineRunGrain run = cluster.Cluster.Client
            .GetGrain<IPipelineRunGrain>($"{second.Ticket.GraphId}/{second.RunId}");

        PipelineFencingException refused =
            await Assert.ThrowsAsync<PipelineFencingException>(() => run.ShutdownAsync(first.Epoch));

        Assert.Equal(second.Epoch, refused.CurrentEpoch);
        Assert.Equal(first.Epoch, refused.CallerEpoch);
        Assert.Contains("epoch", refused.Message, StringComparison.Ordinal);

        // The refusal changed nothing: the run it addressed is still owned by its own epoch and still
        // stops when the right claim asks it to.
        await second.ShutdownAsync();
        await second.Completion;
    }

    [Fact]
    public async Task ReadingAResultWithAnotherRunsEpochIsRejected()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("stale-epoch-result", 2);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        IPipelineRunGrain run = cluster.Cluster.Client
            .GetGrain<IPipelineRunGrain>($"{handle.Ticket.GraphId}/{handle.RunId}");

        PipelineFencingException refused = await Assert.ThrowsAsync<PipelineFencingException>(
            () => run.GetResultAsync(handle.Epoch + 1, TestPipelines.TotalSlot, pipeline.Fingerprint.ToString()));

        Assert.Equal(handle.Epoch, refused.CurrentEpoch);
        Assert.Equal(handle.Epoch + 1, refused.CallerEpoch);
    }

    [Fact]
    public async Task AControlCallToARunThatWasNeverStartedReportsAbsenceRatherThanAStaleClaim()
    {
        // Two different questions with two different answers. A fencing refusal says a run exists and this
        // claim is not its, which sends a caller to their ticket; this says there is no run at all, which
        // sends them to the run's history. Answering absence with a fencing refusal carrying a zero epoch
        // would make every lost attempt look like an out-of-date claim.
        IPipelineRunGrain nowhere = cluster.Cluster.Client
            .GetGrain<IPipelineRunGrain>("never-started/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        PipelineRunLostException refused =
            await Assert.ThrowsAsync<PipelineRunLostException>(() => nowhere.ShutdownAsync(1L));

        Assert.Contains("No run is active", refused.Message, StringComparison.Ordinal);
        Assert.Contains("never started", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AStatusPollOfARunThatWasNeverStartedReportsThatRatherThanRefusing()
    {
        IPipelineRunGrain nowhere = cluster.Cluster.Client
            .GetGrain<IPipelineRunGrain>("never-started/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        RunStatusSnapshot status = await nowhere.GetStatusAsync(7L);

        Assert.Equal(RunPhase.NotStarted, status.Phase);
        Assert.Equal(0L, status.Epoch);
    }

    [Fact]
    public async Task StartingOneRunIdentityTwiceIsRejected()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) =
            TestPipelines.Doubling("one-identity-one-run", 2, halt: "one-identity-one-run");

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await TestSignals.Reached("one-identity-one-run");

        IPipelineRunGrain run = cluster.Cluster.Client
            .GetGrain<IPipelineRunGrain>($"{handle.Ticket.GraphId}/{handle.RunId}");

        byte[] canonical = Serialization.GraphDocumentSerializer.Serialize(pipeline.Document);

        PipelineFencingException refused = await Assert.ThrowsAsync<PipelineFencingException>(
            () => run.StartAsync(canonical, handle.Epoch + 1));

        Assert.Contains("already active", refused.Message, StringComparison.Ordinal);
    }
}
