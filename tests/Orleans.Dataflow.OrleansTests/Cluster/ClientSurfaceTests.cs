using Microsoft.Extensions.DependencyInjection;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.OrleansTests.Provider;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// The small surface a client actually touches: how the host is registered, how it is configured, and what
/// a caller's own cancellation does to a wait.
/// </summary>
/// <remarks>
/// Each of these is a member a user reaches for on their first afternoon with the library, and none of them
/// is covered by the end-to-end tests, which construct what they need directly. A registration nobody
/// resolves and an option nobody sets are exactly the parts that break quietly.
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class ClientSurfaceTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    /// <value>The ambient test's own cancellation token.</value>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public void TheClientRegistrationResolvesOneHostAndTheOptionsItWasConfiguredWith()
    {
        OrleansDataflowHost host = cluster.Cluster.Client.ServiceProvider
            .GetRequiredService<OrleansDataflowHost>();

        OrleansDataflowClientOptions options = cluster.Cluster.Client.ServiceProvider
            .GetRequiredService<OrleansDataflowClientOptions>();

        // A singleton, because a host is stateless and holds no run: two hosts would be two of nothing.
        Assert.Same(host, cluster.Cluster.Client.ServiceProvider.GetRequiredService<OrleansDataflowHost>());
        Assert.Equal(TimeSpan.FromMilliseconds(10), options.PollInterval);
    }

    [Fact]
    public async Task DurableOptionsAreCheckedBeforeAnythingReachesTheCluster()
    {
        PipelineDefinition pipeline = TestPipelines.Recording("client-durable-guard", count: 1, "client-guard");

        // Checked here as well as by the silo, and both are worth having: this one makes a mistake a fast,
        // well-worded exception on the caller's own thread, and the silo's makes it impossible for a
        // hand-built call to get past. What each refusal names is the member that is wrong.
        ArgumentException named = await Assert.ThrowsAsync<ArgumentException>(
            () => cluster.Host.MaterializeDurableAsync(
                pipeline,
                new DurablePipelineOptions { RunId = "Not A Run Id" },
                Token));

        Assert.Contains("run identifier", named.Message, StringComparison.Ordinal);

        ArgumentException interval = await Assert.ThrowsAsync<ArgumentException>(
            () => cluster.Host.MaterializeDurableAsync(
                pipeline,
                new DurablePipelineOptions { RunId = "guarded", Interval = TimeSpan.Zero },
                Token));

        Assert.Contains("due forever", interval.Message, StringComparison.Ordinal);

        ArgumentException elements = await Assert.ThrowsAsync<ArgumentException>(
            () => cluster.Host.MaterializeDurableAsync(
                pipeline,
                new DurablePipelineOptions { RunId = "guarded", EveryElements = 0 },
                Token));

        Assert.Contains("before an element exists", elements.Message, StringComparison.Ordinal);

        // Declaring neither bound is legal and means the run never touches the store, which is the honest
        // reading of the words rather than a mistake this type guesses at.
        await using OrleansRunHandle handle = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = "untimed" },
            Token);

        await handle.Completion;

        Assert.False(cluster.Checkpoints.Holds(
            Identity.GraphId.Create("client-durable-guard"),
            Identity.RunId.Create("untimed")));
    }

    [Fact]
    public void ThePollIntervalDefaultsToSomethingShortAndRefusesSomethingImpossible()
    {
        OrleansDataflowClientOptions options = new();

        Assert.Equal(OrleansDataflowClientOptions.DefaultPollInterval, options.PollInterval);
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => options.PollInterval = TimeSpan.Zero);
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => options.PollInterval = TimeSpan.FromTicks(-1));
        Assert.Equal(OrleansDataflowClientOptions.DefaultPollInterval, options.PollInterval);
    }

    [Fact]
    public async Task ACallersOwnTokenStopsTheirWaitAndLeavesTheRunAlone()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) =
            TestPipelines.Doubling("caller-token", 2, halt: "caller-token");

        OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await TestSignals.Reached("caller-token");

        using CancellationTokenSource giveUp = new();

        await giveUp.CancelAsync();

        // The run is deliberately one that never ends on its own, so a wait that returned at all could only
        // have returned because the caller's token said so.
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => handle.GetValueAsync(slot, giveUp.Token));

        // And the run is untouched by that: the caller stopped looking, not the run.
        Assert.Equal(RunPhase.Running, (await Run(handle).GetStatusAsync(handle.Epoch)).Phase);

        await handle.DisposeAsync();
    }

    [Fact]
    public async Task AHandleRendersItselfAsSomethingAReaderCanFindInALog()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("handle-text", 2);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        string text = handle.ToString();

        Assert.Contains(handle.RunId, text, StringComparison.Ordinal);
        Assert.Contains("handle-text", text, StringComparison.Ordinal);
        Assert.Contains("epoch", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MaterializingRefusesTheNullPipelineRatherThanAddressingNothing()
    {
        _ = await Assert.ThrowsAsync<ArgumentNullException>(() => cluster.Host.MaterializeAsync(null!, Token));
        _ = Assert.Throws<ArgumentNullException>(() => new OrleansDataflowHost(null!));
    }

    /// <summary>Addresses the run grain a handle stands in front of.</summary>
    /// <param name="handle">The handle.</param>
    /// <returns>The grain.</returns>
    private IPipelineRunGrain Run(OrleansRunHandle handle) =>
        cluster.Cluster.Client.GetGrain<IPipelineRunGrain>($"{handle.Ticket.GraphId}/{handle.RunId}");
}
