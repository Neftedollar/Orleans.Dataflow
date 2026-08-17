using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.OrleansTests.Provider;
using Xunit;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// What a pipeline does when a cluster runs it: it produces the result its author asked for, it reports
/// what went wrong when something does, and it stops the two ways a run can be stopped.
/// </summary>
/// <remarks>
/// The phase-1 proof lives here. Every pipeline in these tests is authored through the registered surface,
/// resolved from a catalog by identity inside a silo, built by a runtime factory registered beside it, and
/// read back through a remote handle — so a passing test is a statement about the whole path and not about
/// any one piece of it.
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class ClusterExecutionTests(DataflowCluster cluster)
{
    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    /// <value>The ambient test's own cancellation token.</value>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ARegisteredPipelineRunsInTheClusterAndItsResultArrivesThroughTheHandle()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) = TestPipelines.Doubling("sum-of-doubles", 4);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        // 2 + 4 + 6 + 8: the source emits one through four, the flow doubles each, the sink sums them.
        Assert.Equal(20L, await handle.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task AnEmptySourceResolvesTheSinkSeedRatherThanFailing()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) = TestPipelines.Doubling("sum-of-nothing", 0);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        Assert.Equal(0L, await handle.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task TheTicketReportsTheSameDocumentFingerprintTheClientComputed()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("fingerprint-round-trip", 3);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        Assert.Equal(pipeline.Fingerprint.ToString(), handle.Ticket.GraphFingerprint);
        Assert.Equal(pipeline.Id.Value, handle.Ticket.GraphId);
        Assert.NotEqual(string.Empty, handle.Ticket.CatalogFingerprint);
    }

    [Fact]
    public async Task AFailureInsideAStageSurfacesThroughCompletionAndThroughTheResultEnvelope()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) = TestPipelines.Failing("stage-throws", 5, failAt: 3);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        PipelineRunFailedException fromCompletion =
            await Assert.ThrowsAsync<PipelineRunFailedException>(() => handle.Completion);

        Assert.Equal(typeof(InvalidOperationException).FullName, fromCompletion.FailureType);
        Assert.Contains("fail at 3", fromCompletion.FailureMessage, StringComparison.Ordinal);

        PipelineRunFailedException fromSlot =
            await Assert.ThrowsAsync<PipelineRunFailedException>(() => handle.GetValueAsync(slot, Token));

        Assert.Equal(fromCompletion.FailureType, fromSlot.FailureType);
        Assert.Equal(fromCompletion.FailureMessage, fromSlot.FailureMessage);

        ResultEnvelope envelope = await Run(handle).GetResultAsync(
            handle.Epoch,
            TestPipelines.TotalSlot,
            pipeline.Fingerprint.ToString());

        Assert.Equal(RunPhase.Faulted, envelope.Phase);
        Assert.False(envelope.HasValue);
        Assert.Equal(typeof(InvalidOperationException).FullName, envelope.FailureType);
    }

    [Fact]
    public async Task ShutdownDrainsTheRunAndResolvesThePartialTotal()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) =
            TestPipelines.Doubling("drain-on-shutdown", 3, halt: "drain-on-shutdown");

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        // The source raises this after its third element and then waits for the run to be stopped, so what
        // the sink has seen when the shutdown lands is exactly one, two, and three rather than a race.
        await TestSignals.Reached("drain-on-shutdown");
        await handle.ShutdownAsync();
        await handle.Completion;

        Assert.Equal(12L, await handle.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task DisposingTheHandleCancelsTheRunAndItsResultResolvesNothing()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) =
            TestPipelines.Doubling("cancel-on-dispose", 3, halt: "cancel-on-dispose");

        OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await TestSignals.Reached("cancel-on-dispose");
        await handle.DisposeAsync();

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => handle.Completion);
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => handle.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task MaterializingOnePipelineTwiceStartsTwoRunsThatBothLive()
    {
        (PipelineDefinition pipeline, ResultSlot<long> slot) = TestPipelines.Doubling("two-runs", 4);

        await using OrleansRunHandle first = await cluster.Host.MaterializeAsync(pipeline, Token);
        await using OrleansRunHandle second = await cluster.Host.MaterializeAsync(pipeline, Token);

        Assert.NotEqual(first.RunId, second.RunId);
        Assert.NotEqual(first.Epoch, second.Epoch);

        await first.Completion;
        await second.Completion;

        Assert.Equal(20L, await first.GetValueAsync(slot, Token));
        Assert.Equal(20L, await second.GetValueAsync(slot, Token));
    }

    [Fact]
    public async Task AStatusPollAfterCompletionKeepsReportingTheSameOutcome()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("stable-status", 2);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        RunStatusSnapshot first = await Run(handle).GetStatusAsync(handle.Epoch);
        RunStatusSnapshot second = await Run(handle).GetStatusAsync(handle.Epoch);

        Assert.Equal(RunPhase.Completed, first.Phase);
        Assert.Equal(RunPhase.Completed, second.Phase);
        Assert.Equal(handle.Epoch, first.Epoch);
    }

    [Fact]
    public async Task TheCoordinatorAnswersForARunItStarted()
    {
        (PipelineDefinition pipeline, ResultSlot<long> _) = TestPipelines.Doubling("coordinator-passthrough", 3);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        RunStatusSnapshot status = await cluster.Cluster.Client
            .GetGrain<IPipelineCoordinatorGrain>(pipeline.Id.Value)
            .GetStatusAsync(handle.RunId, handle.Epoch);

        Assert.Equal(RunPhase.Completed, status.Phase);
    }

    /// <summary>Addresses the run grain a handle stands in front of.</summary>
    /// <param name="handle">The handle.</param>
    /// <returns>The grain.</returns>
    /// <remarks>
    /// The handle's own path is the one a user takes; going around it is how a test asserts on the envelope
    /// itself, which the handle deliberately unwraps into an exception or a value.
    /// </remarks>
    private IPipelineRunGrain Run(OrleansRunHandle handle) =>
        cluster.Cluster.Client.GetGrain<IPipelineRunGrain>($"{handle.Ticket.GraphId}/{handle.RunId}");
}
