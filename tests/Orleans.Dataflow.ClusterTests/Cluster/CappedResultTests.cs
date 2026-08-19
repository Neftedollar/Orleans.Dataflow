using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.ClusterTests.Provider;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// Whether the bound a deployment declares is the bound the run grain applies.
/// </summary>
/// <remarks>
/// Without this, every other result-size test would be a statement about one number written in one place,
/// and the option would be a member nobody proved was read. The silo here caps a result at a few hundred
/// bytes, which nothing but a deliberate registration could produce.
/// </remarks>
public sealed class CappedResultTests : IAsyncLifetime
{
    private readonly CappedResultCluster _cluster = new();

    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    /// <inheritdoc/>
    public ValueTask InitializeAsync() => _cluster.InitializeAsync();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _cluster.DisposeAsync();

    [Fact]
    public async Task ASiloReportsItsOwnBoundRatherThanTheDefault()
    {
        // A block of a kibibyte is far inside the default and far outside this silo's, so the refusal names
        // this silo's number and could not have come from the default.
        (PipelineDefinition pipeline, ResultSlot<long> _, ResultSlot<byte[]> payload) =
            TestPipelines.Branching("declared-bound", 2, 1024);

        await using OrleansRunHandle handle = await _cluster.MaterializeAsync(pipeline);

        ResultTooLargeException refused = await Assert.ThrowsAsync<ResultTooLargeException>(
            async () => await handle.GetValueAsync(payload, Token));

        Assert.Equal(CappedResultCluster.MaximumResultBytes, refused.MaximumBytes);
        Assert.NotEqual(OrleansDataflowResults.DefaultMaximumResultBytes, refused.MaximumBytes);
    }

    [Fact]
    public async Task AResultInsideTheDeclaredBoundIsSentAsUsual()
    {
        // The other side of the same bound, so that "it refuses" is a statement about the size rather than
        // about this silo refusing everything.
        (PipelineDefinition pipeline, ResultSlot<long> total, ResultSlot<byte[]> payload) =
            TestPipelines.Branching("inside-bound", 3, 64);

        await using OrleansRunHandle handle = await _cluster.MaterializeAsync(pipeline);

        Assert.Equal(64, (await handle.GetValueAsync(payload, Token)).Length);
        Assert.Equal(6L, await handle.GetValueAsync(total, Token));
    }
}
