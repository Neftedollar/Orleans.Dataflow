using Microsoft.Extensions.DependencyInjection;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.ClusterTests.Provider;
using Orleans.Serialization;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// What a cluster does with a result too large to send, and what it does with the run that produced one.
/// </summary>
/// <remarks>
/// <para>
/// The foot-gun this closes is <c>Collect</c> over a cluster: a terminal whose result nothing in the
/// document bounds, on a message Orleans does not chunk. Without a cap the endings are a codec error whose
/// message is about buffers, a transport failure, or a poll that never answers; with one, the ending is a
/// named exception carrying the size and the bound.
/// </para>
/// <para>
/// The pipeline these tests run is a branching one, which is what makes the second claim provable at all:
/// two legs, two results, one small and one large, so "the run is not faulted and its other results
/// resolve" is a measurement rather than an argument. That pipeline is deployable only because its junction
/// is a registered stage — before M4.5 no multi-result document could reach a cluster.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class ResultSizeTests(DataflowCluster cluster)
{
    /// <summary>A result comfortably past the silo's default bound of one mebibyte.</summary>
    private const int OversizedBytes = 2 * 1024 * 1024;

    /// <summary>A result comfortably inside it.</summary>
    private const int ModestBytes = 4096;

    /// <summary>Gets the token that cancels a hung test rather than letting the run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ADeployableBranchingPipelineResolvesBothOfItsResults()
    {
        // The multi-result claim the capability matrix could not make over a cluster until a junction could
        // be registered: one run, two legs, two slots, resolved independently through the remote handle.
        (PipelineDefinition pipeline, ResultSlot<long> total, ResultSlot<byte[]> payload) =
            TestPipelines.Branching("branching-results", 4, ModestBytes);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        // 1 + 2 + 3 + 4 on the first leg, and the block the second leg's sink produced.
        Assert.Equal(10L, await handle.GetValueAsync(total, Token));
        Assert.Equal(ModestBytes, (await handle.GetValueAsync(payload, Token)).Length);
    }

    [Fact]
    public async Task AResultPastTheCapFailsItsSlotWithANamedErrorCarryingBothNumbers()
    {
        // Not a codec error and not a hung poll: an exception this package declares, naming the slot, the
        // size the value actually serializes to, and the bound the silo declared.
        (PipelineDefinition pipeline, ResultSlot<long> _, ResultSlot<byte[]> payload) =
            TestPipelines.Branching("oversized-result", 2, OversizedBytes);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        ResultTooLargeException refused = await Assert.ThrowsAsync<ResultTooLargeException>(
            async () => await handle.GetValueAsync(payload, Token));

        Assert.Equal(TestPipelines.PayloadSlot, refused.SlotName);
        Assert.Equal(OrleansDataflowResults.DefaultMaximumResultBytes, refused.MaximumBytes);
        Assert.True(
            refused.Bytes >= OversizedBytes,
            $"the block is {OversizedBytes} bytes and the measurement reported {refused.Bytes}");
        Assert.Contains("caps a result at", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheRunIsNotFaultedByAnOversizedResultAndItsOtherResultsStillResolve()
    {
        // The enforcement point stated as behaviour. The cap is applied where the envelope is built, on the
        // grain side, so what it refuses is sending one result: the run has already ended successfully, its
        // completion says so, and the sibling leg's result resolves exactly as it would have.
        (PipelineDefinition pipeline, ResultSlot<long> total, ResultSlot<byte[]> payload) =
            TestPipelines.Branching("oversized-but-completed", 3, OversizedBytes);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        await Assert.ThrowsAsync<ResultTooLargeException>(async () => await handle.GetValueAsync(payload, Token));

        // 1 + 2 + 3, from the other leg of the same run, read after the refusal.
        Assert.Equal(6L, await handle.GetValueAsync(total, Token));

        await handle.Completion;

        Assert.True(handle.Completion.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task ReadingAnOversizedResultTwiceRefusesTwiceRatherThanDegrading()
    {
        // A poll that never answers is one of the endings the cap replaces, so "it refuses" has to mean
        // "every time, promptly" rather than "the first time". The second read is the one that would hang if
        // the refusal had left the slot in some half-read state.
        (PipelineDefinition pipeline, ResultSlot<long> _, ResultSlot<byte[]> payload) =
            TestPipelines.Branching("oversized-twice", 1, OversizedBytes);

        await using OrleansRunHandle handle = await TestPipelines.RunAsync(cluster, pipeline);

        ResultTooLargeException first = await Assert.ThrowsAsync<ResultTooLargeException>(
            async () => await handle.GetValueAsync(payload, Token));
        ResultTooLargeException second = await Assert.ThrowsAsync<ResultTooLargeException>(
            async () => await handle.GetValueAsync(payload, Token));

        Assert.Equal(first.Bytes, second.Bytes);
    }

    [Fact]
    public void MeasuringANullResultIsZeroWorkRatherThanAFailure()
    {
        // A slot may legitimately resolve to null — an optional-first over an empty stream does — and the
        // measurement runs before anything knows that. It is the one input the meter could plausibly have
        // choked on, and a cap that threw on a null result would turn a legal outcome into an error.
        Serializer serializer = cluster.Cluster.Client.ServiceProvider.GetRequiredService<Serializer>();

        long measured = ResultSizeMeter.Measure(serializer, null);

        Assert.True(measured >= 0L);
        Assert.True(measured < OrleansDataflowResults.DefaultMaximumResultBytes);
    }

    [Fact]
    public void TheMeterCountsTheSerializedFormRatherThanTheObjectItWasHanded()
    {
        // The claim the cap rests on: what is measured is what would cross the wire. A block ten times the
        // size measures about ten times as much, which an estimate over the CLR object graph would not.
        Serializer serializer = cluster.Cluster.Client.ServiceProvider.GetRequiredService<Serializer>();

        long small = ResultSizeMeter.Measure(serializer, new byte[1024]);
        long large = ResultSizeMeter.Measure(serializer, new byte[10240]);

        Assert.True(small >= 1024L, $"a kibibyte measured {small}");
        Assert.True(large >= 10240L, $"ten kibibytes measured {large}");
        Assert.True(large - small >= 9216L, $"the difference was {large - small}");
    }

    [Fact]
    public void TheDefaultBoundIsOneMebibyteAndIsStatedInOnePlace()
    {
        // The number a deployment reads and the number the grain applies are the same constant, which is the
        // whole reason it is one. A default nobody chose is the one every deployment inherits.
        Assert.Equal(1024 * 1024, OrleansDataflowResults.DefaultMaximumResultBytes);
    }
}
