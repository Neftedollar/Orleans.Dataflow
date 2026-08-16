using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// Where a run puts a boundary, and where it deliberately does not.
/// </summary>
/// <remarks>
/// <para>
/// Fusion is the default and the claim worth the most here: a chain the author wrote no boundary into
/// still holds exactly one element in flight, which is checkpoint 1's bound restated as a consequence of
/// the compilation rule rather than of the absence of the feature. A test that only checked results would
/// pass for a runtime that queued everything.
/// </para>
/// <para>
/// The counting tests read the bound off <see cref="RecordingEnumerable{T}.PeakInFlight"/>, which is a
/// statement about the whole run rather than a snapshot of it: an element counts as in flight from the
/// moment the source hands it over until the terminal is done with it, so a boundary that existed and
/// should not have, or held more than it should have, raises the peak whatever the timing.
/// </para>
/// </remarks>
public sealed class FusionTests
{
    [Fact]
    public async Task ASynchronousChainHoldsExactlyOneElementInFlightWhateverItsLength()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3, 4);
        Gate gate = new();

        RunnableGraph graph = Source.From(elements)
            .Select(value => value * 2)
            .Where(value => value > 0)
            .Select(value => value + 1)
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        gate.Wait();
                        elements.Consumed();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        // Three stages between the source and the terminal, no boundary between any of them, and therefore
        // no queue: the source has been asked for one element and no more.
        Assert.Equal(1, elements.Pulls);

        gate.Open();
        await run.Completion;

        Assert.Equal(1, elements.PeakInFlight);
        Assert.Equal(4, elements.Pulls);
        Assert.Equal(24L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(0L, run.DroppedElements);
    }

    [Fact]
    public async Task ABufferAdmitsItsCapacityPlusOneElementPerSegmentAndNoMore()
    {
        // One buffer of three between two segments: the terminal segment holds the element it is folding,
        // the channel holds three, and the source segment holds the one it cannot hand over yet. Five, and
        // the source stalls there for as long as the terminal is held.
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8, 9);
        Gate gate = new();
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);

        elements.Pulled = pulls =>
        {
            if (pulls == 5)
            {
                saturated.TrySetResult();
            }
        };

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 3 })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        gate.Wait();
                        elements.Consumed();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;
        await saturated.Task;

        // The source has handed over five and is waiting for room. It cannot be asked for a sixth until
        // the terminal takes one, and the terminal is held.
        Assert.Equal(5, elements.Pulls);

        gate.Open();
        await run.Completion;

        Assert.Equal(5, elements.PeakInFlight);
        Assert.Equal(9, elements.Pulls);
        Assert.Equal(45L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(0L, run.DroppedElements);
    }

    [Fact]
    public async Task ABufferInFrontOfAnAsynchronousStageIsThatStagesOwnChannel()
    {
        // The merge rule, and the only way to see it from outside is to count. One channel of three in
        // front of a stage admitting two callbacks bounds the run at six: two in flight, three queued, one
        // in the source's hand. A second channel between the buffer and the stage, with the relay segment
        // it would need, would make it eight.
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8, 9);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);

        elements.Pulled = pulls =>
        {
            if (pulls == 6)
            {
                saturated.TrySetResult();
            }
        };

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 3 })
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 2 },
                async (value, token) =>
                {
                    await release.Task.WaitAsync(token);

                    return value;
                })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        elements.Consumed();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await saturated.Task;

        release.TrySetResult();
        await run.Completion;

        Assert.Equal(6, elements.PeakInFlight);
        Assert.Equal(45L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task AChainOfEveryBoundaryDeliversEveryElementInOrder()
    {
        // Buffer, asynchronous stage, buffer: four segments and three channels, with fused mappings on
        // both sides of each of them.
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8);
        List<long> observed = [];

        RunnableGraph graph = Source.From(elements)
            .Select(value => value * 10)
            .Buffer(new BufferOptions { Capacity = 2 })
            .Where(value => value > 0)
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 3 },
                (value, _) => Task.FromResult((long)value + 1))
            .Select(value => value * 2)
            .Buffer(new BufferOptions { Capacity = 4 })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        observed.Add(value);

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([22L, 42L, 62L, 82L, 102L, 122L, 142L, 162L], observed);
        Assert.Equal(736L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(8, elements.Pulls);
        Assert.Equal(1, elements.Releases);
        Assert.Equal(0L, run.DroppedElements);
    }

    [Fact]
    public async Task AFlowCarryingABoundaryContributesOneToEveryPlaceItIsComposedInto()
    {
        // A reusable flow with a buffer in it is not one buffer shared between the graphs it joins, and
        // not one buffer shared between two places in the same graph: each occurrence is its own boundary
        // with its own channel.
        Flow<int, int> buffered = Flow.For<int>()
            .Buffer(new BufferOptions { Capacity = 2 })
            .Select(value => value + 1);

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Via(buffered)
            .Via(buffered)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        Assert.Equal(6, graph.Document.Nodes.Count);
        Assert.Equal(2, graph.Document.Nodes.Count(node => node.Stage.Stage.Value == "buffer"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(12L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task EveryRunOfAMultiSegmentGraphPublishesTheStateItsTerminalReached()
    {
        // The fold's state is written by the terminal segment's thread and read by whichever segment
        // happens to finish last, so a run that published it without a barrier between the two would
        // resolve a stale sum now and then rather than always. One run proves nothing about that; the
        // repetition is the test.
        for (int race = 0; race < 200; race++)
        {
            RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
                .Buffer(new BufferOptions { Capacity = 2 })
                .SelectAsyncUnordered(
                    new ParallelismOptions { MaxConcurrency = 3 },
                    (value, _) => Task.FromResult((long)value))
                .Buffer(new BufferOptions { Capacity = 2 })
                .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

            await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
            await run.Completion;

            Assert.Equal(55L, await run.GetValueAsync(total, TestToken));
        }
    }

    [Fact]
    public async Task TwoBuffersInARowAreTwoBoundariesAndTheirCapacitiesAdd()
    {
        // Adjacent buffers are not merged: the author declared two queues and the run holds two, with a
        // segment between them. Two of three, plus one element in each of the three segments, is nine.
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        Gate gate = new();
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);

        elements.Pulled = pulls =>
        {
            if (pulls == 9)
            {
                saturated.TrySetResult();
            }
        };

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 3 })
            .Buffer(new BufferOptions { Capacity = 3 })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        gate.Wait();
                        elements.Consumed();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;
        await saturated.Task;

        Assert.Equal(9, elements.Pulls);

        gate.Open();
        await run.Completion;

        Assert.Equal(9, elements.PeakInFlight);
        Assert.Equal(78L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(0L, run.DroppedElements);
    }
}
