using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What a run with boundaries in it does at the edges of its own life: no elements, no time to start, two
/// runs at once, and bounds at the top of their range.
/// </summary>
/// <remarks>
/// Checkpoint 1 fixed every one of these for a run that was one loop. None of them follows for a run that
/// is several loops joined by channels — a segment that never receives anything has to end anyway, a run
/// stopped before it began has to release channels nobody wrote to, and two runs of one graph now have
/// channels to keep apart as well as enumerators.
/// </remarks>
public sealed class BoundaryLifecycleTests
{
    [Fact]
    public async Task AnEmptySourceCompletesEverySegmentAndResolvesTheSeed()
    {
        RecordingEnumerable<int> elements = new();

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 4 })
            .SelectAsync(new ParallelismOptions { MaxConcurrency = 3 }, (value, _) => Task.FromResult((long)value))
            .Buffer(new BufferOptions { Capacity = 2 })
            .To(s => s.Aggregate(7L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(7L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(0, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task ARunCancelledBeforeItsFirstPullNeverTouchesTheSourceHoweverManySegmentsItHas()
    {
        using CancellationTokenSource cancellation = new();
        await cancellation.CancelAsync();

        RecordingEnumerable<int> elements = new(1, 2, 3);

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 4 })
            .SelectAsync(new ParallelismOptions { MaxConcurrency = 3 }, (value, _) => Task.FromResult((long)value))
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.GetValueAsync(total, TestToken));

        Assert.Equal(0, elements.Enumerations);
        Assert.Equal(0, elements.Pulls);
    }

    [Fact]
    public async Task ShutdownBeforeTheFirstPullCompletesARunWithBoundaries()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3);

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 4 })
            .SelectAsync(new ParallelismOptions { MaxConcurrency = 2 }, (value, _) => Task.FromResult((long)value))
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        // Racing the run to its first element is not the point; whichever of the two happens first, the
        // run completes successfully and resolves the state it reached.
        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.ShutdownAsync();

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.InRange(await run.GetValueAsync(total, TestToken), 0L, 6L);
        Assert.Equal(elements.Enumerations, elements.Releases);
    }

    [Fact]
    public async Task TwoRunsOfOneBufferedGraphKeepIndependentStateAndIndependentChannels()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3);

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 2 })
            .SelectAsync(new ParallelismOptions { MaxConcurrency = 2 }, (value, _) => Task.FromResult((long)value))
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle first = await Host.MaterializeAsync(graph, TestToken);
        await using RunHandle second = await Host.MaterializeAsync(graph, TestToken);

        await Task.WhenAll(first.Completion, second.Completion);

        Assert.Equal(6L, await first.GetValueAsync(total, TestToken));
        Assert.Equal(6L, await second.GetValueAsync(total, TestToken));
        Assert.Equal(2, elements.Enumerations);
        Assert.Equal(6, elements.Pulls);
        Assert.Equal(2, elements.Releases);
    }

    [Fact]
    public async Task ABoundAtTheTopOfItsRangeIsADeclarationRatherThanAnAllocation()
    {
        // Both bounds are validated as positive and nothing else, so the largest legal one has to run. A
        // window or a channel sized by the declared bound rather than by what actually arrives would fail
        // to allocate before the first element moved.
        RecordingEnumerable<int> elements = new(1, 2, 3);

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = int.MaxValue })
            .SelectAsync(new ParallelismOptions { MaxConcurrency = int.MaxValue }, (value, _) => Task.FromResult((long)value))
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task AFailingTerminalReleasesASourceThatIsWaitingForRoom()
    {
        // The deadlock this shape invites: the terminal fails while the source is parked inside a full
        // buffer's offer, so nothing will ever take an element from that buffer again. The failure has to
        // reach the source as a cancellation rather than as silence.
        InvalidOperationException failure = new("the folder refuses the second element");
        RecordingEnumerable<int> elements = new([.. Enumerable.Range(1, 50)]);

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 1 })
            .To(
                s => s.Aggregate(0L, (sum, value) => value == 2 ? throw failure : sum + value),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(total, TestToken)));

        // Two to reach the failing element, and at most two more: one in the buffer and one the source was
        // holding. A source that had run away would have pulled all fifty.
        Assert.InRange(elements.Pulls, 2, 4);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task AFailingCallbackReleasesASourceThatIsWaitingForRoom()
    {
        InvalidOperationException failure = new("the callback refuses the second element");
        RecordingEnumerable<int> elements = new([.. Enumerable.Range(1, 50)]);

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 1 })
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 1 },
                (value, _) => value == 2 ? throw failure : Task.FromResult((long)value))
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // Thrown before the callback returned a task at all, which is the case a runtime that only watched
        // the returned task would miss.
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.InRange(elements.Pulls, 2, 5);
        Assert.Equal(1, elements.Releases);
    }
}
