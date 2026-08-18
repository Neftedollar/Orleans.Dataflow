using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the two asynchronous folds promise: which states come out, how many folds run at once, what a
/// failure mid-fold does, and what a stop leaves behind.
/// </summary>
/// <remarks>
/// <para>
/// One claim underlies all of them and is asserted rather than argued: <b>an asynchronous fold is sequential
/// by construction</b>, because the state the next element folds into is this fold's answer. So there is no
/// bound to declare, no window to hold, and no boundary — and the tests that matter are the ones that would
/// fail for a stage built on the parallel machinery instead: the folds never overlap, the source is pulled
/// exactly as far as the element being folded, and a pause parks between two folds rather than around a
/// window of results.
/// </para>
/// <para>
/// Every hold is a task a test completes by hand, so "the run is inside the fold" is arranged rather than
/// hoped for.
/// </para>
/// </remarks>
public sealed class AsyncFoldTests
{
    [Fact]
    public async Task ScanAsyncEmitsEveryIntermediateStateAndNeverTheSeed()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .ScanAsync(0, async (sum, value, _) =>
            {
                await Task.Yield();

                return sum + value;
            })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // One state out per element in and the seed is where the fold starts rather than something that
        // happened, which is the synchronous scan's contract read through an await.
        Assert.Equal([1, 3, 6], observed);
    }

    [Fact]
    public async Task ScanAsyncOverAnEmptyStreamEmitsNothingAndNeverCallsItsFolder()
    {
        int calls = 0;
        List<int> observed = [];

        RunnableGraph graph = Source.Empty<int>()
            .ScanAsync(0, (sum, value, _) =>
            {
                Interlocked.Increment(ref calls);

                return Task.FromResult(sum + value);
            })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Empty(observed);
        Assert.Equal(0, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task EveryRunOfAScanAsyncStartsFromTheSeedAgain()
    {
        Flow<int, int> running = Flow.For<int>()
            .ScanAsync(0, (sum, value, _) => Task.FromResult(sum + value));
        List<int> first = [];
        List<int> second = [];

        RunnableGraph one = Source.From([1, 2]).Via(running).To(s => s.ForEach(first.Add));
        RunnableGraph two = Source.From([1, 2]).Via(running).To(s => s.ForEach(second.Add));

        await using (RunHandle run = await Host.MaterializeAsync(one, TestToken))
        {
            await run.Completion;
        }

        await using (RunHandle run = await Host.MaterializeAsync(two, TestToken))
        {
            await run.Completion;
        }

        // The state belongs to the run, so a flow carrying one starts from the seed in every graph it is
        // composed into and in every run of each.
        Assert.Equal([1, 3], first);
        Assert.Equal([1, 3], second);
    }

    [Fact]
    public async Task TheFoldsOfAnAsynchronousScanNeverOverlap()
    {
        int concurrent = 0;
        int peak = 0;
        List<int> observed = [];

        RunnableGraph graph = Source.Range(1, 16)
            .ScanAsync(0, async (sum, value, _) =>
            {
                Observe(ref concurrent, ref peak, 1);

                try
                {
                    await Task.Yield();

                    return sum + value;
                }
                finally
                {
                    Observe(ref concurrent, ref peak, -1);
                }
            })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // One fold at a time, with no number anywhere saying so: the next fold needs this one's answer, so
        // the shape is what bounds the concurrency rather than an admission rule.
        Assert.Equal(1, Volatile.Read(ref peak));
        Assert.Equal(136, observed[^1]);
    }

    [Fact]
    public async Task AnAsynchronousScanFusesWithTheStagesAroundIt()
    {
        TaskCompletionSource held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingEnumerable<int> source = new(1, 2, 3, 4, 5);

        RunnableGraph graph = Source.From(source)
            .ScanAsync(0, async (sum, value, _) =>
            {
                entered.TrySetResult();
                await held.Task;

                return sum + value;
            })
            .To(s => s.Ignore());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(entered.Task, "the run is inside the first fold");

        // No boundary, so no prefetch: the source has produced exactly the element being folded. A fold
        // built on the asynchronous stage's machinery would have a handoff channel in front of it and the
        // source would have run one further.
        Assert.Equal(1, source.Pulls);

        held.TrySetResult();
        await run.Completion;

        Assert.Equal(5, source.Pulls);
    }

    [Fact]
    public async Task AFailingFoldFaultsTheRunWithItsOwnException()
    {
        InvalidOperationException failure = new("the fold broke");

        RunnableGraph graph = Source.From([1, 2, 3])
            .ScanAsync(0, async (sum, value, _) =>
            {
                await Task.Yield();

                return value == 2 ? throw failure : sum + value;
            })
            .To(s => s.ForEach(_ => { }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion));
    }

    [Fact]
    public async Task AFolderThatReturnsNoTaskAtAllIsReportedAsASentence()
    {
        RunnableGraph graph = Source.From([1])
            .ScanAsync(0, (_, _, _) => null!)
            .To(s => s.ForEach(_ => { }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException refused =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion);

        Assert.Contains("no task", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheFoldReceivesTheRunsOwnTokenAndObservesItsCancellation()
    {
        using CancellationTokenSource cancellation = new();
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int cancelled = 0;

        RunnableGraph graph = Source.From([1, 2, 3])
            .ScanAsync(0, async (sum, value, token) =>
            {
                entered.TrySetResult();

                try
                {
                    await Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, token);
                }
                catch (OperationCanceledException)
                {
                    Interlocked.Increment(ref cancelled);

                    throw;
                }

                return sum + value;
            })
            .To(s => s.ForEach(_ => { }));

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

        await Reaches(entered.Task, "the run is inside the first fold");

        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run.Completion);
        Assert.Equal(1, Volatile.Read(ref cancelled));
    }

    [Fact]
    public async Task PausingAScanAsyncParksBetweenTwoFolds()
    {
        RunnableGraph graph = Source.Range(1, 6)
            .ScanAsync(0, async (sum, value, _) =>
            {
                await Task.Yield();

                return sum + value;
            })
            .To(TestSink.Probe<int>("out"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISinkProbe<int> sink = await run.GetValueAsync(graph.Control<ISinkProbe<int>>("out"), TestToken);

        Assert.Equal(1, await sink.ReceiveAsync(TestToken));

        // The double pause: the first may be answered by an ordinary park on the way to a wait, so the
        // second is the wait's own. A fold in flight blocks quiescence exactly as any other author callback
        // does, so reaching it twice is a statement that no fold is running and none will start.
        await Reaches(run.PauseAsync(TestToken), "the run reaches quiescence between two folds");
        await Reaches(run.ResumeAsync(), "the run moves again");
        await Reaches(run.PauseAsync(TestToken), "the run reaches quiescence a second time");

        Assert.True(run.IsPaused);

        await Reaches(run.ResumeAsync(), "the run moves again");

        // Nothing was lost across the hold and the states carry on from where the fold was: the running sums
        // of 1..6 and not a state restarted from the seed.
        Assert.Equal(3, await sink.ReceiveAsync(TestToken));
        Assert.Equal(6, await sink.ReceiveAsync(TestToken));
        Assert.Equal(10, await sink.ReceiveAsync(TestToken));
        Assert.Equal(15, await sink.ReceiveAsync(TestToken));
        Assert.Equal(21, await sink.ReceiveAsync(TestToken));
        await sink.ExpectCompletedAsync(TestToken);

        await run.Completion;
    }

    [Fact]
    public async Task AggregateAsyncResolvesItsSlotThroughTheOrdinaryMachinery()
    {
        RunnableGraph graph = Source.Range(1, 4)
            .To(
                s => s.AggregateAsync(0L, async (sum, value, _) =>
                {
                    await Task.Yield();

                    return sum + value;
                }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // A declared slot, resolved when the run ends, which is what "the result-bearing asynchronous
        // terminal" means and what ForEachAsync has never had.
        Assert.Equal(10L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task AggregateAsyncFoldsOneElementAtATime()
    {
        int concurrent = 0;
        int peak = 0;

        RunnableGraph graph = Source.Range(1, 16)
            .To(
                s => s.AggregateAsync(0L, async (sum, value, _) =>
                {
                    Observe(ref concurrent, ref peak, 1);

                    try
                    {
                        await Task.Yield();

                        return sum + value;
                    }
                    finally
                    {
                        Observe(ref concurrent, ref peak, -1);
                    }
                }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(1, Volatile.Read(ref peak));
        Assert.Equal(136L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task AFailingAsynchronousFoldFaultsTheRunAndItsSlot()
    {
        InvalidOperationException failure = new("the fold broke");

        RunnableGraph graph = Source.Range(1, 4)
            .To(
                s => s.AggregateAsync(0L, async (sum, value, _) =>
                {
                    await Task.Yield();

                    return value == 3 ? throw failure : sum + value;
                }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion));
        Assert.Same(
            failure,
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.GetValueAsync(total, TestToken)));
    }

    [Fact]
    public async Task AShutdownResolvesTheStateAnAsynchronousFoldHadSoFar()
    {
        RunnableGraph graph = TestSource.Probe<long>("emitted")
            .To(
                s => s.AggregateAsync(0L, async (sum, value, _) =>
                {
                    await Task.Yield();

                    return sum + value;
                }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISourceProbe<long> source = await run.GetValueAsync(graph.Control<ISourceProbe<long>>("emitted"), TestToken);

        await source.EmitAsync(4L, TestToken);
        await source.EmitAsync(6L, TestToken);
        await Reaches(run.ShutdownAsync().AsTask(), "the shutdown returns");
        await run.Completion;

        // Shutdown is "stop pulling and keep what you have", and an asynchronous fold's state is what it
        // has: the slot resolves rather than faulting or cancelling.
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(10L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task ACancelledRunCancelsTheSlotOfAnAsynchronousFold()
    {
        using CancellationTokenSource cancellation = new();

        RunnableGraph graph = Source.Range(1, 8)
            .To(
                s => s.AggregateAsync(0L, async (sum, value, _) =>
                {
                    if (value == 3)
                    {
                        await cancellation.CancelAsync();
                    }

                    return sum + value;
                }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run.Completion);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task TheNamedSinkSpellingBuildsTheSameAsynchronousFold()
    {
        RunnableGraph graph = Source.Range(1, 4)
            .To(
                Sink.AggregateAsync<int, long>(0L, (sum, value, _) => Task.FromResult(sum + value)),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(10L, await run.GetValueAsync(total, TestToken));
    }

    /// <summary>Records one entry into or exit from a callback and the greatest overlap seen.</summary>
    /// <param name="concurrent">The number of callbacks executing.</param>
    /// <param name="peak">The greatest number seen at once.</param>
    /// <param name="delta">One on entry and minus one on exit.</param>
    private static void Observe(ref int concurrent, ref int peak, int delta)
    {
        int running = Interlocked.Add(ref concurrent, delta);
        int highest = Volatile.Read(ref peak);

        while (running > highest)
        {
            int seen = Interlocked.CompareExchange(ref peak, running, highest);

            if (seen == highest)
            {
                return;
            }

            highest = seen;
        }
    }
}
