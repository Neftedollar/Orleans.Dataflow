using System.Globalization;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What an asynchronous stage promises: how many callbacks run at once, in what order their results come
/// out, and what a failure or a stop does to the ones still running.
/// </summary>
/// <remarks>
/// <para>
/// Every callback here is a task a test completes by hand, so "the third finished before the first" is
/// something the test arranges rather than something it hopes for. Nothing waits on a clock.
/// </para>
/// <para>
/// The ordering claims are asserted on the whole delivered sequence rather than on a moment, because that
/// is what tells the two spellings apart: with the callbacks completed backwards, an ordered stage can only
/// deliver the input order and an unordered one can only deliver the reverse, so each test fails for the
/// other implementation whatever the timing.
/// </para>
/// </remarks>
public sealed class AsyncStageTests
{
    [Fact]
    public async Task OrderedEmissionFollowsInputOrderThoughTheCallbacksCompleteBackwards()
    {
        TaskCompletionSource<long>[] callbacks = Sources<long>(3);
        TaskCompletionSource[] entered = Signals(3);
        List<long> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(0, 1, 2))
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 3 },
                async (value, token) =>
                {
                    entered[value].TrySetResult();

                    return await callbacks[value].Task.WaitAsync(token);
                })
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

        await Task.WhenAll(entered.Select(signal => signal.Task));

        callbacks[2].TrySetResult(300L);
        callbacks[1].TrySetResult(200L);
        callbacks[0].TrySetResult(100L);

        await run.Completion;

        Assert.Equal([100L, 200L, 300L], observed);
        Assert.Equal(600L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task AdmissionContinuesToTheBoundWhileTheHeadBlocksEmission()
    {
        TaskCompletionSource<long>[] callbacks = Sources<long>(5);
        TaskCompletionSource[] entered = Signals(5);
        List<long> observed = [];
        int concurrent = 0;
        int peak = 0;

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(0, 1, 2, 3, 4))
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 3 },
                async (value, token) =>
                {
                    Observe(ref concurrent, ref peak, 1);
                    entered[value].TrySetResult();

                    try
                    {
                        return await callbacks[value].Task.WaitAsync(token);
                    }
                    finally
                    {
                        Observe(ref concurrent, ref peak, -1);
                    }
                })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        observed.Add(value);

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Task.WhenAll(entered[0].Task, entered[1].Task, entered[2].Task);

        // Three callbacks are running behind a head that has not finished, which is admission carrying on
        // while emission cannot. The fourth cannot start: a slot is freed by emission, and nothing has
        // been emitted.
        Assert.False(entered[3].Task.IsCompleted);
        Assert.False(entered[4].Task.IsCompleted);

        for (int index = 4; index >= 0; index--)
        {
            callbacks[index].TrySetResult(index * 100L);
        }

        await run.Completion;

        Assert.Equal([0L, 100L, 200L, 300L, 400L], observed);
        Assert.Equal(3, Volatile.Read(ref peak));
    }

    [Fact]
    public async Task UnorderedEmissionFollowsCompletionOrder()
    {
        TaskCompletionSource<long>[] callbacks = Sources<long>(3);
        TaskCompletionSource[] entered = Signals(3);
        TaskCompletionSource[] emitted = Signals(3);
        List<long> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(0, 1, 2))
            .SelectAsyncUnordered(
                new ParallelismOptions { MaxConcurrency = 3 },
                async (value, token) =>
                {
                    entered[value].TrySetResult();

                    return await callbacks[value].Task.WaitAsync(token);
                })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        observed.Add(value);
                        emitted[value / 100L].TrySetResult();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Task.WhenAll(entered.Select(signal => signal.Task));

        // Each result is let out and seen out before the next one is allowed to finish, so the completion
        // order is exactly the one written here and the emission order has nothing to race with.
        callbacks[2].TrySetResult(200L);
        await emitted[2].Task;

        callbacks[1].TrySetResult(100L);
        await emitted[1].Task;

        callbacks[0].TrySetResult(0L);
        await emitted[0].Task;

        await run.Completion;

        Assert.Equal([200L, 100L, 0L], observed);
        Assert.Equal(300L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task AnOrderedStageWithAMaximumOfOneIsTheSequentialAsynchronousMap()
    {
        // One callback at a time and emission before admission: the element is delivered all the way to
        // the terminal before the next one starts, which is what makes this spelling the sequential map
        // rather than a stage that merely happens to run one thing at a time.
        List<string> trace = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 1 },
                (value, _) =>
                {
                    trace.Add(Describe("start", value));

                    return Task.FromResult(value);
                })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        trace.Add(Describe("end", value));

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(
            ["start 1", "end 1", "start 2", "end 2", "start 3", "end 3"],
            trace);
        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task AFailingCallbackFaultsTheRunCancelsTheOthersAndStartsNoLaterElement()
    {
        InvalidOperationException failure = new("the callback refuses the first element");
        TaskCompletionSource neighbour = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource observedCancellation = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<long> parked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> started = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(0, 1, 2, 3, 4))
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 2 },
                async (value, token) =>
                {
                    started.Add(value);

                    if (value == 0)
                    {
                        // Fails only once the callback beside it is provably running, so that "the others
                        // were cancelled" is a claim about a callback that had started.
                        await neighbour.Task;

                        throw failure;
                    }

                    neighbour.TrySetResult();

                    try
                    {
                        return await parked.Task.WaitAsync(token);
                    }
                    catch (OperationCanceledException)
                    {
                        observedCancellation.TrySetResult();

                        throw;
                    }
                })
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(total, TestToken)));

        // The callback beside the failing one saw the run's token cancel, and the three elements behind
        // them never started at all.
        await observedCancellation.Task;
        Assert.Equal([0, 1], started);
    }

    [Fact]
    public async Task TheCallbackReceivesTheRunsTokenAndCancellationAbandonsWhatIsQueuedBehindIt()
    {
        using CancellationTokenSource cancellation = new();
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource observedCancellation = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<long> parked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<long> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3, 4))
            .Buffer(new BufferOptions { Capacity = 3 })
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 1 },
                async (value, token) =>
                {
                    entered.TrySetResult();

                    try
                    {
                        return await parked.Task.WaitAsync(token);
                    }
                    catch (OperationCanceledException)
                    {
                        observedCancellation.TrySetResult();

                        throw;
                    }
                })
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

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);
        await entered.Task;
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.GetValueAsync(total, TestToken));

        // The token the callback was handed is the run's own, and nothing behind the abandoned callback
        // was delivered.
        await observedCancellation.Task;
        Assert.Empty(observed);
    }

    [Fact]
    public async Task ShutdownDeliversTheBufferedElementsAndAwaitsTheCallbackInFlight()
    {
        Gate held = new();
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<long> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8)
        {
            Pulled = pulls =>
            {
                if (pulls == 5)
                {
                    saturated.TrySetResult();
                }
            },
        };

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 3 })
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 1 },
                (value, _) =>
                {
                    entered.TrySetResult();
                    held.Wait();

                    return Task.FromResult((long)value);
                })
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

        // One callback is in flight, the buffer holds three, and the source is holding a fifth element it
        // has nowhere to put.
        await entered.Task;
        await saturated.Task;

        Task shutdown = run.ShutdownAsync().AsTask();

        held.Open();
        await shutdown;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        // The callback that was running was awaited, the buffered elements were delivered, and the one the
        // source was holding went through too; the three it had not reached were never pulled.
        Assert.Equal([1L, 2L, 3L, 4L, 5L], observed);
        Assert.Equal(15L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(5, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task ACallbackThatCompletesSynchronouslyStillRunsWithinTheBound()
    {
        // The degenerate shape a bound could be lost in: nothing ever suspends, so every callback finishes
        // inside the admission loop and a stage that counted slots wrongly would run the whole sequence at
        // once without ever waiting.
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6);
        int concurrent = 0;
        int peak = 0;

        RunnableGraph graph = Source.From(elements)
            .SelectAsyncUnordered(
                new ParallelismOptions { MaxConcurrency = 2 },
                (value, _) =>
                {
                    Observe(ref concurrent, ref peak, 1);
                    Observe(ref concurrent, ref peak, -1);

                    return Task.FromResult((long)value * 2L);
                })
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(42L, await run.GetValueAsync(total, TestToken));
        Assert.InRange(Volatile.Read(ref peak), 1, 2);
        Assert.Equal(6, elements.Pulls);
    }

    /// <summary>Builds the per-element task sources a test completes by hand.</summary>
    /// <typeparam name="TResult">The result type of each callback.</typeparam>
    /// <param name="count">How many to build.</param>
    /// <returns>The sources, indexed by element.</returns>
    private static TaskCompletionSource<TResult>[] Sources<TResult>(int count) =>
        [.. Enumerable.Range(0, count).Select(_ => new TaskCompletionSource<TResult>(TaskCreationOptions.RunContinuationsAsynchronously))];

    /// <summary>Builds the per-element signals a test waits on.</summary>
    /// <param name="count">How many to build.</param>
    /// <returns>The signals, indexed by element.</returns>
    private static TaskCompletionSource[] Signals(int count) =>
        [.. Enumerable.Range(0, count).Select(_ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously))];

    /// <summary>Counts a callback in or out and remembers the greatest count seen.</summary>
    /// <param name="concurrent">The running count.</param>
    /// <param name="peak">The greatest count seen so far.</param>
    /// <param name="change">One to enter, minus one to leave.</param>
    /// <remarks>
    /// Interlocked because callbacks resume on whatever thread completed their task, so entering and
    /// leaving are genuinely concurrent even though admission is not.
    /// </remarks>
    private static void Observe(ref int concurrent, ref int peak, int change)
    {
        int running = Interlocked.Add(ref concurrent, change);
        int seen = Volatile.Read(ref peak);

        while (running > seen && Interlocked.CompareExchange(ref peak, running, seen) != seen)
        {
            seen = Volatile.Read(ref peak);
        }
    }

    /// <summary>Renders one step of the sequential trace.</summary>
    /// <param name="step">The word for the step.</param>
    /// <param name="value">The element.</param>
    /// <returns>Text of the form <c>start 1</c>.</returns>
    private static string Describe(string step, int value) =>
        string.Create(CultureInfo.InvariantCulture, $"{step} {value}");
}
