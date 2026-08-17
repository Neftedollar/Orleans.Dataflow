using System.Threading.Tasks.Sources;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the value-task asynchronous stages promise, and that it is the very same promise the task-shaped
/// family makes.
/// </summary>
/// <remarks>
/// <para>
/// The two families are one implementation with a conversion in front of it, so these tests are the
/// evidence for that claim rather than a second copy of the asynchronous-stage suite: ordering under
/// callbacks that finish backwards, admission bounded by the declared concurrency, a failure that faults
/// the run with the author's own exception, and a cancellation that reaches the callback's token. Whatever
/// else the driver does is already proven where the driver is tested.
/// </para>
/// <para>
/// The single-consumption rule gets a test of its own, and a hostile one: a value task that refuses to be
/// consumed twice makes "the runtime awaits each exactly once" a fact the suite would notice losing,
/// rather than a sentence in a documentation comment.
/// </para>
/// </remarks>
public sealed class ValueTaskStageTests
{
    [Fact]
    public async Task OrderedEmissionFollowsInputOrderThoughTheCallbacksCompleteBackwards()
    {
        TaskCompletionSource<long>[] callbacks = Sources(3);
        TaskCompletionSource[] entered = Signals(3);
        List<long> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(0, 1, 2))
            .SelectValueTaskAsync(
                new ParallelismOptions { MaxConcurrency = 3 },
                async (value, token) =>
                {
                    entered[value].TrySetResult();

                    return await callbacks[value].Task.WaitAsync(token);
                })
            .To(s => s.Aggregate(0L, (sum, value) => Fold(observed, sum, value)), "total", out ResultSlot<long> total);

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
    public async Task UnorderedEmissionFollowsCompletionOrder()
    {
        TaskCompletionSource<long>[] callbacks = Sources(3);
        TaskCompletionSource[] entered = Signals(3);
        TaskCompletionSource[] emitted = Signals(3);
        List<long> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(0, 1, 2))
            .SelectValueTaskAsyncUnordered(
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
                        emitted[(int)(value / 100L) - 1].TrySetResult();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Task.WhenAll(entered.Select(signal => signal.Task));

        // Released one at a time and awaited in between, so the completion order is arranged rather than
        // hoped for: the third result is emitted before the first callback has even finished.
        callbacks[2].TrySetResult(300L);
        await emitted[2].Task;
        callbacks[0].TrySetResult(100L);
        await emitted[0].Task;
        callbacks[1].TrySetResult(200L);

        await run.Completion;

        Assert.Equal([300L, 100L, 200L], observed);
        Assert.Equal(600L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task AdmissionStopsAtTheDeclaredBound()
    {
        TaskCompletionSource<long>[] callbacks = Sources(4);
        TaskCompletionSource[] entered = Signals(4);

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(0, 1, 2, 3))
            .SelectValueTaskAsync(
                new ParallelismOptions { MaxConcurrency = 2 },
                async (value, token) =>
                {
                    entered[value].TrySetResult();

                    return await callbacks[value].Task.WaitAsync(token);
                })
            .To(Sink.Ignore<long>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Task.WhenAll(entered[0].Task, entered[1].Task);

        // Two in flight and no third started: a slot of the window is freed by emission, and nothing has
        // been emitted.
        Assert.False(entered[2].Task.IsCompleted);
        Assert.False(entered[3].Task.IsCompleted);

        for (int index = 0; index < callbacks.Length; index++)
        {
            callbacks[index].TrySetResult(index);
        }

        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
    }

    [Fact]
    public async Task ACallbackFailureFaultsTheRunWithThatInstanceAndCancelsTheOthers()
    {
        InvalidOperationException failure = new("the value-task callback refuses");
        TaskCompletionSource<long> held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource cancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(0, 1))
            .SelectValueTaskAsyncUnordered(
                new ParallelismOptions { MaxConcurrency = 2 },
                async (value, token) =>
                {
                    if (value == 1)
                    {
                        await entered.Task.WaitAsync(token);

                        throw failure;
                    }

                    entered.TrySetResult();

                    try
                    {
                        return await held.Task.WaitAsync(token);
                    }
                    catch (OperationCanceledException)
                    {
                        cancelled.TrySetResult();

                        throw;
                    }
                })
            .To(Sink.Ignore<long>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // The other callback is cancelled rather than left running, and the run faults with the very
        // exception the author threw.
        await cancelled.Task;

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
    }

    [Fact]
    public async Task TheCallbackReceivesTheRunsOwnTokenAndDisposalCancelsIt()
    {
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<long> held = new(TaskCreationOptions.RunContinuationsAsynchronously);

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(0, 1, 2))
            .SelectValueTaskAsync(
                new ParallelismOptions { MaxConcurrency = 1 },
                async (value, token) =>
                {
                    entered.TrySetResult();

                    return await held.Task.WaitAsync(token);
                })
            .To(Sink.Ignore<long>());

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await entered.Task;
        await run.DisposeAsync();

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
    }

    [Fact]
    public async Task ValueTasksThatCompletedSynchronouslyTravelTheOrdinaryPath()
    {
        // The case the family exists for: a callback that has the answer already. Nothing about the driver
        // changes, and the wrapper that converts the shape is what makes an already-finished value task an
        // already-finished task rather than a special case in the loop.
        List<long> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3, 4))
            .SelectValueTaskAsync(
                new ParallelismOptions { MaxConcurrency = 4 },
                (value, _) => ValueTask.FromResult((long)value * 10L))
            .SelectValueTaskAsyncUnordered(
                new ParallelismOptions { MaxConcurrency = 1 },
                (value, _) => ValueTask.FromResult(value + 1L))
            .To(s => s.Aggregate(0L, (sum, value) => Fold(observed, sum, value)), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([11L, 21L, 31L, 41L], observed);
        Assert.Equal(104L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task TheRuntimeConsumesEachValueTaskExactlyOnce()
    {
        // The rule a value task imposes on whoever consumes it, pinned by a source that refuses to be
        // consumed twice. A runtime that awaited one a second time, or that read its result after awaiting
        // it, would fault this run with the refusal instead of completing it.
        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .SelectValueTaskAsync(
                new ParallelismOptions { MaxConcurrency = 2 },
                (value, _) => new ValueTask<long>(new SingleUseResult(value * 10L), token: 0))
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(60L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task AValueTaskStageComposesInsideAReusableFlow()
    {
        Flow<int, long> doubled = Flow.For<int>()
            .SelectValueTaskAsync(new ParallelismOptions { MaxConcurrency = 2 }, (value, _) => ValueTask.FromResult((long)value))
            .SelectValueTaskAsyncUnordered(new ParallelismOptions { MaxConcurrency = 2 }, (value, _) => ValueTask.FromResult(value * 2L));

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Via(doubled)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(12L, await run.GetValueAsync(total, TestToken));
    }

    /// <summary>Records an element as the fold reaches it and adds it to the running total.</summary>
    /// <param name="observed">The sequence being recorded.</param>
    /// <param name="sum">The running total.</param>
    /// <param name="value">The element.</param>
    /// <returns>The new total.</returns>
    private static long Fold(List<long> observed, long sum, long value)
    {
        observed.Add(value);

        return sum + value;
    }

    /// <summary>Creates the sources a test completes by hand, one per element.</summary>
    /// <param name="count">How many to create.</param>
    /// <returns>The sources.</returns>
    private static TaskCompletionSource<long>[] Sources(int count) =>
        [.. Enumerable.Range(0, count).Select(_ => new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously))];

    /// <summary>Creates the signals a test awaits, one per element.</summary>
    /// <param name="count">How many to create.</param>
    /// <returns>The signals.</returns>
    private static TaskCompletionSource[] Signals(int count) =>
        [.. Enumerable.Range(0, count).Select(_ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously))];

    /// <summary>
    /// A value-task source that hands its result over once and refuses ever after.
    /// </summary>
    /// <param name="value">The result to hand over.</param>
    /// <remarks>
    /// The hostile half of the single-consumption rule. A real pooled source would not refuse — it would
    /// quietly serve whatever it had been recycled for, which is the bug this makes loud.
    /// </remarks>
    private sealed class SingleUseResult(long value) : IValueTaskSource<long>
    {
        private int _consumed;

        /// <inheritdoc/>
        public long GetResult(short token) =>
            Interlocked.Exchange(ref _consumed, 1) == 0
                ? value
                : throw new InvalidOperationException(
                    "This value task was consumed a second time. A value task may be awaited once, and its result read once, by exactly one consumer.");

        /// <inheritdoc/>
        public ValueTaskSourceStatus GetStatus(short token) => ValueTaskSourceStatus.Succeeded;

        /// <inheritdoc/>
        public void OnCompleted(
            Action<object?> continuation,
            object? state,
            short token,
            ValueTaskSourceOnCompletedFlags flags) => continuation(state);
    }
}
