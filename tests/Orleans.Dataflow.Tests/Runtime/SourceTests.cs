using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What each source emits, how it ends, and what a second run of the same graph gets.
/// </summary>
/// <remarks>
/// <para>
/// Every source is asserted on the elements it delivered rather than on a count, and every one that carries
/// state between elements is materialized twice, because a source that continued where the previous run
/// left off is exactly what one run cannot show.
/// </para>
/// <para>
/// The two sources whose behavior is entirely in the document — the empty one and the range — are the only
/// ones that could be executed by a runtime that never saw this process, and they are asserted here as
/// streams; that their documents say so is <see cref="Api.OperatorAuthoringTests"/>'s subject.
/// </para>
/// </remarks>
public sealed class SourceTests
{
    [Fact]
    public async Task AnEmptySourceCompletesAtOnceAndResolvesTheSeed()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.Empty<int>()
            .Select(value =>
            {
                observed.Add(value);

                return value;
            })
            .To(s => s.Aggregate(7L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Empty(observed);
        Assert.Equal(7L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task ASingleElementSourceEmitsThatElementAndEnds()
    {
        List<string> observed = [];

        RunnableGraph graph = Source.Single("only").To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(["only"], observed);
    }

    [Fact]
    public async Task ASingleElementSourceEmitsANullElementAsAnElement()
    {
        // The element is a value and not a signal, so null is a perfectly ordinary one. A source that
        // treated it as "nothing to emit" would silently turn a one-element stream into an empty one.
        List<string?> observed = [];

        RunnableGraph graph = Source.Single<string?>(null).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([null], observed);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(5)]
    public async Task ARepeatEmitsItsElementExactlyItsCountOfTimes(int count)
    {
        List<string> observed = [];

        RunnableGraph graph = Source.Repeat("x", count).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(count, observed.Count);
        Assert.All(observed, value => Assert.Equal("x", value));
    }

    [Fact]
    public async Task ARangeEmitsItsIntegersAscendingFromItsStart()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.Range(-2, 5).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([-2, -1, 0, 1, 2], observed);
    }

    [Fact]
    public async Task ARangeEndingAtTheLargestIntegerFinishesRatherThanOverflowing()
    {
        // The largest range the authoring surface admits at that end. A loop that compared against the
        // last element rather than counting would overflow on the comparison that was meant to stop it.
        List<int> observed = [];

        RunnableGraph graph = Source.Range(int.MaxValue - 2, 3).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([int.MaxValue - 2, int.MaxValue - 1, int.MaxValue], observed);
    }

    [Fact]
    public async Task ARangeOfNoElementsIsAnEmptyStream()
    {
        RunnableGraph graph = Source.Range(10, 0)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(0L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task ATaskSourceEmitsTheTasksValueOncePerRun()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.FromTask(Task.FromResult(42)).To(s => s.ForEach(observed.Add));

        await using (RunHandle first = await Host.MaterializeAsync(graph, TestToken))
        {
            await first.Completion;
        }

        await using (RunHandle second = await Host.MaterializeAsync(graph, TestToken))
        {
            await second.Completion;
        }

        // A completed task is a value and not an event, so it replays into every run of the graph.
        Assert.Equal([42, 42], observed);
    }

    [Fact]
    public async Task ATaskSourceWaitsForATaskThatHasNotFinishedYet()
    {
        TaskCompletionSource<int> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> observed = [];

        RunnableGraph graph = Source.FromTask(pending.Task).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.False(run.Completion.IsCompleted);

        pending.TrySetResult(7);
        await run.Completion;

        Assert.Equal([7], observed);
    }

    [Fact]
    public async Task AFailingTaskFaultsTheRunWithItsOwnExceptionRatherThanAnAggregateOne()
    {
        InvalidOperationException failure = new("the task refuses");
        TaskCompletionSource<int> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);

        pending.TrySetException(failure);

        RunnableGraph graph = Source.FromTask(pending.Task)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // Unwrapped and instance-identical: a task carries its failure inside an AggregateException, and a
        // runtime that reported that one would put a wrapper between an author's exception and the run.
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(total, TestToken)));
    }

    [Fact]
    public async Task ACancelledTaskFaultsTheRunRatherThanCancellingIt()
    {
        // The run was not asked to stop, so this is a source that could not produce its element rather
        // than a cancellation of the run. It follows the rule every stage follows: an
        // OperationCanceledException is the run's own cancellation only while the run is being cancelled,
        // and is a failure otherwise.
        TaskCompletionSource<int> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);

        pending.TrySetCanceled(new CancellationToken(canceled: true));

        RunnableGraph graph = Source.FromTask(pending.Task).To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);
        Assert.Equal(TaskStatus.Faulted, run.Completion.Status);
    }

    [Fact]
    public async Task ATaskSourceAcceptsTheTaskAnAsynchronousMethodReturns()
    {
        // The shape a check against Task<T> itself would reject: the task an async method returns is an
        // instance of a private class deriving from Task<T> rather than of Task<T>.
        List<int> observed = [];

        RunnableGraph graph = Source.FromTask(Produce()).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([13], observed);

        static async Task<int> Produce()
        {
            await Task.Yield();

            return 13;
        }
    }

    [Fact]
    public async Task AFailedSourceFaultsEveryRunWithTheVeryExceptionItWasGiven()
    {
        InvalidOperationException failure = new("this graph cannot run");
        List<int> observed = [];

        RunnableGraph graph = Source.Failed<int>(failure).To(s => s.ForEach(observed.Add));

        await using (RunHandle first = await Host.MaterializeAsync(graph, TestToken))
        {
            Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => first.Completion));
        }

        await using (RunHandle second = await Host.MaterializeAsync(graph, TestToken))
        {
            Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => second.Completion));
        }

        Assert.Empty(observed);
    }

    [Fact]
    public async Task AnUnfoldEmitsWhatItsGeneratorProducesUntilItStops()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.Unfold(
                1,
                (int state, out int value, out int next) =>
                {
                    value = state;
                    next = state * 3;

                    return state < 30;
                })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 3, 9, 27], observed);
    }

    [Fact]
    public async Task AnUnfoldThatStopsAtOnceIsAnEmptyStream()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.Unfold(
                0,
                (int state, out int value, out int next) =>
                {
                    value = state;
                    next = state;

                    return false;
                })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Empty(observed);
    }

    [Fact]
    public async Task EveryRunOfAnUnfoldBeginsAtItsSeedAgain()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.Unfold(
                1,
                (int state, out int value, out int next) =>
                {
                    value = state;
                    next = state + 1;

                    return state <= 3;
                })
            .To(s => s.ForEach(observed.Add));

        await using (RunHandle first = await Host.MaterializeAsync(graph, TestToken))
        {
            await first.Completion;
        }

        await using (RunHandle second = await Host.MaterializeAsync(graph, TestToken))
        {
            await second.Completion;
        }

        Assert.Equal([1, 2, 3, 1, 2, 3], observed);
    }

    [Fact]
    public async Task CancellationStopsASourceThatWouldNeverStopByItself()
    {
        // An endless source is a legitimate thing to write, so the ordinary way out of one has to work:
        // cancellation is examined before every pull, and a run of it ends where it stands.
        using CancellationTokenSource cancellation = new();
        Gate gate = new();
        long delivered = 0;

        RunnableGraph graph = Source.Unfold(
                0L,
                (long state, out long value, out long next) =>
                {
                    value = state;
                    next = state + 1L;

                    return true;
                })
            .To(s => s.ForEach(_ =>
            {
                if (Interlocked.Increment(ref delivered) == 1L)
                {
                    gate.Wait();
                }
            }));

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);
        await gate.Reached;
        await cancellation.CancelAsync();

        gate.Open();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);
    }

    [Fact]
    public async Task AGeneratorThatThrowsFaultsTheRunWithTheExceptionItThrew()
    {
        InvalidOperationException failure = new("the generator refuses the third state");
        List<int> observed = [];

        RunnableGraph graph = Source.Unfold(
                1,
                (int state, out int value, out int next) =>
                {
                    if (state == 3)
                    {
                        throw failure;
                    }

                    value = state;
                    next = state + 1;

                    return true;
                })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Equal([1, 2], observed);
    }

    [Fact]
    public async Task ASourceWithNoElementsStillReleasesWhateverItObtained()
    {
        // Every source ends the same way, whichever of them it is: the run holds nothing afterwards.
        foreach (RunnableGraph graph in (RunnableGraph[])
        [
            Source.Empty<int>().To(Sink.Ignore<int>()),
            Source.Repeat(1, 0).To(Sink.Ignore<int>()),
            Source.Range(0, 0).To(Sink.Ignore<int>()),
        ])
        {
            await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
            await run.Completion;

            Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        }
    }
}
