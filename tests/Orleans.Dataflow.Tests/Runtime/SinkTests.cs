using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What each sink does with the elements that reach it, what it exposes afterwards, and what it does about
/// an empty stream and a failure.
/// </summary>
/// <remarks>
/// <para>
/// The two callback sinks are the interesting pair. The synchronous one is the sequential boundary — one
/// element at a time, in order, and a slow callback is backpressure rather than a queue — and the
/// asynchronous one is an asynchronous stage that emits nothing, so it has to keep every promise that
/// family makes: a bound on callbacks in flight, the run's own token, a completion that waits for the
/// callbacks it started, and a failure that cancels the ones beside it.
/// </para>
/// <para>
/// The result-bearing sinks are asserted through their slots, because that is the only way an author can
/// read them, and the honest first-element sink is asserted for both a reference and a value element type:
/// the default value it resolves is one the authoring surface computed and handed over, and a runtime that
/// had lost it would fail on a value type and pass on a reference one.
/// </para>
/// </remarks>
public sealed class SinkTests
{
    [Fact]
    public async Task ForEachSeesEveryElementInOrder()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .To(Sink.ForEach<int>(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 2, 3], observed);
    }

    [Fact]
    public async Task ForEachIsFinishedWithOneElementBeforeTheNextIsPulled()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3, 4);
        Gate gate = new();

        RunnableGraph graph = Source.From(elements)
            .To(s => s.ForEach(_ =>
            {
                gate.Wait();
                elements.Consumed();
            }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        Assert.Equal(1, elements.Pulls);

        gate.Open();
        await run.Completion;

        Assert.Equal(1, elements.PeakInFlight);
        Assert.Equal(4, elements.Pulls);
    }

    [Fact]
    public async Task AForEachCallbackThatThrowsFaultsTheRunWithTheExceptionItThrew()
    {
        InvalidOperationException failure = new("the callback refuses the second element");
        RecordingEnumerable<int> elements = new(1, 2, 3);
        List<int> observed = [];

        RunnableGraph graph = Source.From(elements)
            .To(Sink.ForEach<int>(value =>
            {
                if (value == 2)
                {
                    throw failure;
                }

                observed.Add(value);
            }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Equal([1], observed);
        Assert.Equal(2, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task AFoldWhoseCheckedArithmeticOverflowsFaultsTheRunWithTheAuthorsOwnException()
    {
        // What a fold does about overflow is the author's arithmetic and nothing this runtime adds. A folder
        // written with `checked` raises at the element that overflows, and the run reports that very
        // instance: a runtime that caught and rewrapped it would make every author's catch block a guess,
        // and one that folded through its own unchecked accumulator would silently decide numeric semantics
        // an author had already spelled out.
        RecordingEnumerable<int> elements = new(1, 2, 3);
        OverflowException? raised = null;

        RunnableGraph graph = Source.From(elements)
            .To(
                s => s.Aggregate(
                    long.MaxValue - 1L,
                    (sum, value) =>
                    {
                        try
                        {
                            return checked(sum + value);
                        }
                        catch (OverflowException failure)
                        {
                            // Captured on its way out rather than constructed here: the instance this test
                            // compares against has to be the one the CLR raised for the author's own
                            // expression, or the comparison proves nothing.
                            raised = failure;

                            throw;
                        }
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        OverflowException faulted = await Assert.ThrowsAsync<OverflowException>(() => run.Completion);

        Assert.Same(raised, faulted);
        Assert.Same(faulted, await Assert.ThrowsAsync<OverflowException>(() => run.GetValueAsync(total, TestToken)));

        // The seed plus one fits and the second element is the one that does not, so the run stopped at the
        // element whose arithmetic failed rather than at the end of the sequence.
        Assert.Equal(2, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task ForEachAsyncRunsUpToItsBoundOfCallbacksAtOnceAndNoMore()
    {
        TaskCompletionSource[] entered =
            [.. Enumerable.Range(0, 5).Select(_ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously))];
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int concurrent = 0;
        int peak = 0;

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(0, 1, 2, 3, 4))
            .To(s => s.ForEachAsync(
                new ParallelismOptions { MaxConcurrency = 2 },
                async (value, token) =>
                {
                    Observe(ref concurrent, ref peak, 1);
                    entered[value].TrySetResult();

                    try
                    {
                        await release.Task.WaitAsync(token);
                    }
                    finally
                    {
                        Observe(ref concurrent, ref peak, -1);
                    }
                }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Task.WhenAll(entered[0].Task, entered[1].Task);

        // Two are running and the third cannot start until one of them finishes.
        Assert.False(entered[2].Task.IsCompleted);

        release.TrySetResult();
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(2, Volatile.Read(ref peak));
        Assert.All(entered, signal => Assert.True(signal.Task.IsCompletedSuccessfully));
    }

    [Fact]
    public async Task ForEachAsyncCompletesOnlyOnceEveryCallbackItStartedHas()
    {
        // The promise a sink that emits nothing still has to keep: the run is not over while work it
        // started is still running, or an author who awaited completion would read a half-written result.
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int finished = 0;

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .To(s => s.ForEachAsync(
                new ParallelismOptions { MaxConcurrency = 3 },
                async (_, _) =>
                {
                    entered.TrySetResult();

                    await release.Task;

                    Interlocked.Increment(ref finished);
                }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await entered.Task;

        Assert.False(run.Completion.IsCompleted);

        release.TrySetResult();
        await run.Completion;

        Assert.Equal(3, Volatile.Read(ref finished));
    }

    [Fact]
    public async Task AFailingForEachAsyncCallbackFaultsTheRunAndCancelsTheOnesBesideIt()
    {
        InvalidOperationException failure = new("the callback refuses the first element");
        TaskCompletionSource neighbour = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource observedCancellation = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource parked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> started = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(0, 1, 2, 3))
            .To(s => s.ForEachAsync(
                new ParallelismOptions { MaxConcurrency = 2 },
                async (value, token) =>
                {
                    lock (started)
                    {
                        started.Add(value);
                    }

                    if (value == 0)
                    {
                        await neighbour.Task;

                        throw failure;
                    }

                    neighbour.TrySetResult();

                    try
                    {
                        await parked.Task.WaitAsync(token);
                    }
                    catch (OperationCanceledException)
                    {
                        observedCancellation.TrySetResult();

                        throw;
                    }
                }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));

        await observedCancellation.Task;

        lock (started)
        {
            Assert.Equal([0, 1], started);
        }
    }

    [Fact]
    public async Task ShutdownOfACallbackSinkDeliversWhatWasAdmittedAndAwaitsWhatWasStarted()
    {
        // The terminal discipline of checkpoint 2 restated for a sink that emits nothing: a graceful stop
        // is a drain, so the callback in flight is awaited and the run reports success.
        Gate held = new();
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int finished = 0;
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8);

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 2 })
            .To(s => s.ForEachAsync(
                new ParallelismOptions { MaxConcurrency = 1 },
                (_, _) =>
                {
                    entered.TrySetResult();
                    held.Wait();
                    Interlocked.Increment(ref finished);

                    return Task.CompletedTask;
                }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await entered.Task;

        Task shutdown = run.ShutdownAsync().AsTask();

        held.Open();
        await shutdown;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.InRange(Volatile.Read(ref finished), 1, elements.Pulls);
    }

    [Fact]
    public async Task FirstResolvesTheFirstElementAndEndsTheRunThere()
    {
        RecordingEnumerable<int> elements = new([.. Enumerable.Range(1, 50)]);

        RunnableGraph graph = Source.From(elements)
            .To(s => s.First(), "head", out ResultSlot<int> head);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(1, await run.GetValueAsync(head, TestToken));

        // Early completion, exactly as a Take(1) in its place would be.
        Assert.Equal(1, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task FirstFaultsOnAnEmptyStreamAndSaysWhichResultHasNoValue()
    {
        RunnableGraph graph = Source.Empty<int>().To(s => s.First(), "head", out ResultSlot<int> head);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException failure =
            await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

        // The wording the base class library uses for the same question, plus the name of the result the
        // author asked for.
        Assert.Contains("Sequence contains no elements", failure.Message, StringComparison.Ordinal);
        Assert.Contains("'head'", failure.Message, StringComparison.Ordinal);
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(head, TestToken)));
    }

    [Fact]
    public async Task FirstFaultsWhenEveryElementWasFilteredOutBeforeReachingIt()
    {
        // Emptiness is what the sink saw and not what the source held, so a stream every operator emptied
        // is as empty as one that never had anything.
        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Where(value => value > 10)
            .To(s => s.First(), "head", out ResultSlot<int> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException failure =
            await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

        Assert.Contains("Sequence contains no elements", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FirstAcceptsAnElementThatIsNullRatherThanCallingTheStreamEmpty()
    {
        // The case that separates "saw no element" from "saw the default value": a stream of one null is
        // not an empty stream, and a sink that inferred emptiness from its own state would fail on it.
        RunnableGraph graph = Source.Single<string?>(null)
            .To(s => s.First(), "head", out ResultSlot<string?> head);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Null(await run.GetValueAsync(head, TestToken));
    }

    [Fact]
    public async Task FirstOrDefaultResolvesTheFirstElementWhenThereIsOne()
    {
        RunnableGraph graph = Source.From(new RecordingEnumerable<string>("head", "tail"))
            .To(s => s.FirstOrDefault(), "head", out ResultSlot<string?> head);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal("head", await run.GetValueAsync(head, TestToken));
    }

    [Fact]
    public async Task FirstOrDefaultResolvesTheReferenceTypesDefaultOnAnEmptyStream()
    {
        RunnableGraph graph = Source.Empty<string>()
            .To(s => s.FirstOrDefault(), "head", out ResultSlot<string?> head);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Null(await run.GetValueAsync(head, TestToken));
    }

    [Fact]
    public async Task FirstOrDefaultResolvesTheValueTypesDefaultOnAnEmptyStream()
    {
        // The half a reference type could not have caught: the value the sink resolves is a boxed
        // default(T) the authoring surface computed, and a runtime that resolved nothing would fail the
        // cast rather than answer zero.
        RunnableGraph graph = Source.Empty<int>()
            .To(s => s.FirstOrDefault(), "head", out ResultSlot<int> head);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(0, await run.GetValueAsync(head, TestToken));
    }

    [Fact]
    public async Task CountResolvesTheNumberOfElementsThatReachedIt()
    {
        RunnableGraph graph = Source.Range(1, 7)
            .Where(value => value % 2 == 1)
            .To(s => s.Count(), "counted", out ResultSlot<long> counted);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(4L, await run.GetValueAsync(counted, TestToken));
    }

    [Fact]
    public async Task CountResolvesZeroForAnEmptyStreamAndStartsFromZeroInEveryRun()
    {
        RunnableGraph empty = Source.Empty<int>().To(s => s.Count(), "counted", out ResultSlot<long> none);

        await using (RunHandle run = await Host.MaterializeAsync(empty, TestToken))
        {
            await run.Completion;

            Assert.Equal(0L, await run.GetValueAsync(none, TestToken));
        }

        RunnableGraph three = Source.Range(0, 3).To(s => s.Count(), "counted", out ResultSlot<long> counted);

        await using (RunHandle first = await Host.MaterializeAsync(three, TestToken))
        {
            await first.Completion;

            Assert.Equal(3L, await first.GetValueAsync(counted, TestToken));
        }

        await using (RunHandle second = await Host.MaterializeAsync(three, TestToken))
        {
            await second.Completion;

            Assert.Equal(3L, await second.GetValueAsync(counted, TestToken));
        }
    }

    [Fact]
    public async Task TheNamedFactoryBuildsTheSameSinksAsTheLambdaOne()
    {
        // Sink.For<T>() and the factory a To lambda receives are one instance, so a sink built either way
        // is the same sink and closes the same document.
        RunnableGraph named = Source.Range(1, 4).To(Sink.For<int>().Count(), "counted", out ResultSlot<long> counted);
        RunnableGraph lambda = Source.Range(1, 4).To(s => s.Count(), "counted", out ResultSlot<long> _);

        Assert.Equal(lambda.Fingerprint, named.Fingerprint);

        await using RunHandle run = await Host.MaterializeAsync(named, TestToken);
        await run.Completion;

        Assert.Equal(4L, await run.GetValueAsync(counted, TestToken));
    }

    [Fact]
    public async Task AResultBearingSinkWhoseResultIsDiscardedStillRunsTheGraph()
    {
        // The conversion keeps the sink and drops the declaration, so the run still does the work and
        // simply exposes nothing to ask for. A first-element sink still ends the run early.
        RecordingEnumerable<int> elements = new([.. Enumerable.Range(1, 20)]);

        RunnableGraph graph = Source.From(elements).To(Sink.First<int>().ToSink());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Empty(graph.ResultSlots);
        Assert.Equal(1, elements.Pulls);
    }

    [Fact]
    public async Task ADiscardedFirstStillFaultsOnAnEmptyStreamAndSaysSoWithoutASlotName()
    {
        RunnableGraph graph = Source.Empty<int>().To(Sink.First<int>().ToSink());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException failure =
            await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

        Assert.Contains("Sequence contains no elements", failure.Message, StringComparison.Ordinal);
        Assert.Contains("this run's result", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACancelledRunOfAFirstSinkCancelsRatherThanReportingAnEmptyStream()
    {
        // Cancellation is examined before emptiness, because a run that was stopped never reached the end
        // of its stream and has nothing to say about how many elements it had. The token is already
        // cancelled at materialization, which is the one shape where "the sink saw no element" is a fact
        // rather than a race.
        using CancellationTokenSource cancellation = new();
        await cancellation.CancelAsync();

        RecordingEnumerable<int> elements = new(1, 2, 3);
        RunnableGraph graph = Source.From(elements).To(s => s.First(), "head", out ResultSlot<int> head);

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.GetValueAsync(head, TestToken));
        Assert.Equal(0, elements.Pulls);
    }

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
}
