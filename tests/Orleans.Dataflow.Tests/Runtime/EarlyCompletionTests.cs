using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The first completion a run has that does not come from its source: a stage that has taken what it was
/// asked for ends the stream where it stands.
/// </summary>
/// <remarks>
/// <para>
/// The claim is that it ends the run the way the source running out does, and every part of that is worth a
/// test of its own: the run reports success, the result resolves, the elements that already passed are
/// delivered, the elements upstream are abandoned without being counted as losses, and everything above the
/// stage stops and releases what it held.
/// </para>
/// <para>
/// The release is the part that could deadlock rather than merely misbehave, so it is proven with the same
/// shape checkpoint 2 used for a failing terminal: a source deliberately parked inside a full buffer's
/// offer, with the completion arriving while it is parked. A run that never released it would not fail this
/// test; it would hang, which is why the source is held until the test knows it is parked.
/// </para>
/// </remarks>
public sealed class EarlyCompletionTests
{
    [Fact]
    public async Task TakeEndsTheRunTheWayTheSourceRunningOutDoes()
    {
        RecordingEnumerable<int> elements = new([.. Enumerable.Range(1, 50)]);

        RunnableGraph graph = Source.From(elements)
            .Take(3)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));

        // The bound is reached on the third element and not on the fourth, so a source of fifty is asked
        // for three and released.
        Assert.Equal(3, elements.Pulls);
        Assert.Equal(1, elements.Releases);
        Assert.Equal(0L, run.DroppedElements);
    }

    [Fact]
    public async Task TakeReleasesASourceParkedInsideAFullBuffersOffer()
    {
        // The deadlock this shape invites: the take completes while the source is waiting for room in a
        // buffer nothing will ever read again. The completion has to reach it as the closing of that
        // buffer rather than as silence.
        Gate held = new();
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingEnumerable<int> elements = new([.. Enumerable.Range(1, 50)])
        {
            Pulled = pulls =>
            {
                if (pulls == 3)
                {
                    saturated.TrySetResult();
                }
            },
        };

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 1 })
            .Select(value =>
            {
                held.Wait();

                return value;
            })
            .Take(1)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // The downstream segment is held on the first element, the buffer holds the second, and the source
        // is parked offering the third.
        await held.Reached;
        await saturated.Task;

        Assert.Equal(3, elements.Pulls);

        held.Open();
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(1L, await run.GetValueAsync(total, TestToken));

        // The source was released where it stood: it was never asked for a fourth element, and the two it
        // had already handed over were abandoned rather than dropped.
        Assert.Equal(3, elements.Pulls);
        Assert.Equal(1, elements.Releases);
        Assert.Equal(0L, run.DroppedElements);
    }

    [Fact]
    public async Task TakeOfNoElementsCompletesTheRunWithoutTouchingTheSourceAtAll()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3);
        List<int> observed = [];

        RunnableGraph graph = Source.From(elements)
            .Take(0)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Empty(observed);

        // Not "pulled nothing" but "never enumerated": a stage that can never emit is known before the run
        // starts, and waiting for an element to discover it would stall on a source that never ends.
        Assert.Equal(0, elements.Enumerations);
        Assert.Equal(0, elements.Pulls);
    }

    [Fact]
    public async Task TakeOfNoElementsStillResolvesTheResultTheTerminalStartedFrom()
    {
        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Take(0)
            .To(s => s.Aggregate(41L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(41L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task TakeOfNoElementsCompletesARunThatHasBoundariesInIt()
    {
        // The completion before the first pull has to reach segments that had not started yet, through
        // channels nobody ever wrote to. A run that only stopped the segment holding the take would leave
        // the others waiting on an input that never arrives.
        RecordingEnumerable<int> elements = new(1, 2, 3);

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 2 })
            .Take(0)
            .Buffer(new BufferOptions { Capacity = 2 })
            .To(s => s.Aggregate(5L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(5L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(0, elements.Enumerations);
    }

    [Fact]
    public async Task TakeOfNoElementsCompletesARunWhoseSegmentIsAsynchronous()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3);
        int started = 0;

        RunnableGraph graph = Source.From(elements)
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 2 },
                (value, _) =>
                {
                    Interlocked.Increment(ref started);

                    return Task.FromResult((long)value);
                })
            .Take(0)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(0L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(0, elements.Enumerations);
        Assert.Equal(0, Volatile.Read(ref started));
    }

    [Fact]
    public async Task TakeOfNoElementsInFrontOfAFirstSinkIsAnEmptyStreamAndFaultsAsOne()
    {
        // The two rules meet: the stream ended without an element, and a strict first-element sink says
        // what that means rather than resolving something it never saw.
        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Take(0)
            .To(s => s.First(), "head", out ResultSlot<int> head);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException failure =
            await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

        Assert.Contains("Sequence contains no elements", failure.Message, StringComparison.Ordinal);
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(head, TestToken)));
    }

    [Fact]
    public async Task EveryRunOfATakeAcrossTwoBoundariesEndsTheSameWay()
    {
        // The completion path crosses two channels and an asynchronous window here, and every part of it
        // races the segments it is stopping. One run proves nothing about that; the repetition is the test.
        for (int race = 0; race < 200; race++)
        {
            RunnableGraph graph = Source.From(new RecordingEnumerable<int>([.. Enumerable.Range(1, 30)]))
                .Buffer(new BufferOptions { Capacity = 4 })
                .SelectAsync(
                    new ParallelismOptions { MaxConcurrency = 3 },
                    (value, _) => Task.FromResult((long)value))
                .Take(3)
                .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

            await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
            await run.Completion;

            Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
            Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
        }
    }

    [Fact]
    public async Task ATakeOfMoreElementsThanArriveIsNeverReached()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3);

        RunnableGraph graph = Source.From(elements)
            .Take(10)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(3, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task ATakeBelowTwoBufferedSegmentsStopsBothOfThem()
    {
        // Three segments above the take rather than one, so that the completion has to travel rather than
        // merely arrive: every segment between the source and the take has to stop and close the channel
        // above it in turn.
        RecordingEnumerable<int> elements = new([.. Enumerable.Range(1, 200)]);

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 2 })
            .Select(value => value * 2)
            .Buffer(new BufferOptions { Capacity = 2 })
            .Select(value => value + 1)
            .Take(2)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(3L + 5L, await run.GetValueAsync(total, TestToken));

        // The two segments above the take hold one element each and the two buffers hold two each, so a
        // run that stopped promptly has asked for far fewer than the two hundred available.
        Assert.InRange(elements.Pulls, 2, 12);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task ATakeAboveABufferStillDeliversWhatAlreadyPassedIt()
    {
        // The other direction: everything below the take drains, because those elements were admitted
        // before the stream ended and an early completion is not a cancellation.
        List<int> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3, 4, 5))
            .Take(3)
            .Buffer(new BufferOptions { Capacity = 8 })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 2, 3], observed);
    }

    [Fact]
    public async Task ATakeInsideAnAsynchronousSegmentAwaitsTheCallbacksItHadStarted()
    {
        // The question an early completion asks of an asynchronous stage: the callbacks in flight belong to
        // elements upstream of the take, whose results are abandoned. They are awaited rather than
        // cancelled, because the run is ending successfully and cancelling an author's callback to end a
        // successful run would report a cancellation nobody asked for.
        TaskCompletionSource<long>[] callbacks =
            [.. Enumerable.Range(0, 3).Select(_ => new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously))];
        TaskCompletionSource[] entered =
            [.. Enumerable.Range(0, 3).Select(_ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously))];
        int cancellations = 0;
        int finished = 0;
        List<long> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(0, 1, 2))
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 3 },
                async (value, token) =>
                {
                    entered[value].TrySetResult();

                    try
                    {
                        return await callbacks[value].Task.WaitAsync(token);
                    }
                    catch (OperationCanceledException)
                    {
                        Interlocked.Increment(ref cancellations);

                        throw;
                    }
                    finally
                    {
                        Interlocked.Increment(ref finished);
                    }
                })
            .Take(1)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Task.WhenAll(entered.Select(signal => signal.Task));

        // The first result completes the take; the other two are in flight and are the ones under test.
        callbacks[0].TrySetResult(10L);
        callbacks[1].TrySetResult(20L);
        callbacks[2].TrySetResult(30L);

        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([10L], observed);
        Assert.Equal(0, Volatile.Read(ref cancellations));
        Assert.Equal(3, Volatile.Read(ref finished));
    }

    [Fact]
    public async Task ATakeAfterAnAsynchronousBoundaryReleasesTheSourceAboveIt()
    {
        RecordingEnumerable<int> elements = new([.. Enumerable.Range(1, 100)]);

        RunnableGraph graph = Source.From(elements)
            .SelectAsync(new ParallelismOptions { MaxConcurrency = 1 }, (value, _) => Task.FromResult((long)value))
            .Take(2)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(3L, await run.GetValueAsync(total, TestToken));

        // A handful rather than a hundred. The exact number is a race between the source filling the
        // handoff channel and the asynchronous segment emptying it, and pinning it would be pinning the
        // scheduler; what is under test is that the source stopped instead of running to the end.
        Assert.InRange(elements.Pulls, 2, 20);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task ATakeInFrontOfAnAsynchronousStageLetsThatStageFinishWhatItStarted()
    {
        // The take is above the boundary this time, so the asynchronous segment is downstream of the
        // completion and drains: both admitted callbacks run to their end and both results are delivered.
        List<long> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5);

        RunnableGraph graph = Source.From(elements)
            .Take(2)
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 2 },
                (value, _) => Task.FromResult((long)value * 10L))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([10L, 20L], observed);
        Assert.Equal(2, elements.Pulls);
    }

    [Fact]
    public async Task AScanBoundedByATakeEmitsExactlyTheStatesItReached()
    {
        List<long> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6);

        RunnableGraph graph = Source.From(elements)
            .Scan(0L, (sum, value) => sum + value)
            .Take(3)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1L, 3L, 6L], observed);
        Assert.Equal(3, elements.Pulls);
    }

    [Fact]
    public async Task AnEndlessUnfoldIsBoundedByTakeAndByNothingElse()
    {
        // The only sanctioned way to end an unfold that never ends itself, and the reason a take completes
        // on the element that reaches its bound rather than on the one after it: waiting for one more
        // element from this source would never return.
        List<int> observed = [];

        RunnableGraph graph = Source.Unfold(
                1,
                (int state, out int value, out int next) =>
                {
                    value = state;
                    next = state + 1;

                    return true;
                })
            .Take(4)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 2, 3, 4], observed);
    }

    [Fact]
    public async Task AnEndlessRepeatOfOneElementIsBoundedByTakeThrough()
    {
        // The same claim for the inclusive spelling, over a source that is endless in a different way: the
        // stream ends at the element the predicate rejects and that element is delivered.
        List<long> observed = [];

        RunnableGraph graph = Source.Unfold(
                1L,
                (long state, out long value, out long next) =>
                {
                    value = state;
                    next = state * 2L;

                    return true;
                })
            .TakeThrough(value => value < 8L)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1L, 2L, 4L, 8L], observed);
    }

    [Fact]
    public async Task AnEndlessSourceEndedByTakeWhileStopsAtTheFirstRejection()
    {
        List<long> observed = [];

        RunnableGraph graph = Source.Unfold(
                1L,
                (long state, out long value, out long next) =>
                {
                    value = state;
                    next = state * 2L;

                    return true;
                })
            .TakeWhile(value => value < 8L)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1L, 2L, 4L], observed);
    }

    [Fact]
    public async Task TwoTakesInOneChainAreBothHonouredAndTheTighterOneWins()
    {
        List<int> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6);

        RunnableGraph graph = Source.From(elements)
            .Take(4)
            .Buffer(new BufferOptions { Capacity = 8 })
            .Take(2)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 2], observed);
        Assert.InRange(elements.Pulls, 2, 4);
    }

    [Fact]
    public async Task ACallbackThatFailsWhileItIsBeingDrainedStillFaultsTheRun()
    {
        // The other half of the drain rule, and the one worth being explicit about: the callbacks left
        // over after an early completion are awaited rather than cancelled, and if one of them fails the
        // run reports that failure. Failure wins over an ending nobody can see yet.
        InvalidOperationException failure = new("the abandoned callback refuses");
        TaskCompletionSource<long>[] callbacks =
            [.. Enumerable.Range(0, 3).Select(_ => new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously))];
        TaskCompletionSource[] entered =
            [.. Enumerable.Range(0, 3).Select(_ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously))];
        TaskCompletionSource emitted = new(TaskCreationOptions.RunContinuationsAsynchronously);

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(0, 1, 2))
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 3 },
                async (value, token) =>
                {
                    entered[value].TrySetResult();

                    return await callbacks[value].Task.WaitAsync(token);
                })
            .Take(1)
            .To(s => s.ForEach(_ => emitted.TrySetResult()));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Task.WhenAll(entered.Select(signal => signal.Task));

        // The first result ends the stream; the run is now waiting for the two callbacks it abandoned.
        callbacks[0].TrySetResult(10L);
        await emitted.Task;

        callbacks[1].TrySetException(failure);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
    }

    [Fact]
    public async Task ATakeAndAShutdownAskingForTheSameThingBothEndTheRunSuccessfully()
    {
        // Two clean endings racing each other. Which of them arrives first is not the point and is not
        // asserted; that the run ends once, successfully, and resolves a state it actually reached, is.
        RunnableGraph graph = Source.From(new RecordingEnumerable<int>([.. Enumerable.Range(1, 50)]))
            .Take(3)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.ShutdownAsync();

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.InRange(await run.GetValueAsync(total, TestToken), 0L, 6L);
    }

    [Fact]
    public async Task TakeOfNoElementsCompletesARunWhoseTerminalIsAnAsynchronousCallbackSink()
    {
        // The two mechanisms this checkpoint adds, met in one graph: a stream that is over before it
        // starts, and a sink that is the head of its own segment. The sink's segment has to end on an
        // input that closes without ever carrying anything.
        RecordingEnumerable<int> elements = new(1, 2, 3);
        int called = 0;

        RunnableGraph graph = Source.From(elements)
            .Take(0)
            .To(s => s.ForEachAsync(
                new ParallelismOptions { MaxConcurrency = 2 },
                (_, _) =>
                {
                    Interlocked.Increment(ref called);

                    return Task.CompletedTask;
                }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(0, elements.Enumerations);
        Assert.Equal(0, Volatile.Read(ref called));
    }

    [Fact]
    public async Task TwoRunsOfOneTakingGraphCompleteIndependentlyOfEachOther()
    {
        // Two runs at once over stateful stages: the counters, the remembered keys, and the completion
        // that ends each of them belong to one run and to no other.
        RunnableGraph graph = Source.Range(1, 100)
            .Distinct(new DistinctOptions { MaxTrackedKeys = 128 })
            .Take(4)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle first = await Host.MaterializeAsync(graph, TestToken);
        await using RunHandle second = await Host.MaterializeAsync(graph, TestToken);

        await Task.WhenAll(first.Completion, second.Completion);

        Assert.Equal(10L, await first.GetValueAsync(total, TestToken));
        Assert.Equal(10L, await second.GetValueAsync(total, TestToken));
    }

}
