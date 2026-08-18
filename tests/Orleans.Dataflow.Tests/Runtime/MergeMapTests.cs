using System.Globalization;
using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What bounded-parallel flattening promises: which elements come out and in what order, how many inner
/// sequences are open at once, what is released and when, and where a stop lands.
/// </summary>
/// <remarks>
/// <para>
/// The two order claims are the ones to read first, and both are asserted rather than described.
/// <b>Emission is unordered across inner sequences</b>: with one inner sequence held and another free, every
/// element of the free one comes out before the held one's first, which is an ordering no concat-map could
/// produce. <b>The order of each inner sequence is preserved</b>: each inner sequence's elements arrive in
/// its own order however they are interleaved with everyone else's, which a pump that asked for a second
/// element before delivering the first could not promise.
/// </para>
/// <para>
/// Every hold here is a gate or a rendezvous and never a delay, so "the pump has not done that yet" is a
/// fact rather than a hope. The bound is read the way every bound in this suite is read — as how far a held
/// source got — and every claim about disposal comes from a sequence that counts its own disposals and
/// completes a task only once its <c>DisposeAsync</c> has <i>returned</i>.
/// </para>
/// </remarks>
public sealed class MergeMapTests
{
    [Fact]
    public async Task MergeMapFlattensEveryInnerSequence()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 2 }, value => Counting(value))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The multiset and not the sequence, because the order across inner sequences is exactly what this
        // operator does not promise. Nothing is lost and nothing is duplicated.
        Assert.Equal([1, 2, 2, 3, 3, 3], [.. observed.Order()]);
    }

    [Fact]
    public async Task EmissionIsUnorderedAcrossInnerSequencesAndInOrderWithinEachOfThem()
    {
        Gate held = new();

        RunnableGraph graph = Source.From(["a", "b"])
            .MergeMap(
                new ParallelismOptions { MaxConcurrency = 2 },
                name => Labelled(name, name == "a" ? held : null))
            .To(TestSink.Probe<string>("out"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISinkProbe<string> sink = await run.GetValueAsync(graph.Control<ISinkProbe<string>>("out"), TestToken);

        // Three elements of the second inner sequence delivered while the first has produced nothing at all.
        // A concat-map cannot produce this ordering: it would have read 'a' to its end first.
        Assert.Equal("b1", await sink.ReceiveAsync(TestToken));
        Assert.Equal("b2", await sink.ReceiveAsync(TestToken));
        Assert.Equal("b3", await sink.ReceiveAsync(TestToken));

        held.Open();

        // And the held sequence's own three arrive in its own order once it is released, which is the other
        // half of the sentence: interleaving across sequences never reorders one.
        Assert.Equal("a1", await sink.ReceiveAsync(TestToken));
        Assert.Equal("a2", await sink.ReceiveAsync(TestToken));
        Assert.Equal("a3", await sink.ReceiveAsync(TestToken));
        await sink.ExpectCompletedAsync(TestToken);

        await run.Completion;
    }

    [Fact]
    public async Task AMergeMapNeverReadsMoreThanOneElementAheadPerOpenSequence()
    {
        RecordingAsyncEnumerable<int> inner = new(1, 2, 3, 4, 5);

        RunnableGraph graph = Source.From([0])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 4 }, _ => inner)
            .To(TestSink.Probe<int>("out"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISinkProbe<int> sink = await run.GetValueAsync(graph.Control<ISinkProbe<int>>("out"), TestToken);

        for (int received = 1; received <= 3; received++)
        {
            Assert.Equal(received, await sink.ReceiveAsync(TestToken));

            // The demand meter of a merge-map. An open sequence is asked for its next element only once the
            // one before it has been delivered, so nothing is ever collected and what an open sequence holds
            // is one element.
            Assert.True(inner.Pulls <= received + 1, $"the inner sequence was read {inner.Pulls} times");
        }

        await run.DisposeAsync();
    }

    [Fact]
    public async Task TheBoundIsOnOpenSequencesAndASlotIsFreedOnlyWhenOneEnds()
    {
        Gate held = new();
        TaskCompletionSource both = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int opened = 0;

        RunnableGraph graph = TestSource.Probe<int>("emitted")
            .MergeMap(
                new ParallelismOptions { MaxConcurrency = 2 },
                value =>
                {
                    if (Interlocked.Increment(ref opened) == 2)
                    {
                        both.TrySetResult();
                    }

                    return Labelled(value.ToString(CultureInfo.InvariantCulture), held);
                })
            .To(s => s.Ignore());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISourceProbe<int> source = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);

        for (int element = 1; element <= 4; element++)
        {
            await source.EmitAsync(element, TestToken);
        }

        await Reaches(both.Task, "two sequences are open");

        // Four elements absorbed with every sequence held at its first step: two of them are open sequences,
        // one is in the handoff channel in front of the pump, and one is in the source segment's hand. An
        // emit completes when the run has taken the element, so this is the run's own accounting and not a
        // guess. A third sequence cannot open while these two are held, because a slot is freed by an ending
        // and nothing here ends, so the reading is stable rather than a moment caught in passing.
        Assert.Equal(2, Volatile.Read(ref opened));
        Assert.True(source.PullsObserved <= 5L, $"the run pulled {source.PullsObserved} times");

        source.Complete();
        held.Open();

        await run.Completion;

        // A slot is freed when a sequence ends, and every element of the source eventually gets one.
        Assert.Equal(4, Volatile.Read(ref opened));
    }

    [Fact]
    public async Task AStreamThatIsOverBeforeItBeganOpensNoSequenceAtAll()
    {
        int opened = 0;

        RunnableGraph graph = Source.From([1, 2])
            .MergeMap(
                new ParallelismOptions { MaxConcurrency = 2 },
                value =>
                {
                    _ = Interlocked.Increment(ref opened);

                    return Counting(value);
                })
            .Take(0)
            .To(s => s.Count(), "counted", out ResultSlot<long> counted);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // A take of no elements resolves at plan time: the segment is completed before anything starts, so
        // the pump observes it at its first look and the author's function is never called.
        Assert.Equal(0L, await run.GetValueAsync(counted, TestToken));
        Assert.Equal(0, Volatile.Read(ref opened));
    }

    [Fact]
    public async Task AnEmptyInnerSequenceFreesItsSlotAtOnce()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.Range(1, 6)
            .MergeMap(
                new ParallelismOptions { MaxConcurrency = 1 },
                value => value % 2 == 0 ? Counting(1, value) : Counting(0, value))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Filtering is a special case of merging exactly as it is of flattening: an element whose sequence
        // is empty drops, and the slot it held is free again on that sequence's first step.
        Assert.Equal([2, 4, 6], observed);
    }

    [Fact]
    public async Task AMergeMapOfOneOpenSequenceIsAConcatMapThatCostsASegment()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 1 }, value => Counting(value))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // With one slot there is nothing to interleave with, so the result is the order of the input — the
        // answer SelectMany gives, reached by a pump that reads one sequence at a time because that is all
        // its bound allows.
        Assert.Equal([1, 2, 2, 3, 3, 3], observed);
    }

    [Fact]
    public async Task TheOrdinarySequenceSpellingIsTheSameOperator()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 3 }, value => Enumerable.Repeat(value, value))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 2, 3, 3, 3], [.. observed.Order()]);
    }

    [Fact]
    public async Task AFunctionAnsweringNullFailsTheRun()
    {
        RunnableGraph graph = Source.From([1])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 2 }, _ => (IAsyncEnumerable<int>)null!)
            .To(s => s.ForEach(_ => { }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException refused =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion);

        Assert.Contains("empty sequence", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AFailingFunctionFailsTheRunAndReleasesTheSequencesAlreadyOpen()
    {
        InvalidOperationException failure = new("no sequence for this one");
        RecordingAsyncEnumerable<int> first = new(1, 2, 3) { PullBarrier = _ => Never() };

        RunnableGraph graph = Source.From([1, 2])
            .MergeMap<int>(
                new ParallelismOptions { MaxConcurrency = 2 },
                value => value == 1 ? first : throw failure)
            .To(s => s.ForEach(_ => { }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion));

        // The sequence opened for the element before the failing one is released, and released means its own
        // asynchronous disposal returned rather than was started.
        await Reaches(first.DisposalCompleted, "the sequence opened before the failure is released");
        Assert.Equal(1, first.CompletedDisposals);
    }

    [Fact]
    public async Task AFailingInnerSequenceFailsTheRunAndReleasesTheOthers()
    {
        InvalidOperationException failure = new("the inner sequence broke");
        RecordingAsyncEnumerable<int> sound = new(1, 2, 3) { PullBarrier = _ => Never() };
        RecordingAsyncEnumerable<int> broken =
            new(1, 2, 3) { PullFailure = position => position == 1 ? failure : null };

        RunnableGraph graph = Source.From([1, 2])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 2 }, value => value == 1 ? sound : broken)
            .To(s => s.ForEach(_ => { }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion));

        // The other sequence was in the middle of a step it would never finish on its own; the run's own
        // token is what released it, and its release is what the disposal waited for.
        await Reaches(sound.DisposalCompleted, "the sibling sequence is released");
        Assert.Equal(1, sound.CompletedDisposals);
    }

    [Fact]
    public async Task AnInnerFailureReachesAPumpParkedForRoomBelowIt()
    {
        InvalidOperationException failure = new("the inner sequence broke");
        TaskCompletionSource broke = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingAsyncEnumerable<int> talkative = new(1, 2, 3, 4, 5);
        RecordingAsyncEnumerable<int> broken = new(9) { PullBarrier = _ => broke.Task };

        RunnableGraph graph = Source.From([1, 2])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 2 }, value => value == 1 ? talkative : broken)
            .To(TestSink.Probe<int>("out"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISinkProbe<int> sink = await run.GetValueAsync(graph.Control<ISinkProbe<int>>("out"), TestToken);

        // One element taken and the next one waiting at the rendezvous with nobody receiving: the pump is
        // asleep in a wait that has nothing to do with its window. A failure observed only when the pump next
        // examined that window would never be observed at all, and the run would wait for room forever.
        Assert.Equal(1, await sink.ReceiveAsync(TestToken));

        broke.TrySetException(failure);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion));
    }

    [Fact]
    public async Task EveryOpenSequenceIsReleasedWhenSomethingBelowEndsTheStream()
    {
        RecordingAsyncEnumerable<int> first = new(1, 2, 3, 4, 5, 6, 7, 8);
        RecordingAsyncEnumerable<int> second = new(1, 2, 3, 4, 5, 6, 7, 8);

        RunnableGraph graph = Source.From([1, 2])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 2 }, value => value == 1 ? first : second)
            .Take(4)
            .To(s => s.Ignore());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // A take ending the stream is a success, and the sequences the merge-map still had open are released
        // rather than drained: there is nowhere left to deliver, and an endless inner sequence must not
        // outlive the stream it was feeding.
        await Reaches(first.DisposalCompleted, "the first sequence is released");
        await Reaches(second.DisposalCompleted, "the second sequence is released");
        Assert.Equal(1, first.CompletedDisposals);
        Assert.Equal(1, second.CompletedDisposals);
    }

    [Fact]
    public async Task ACancelledRunAbandonsTheOpenSequencesAndReleasesThem()
    {
        using CancellationTokenSource cancellation = new();
        RecordingAsyncEnumerable<int> first = new(1, 2, 3, 4, 5, 6, 7, 8);
        RecordingAsyncEnumerable<int> second = new(1, 2, 3, 4, 5, 6, 7, 8);
        int observed = 0;

        RunnableGraph graph = Source.From([1, 2])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 2 }, value => value == 1 ? first : second)
            .To(s => s.ForEach(_ =>
            {
                if (Interlocked.Increment(ref observed) == 3)
                {
                    cancellation.Cancel();
                }
            }));

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run.Completion);

        await Reaches(first.DisposalCompleted, "the first sequence is released");
        await Reaches(second.DisposalCompleted, "the second sequence is released");
        Assert.True(Volatile.Read(ref observed) < 16, $"the run delivered {observed} elements after cancelling");
    }

    [Fact]
    public async Task DisposingARunMidFlightReturnsAndReleasesEveryOpenSequence()
    {
        TaskCompletionSource opened = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource reopened = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingAsyncEnumerable<int> first = new(1, 2, 3) { PullBarrier = Holding(opened) };
        RecordingAsyncEnumerable<int> second = new(1, 2, 3) { PullBarrier = Holding(reopened) };

        RunnableGraph graph = Source.From([1, 2])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 2 }, value => value == 1 ? first : second)
            .To(s => s.Ignore());

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(opened.Task, "the first sequence reaches its endless step");
        await Reaches(reopened.Task, "the second sequence reaches its endless step");

        // Both sequences are asleep in a step nothing but the run's own token will ever end, which is the
        // state a pump that could not be woken would hang in. That the disposal returns at all is the claim.
        await Reaches(run.DisposeAsync().AsTask(), "the disposal returns with both sequences mid-step");
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run.Completion);
        Assert.Equal(1, first.CompletedDisposals);
        Assert.Equal(1, second.CompletedDisposals);
    }

    [Fact]
    public async Task AShutdownPlaysTheOpenSequencesOutToTheirNaturalEnd()
    {
        Gate held = new();
        List<string> observed = [];
        int opened = 0;

        RunnableGraph graph = TestSource.Probe<string>("emitted")
            .MergeMap(
                new ParallelismOptions { MaxConcurrency = 2 },
                name =>
                {
                    _ = Interlocked.Increment(ref opened);

                    return Labelled(name, held);
                })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISourceProbe<string> source =
            await run.GetValueAsync(graph.Control<ISourceProbe<string>>("emitted"), TestToken);

        await source.EmitAsync("a", TestToken);
        await source.EmitAsync("b", TestToken);
        await Reaches(held.Reached, "an inner sequence is held at its first step");

        ValueTask shutdown = run.ShutdownAsync();

        held.Open();

        await Reaches(shutdown.AsTask(), "the shutdown returns once the open sequences have played out");
        await run.Completion;

        // Everything the sequences already open still had is delivered, which is what draining means for
        // work already admitted: a shutdown is not a cancellation, and the elements of an admitted sequence
        // were admitted.
        Assert.Equal(2, Volatile.Read(ref opened));
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(["a1", "a2", "a3"], [.. observed.Where(element => element.StartsWith('a'))]);
        Assert.Equal(["b1", "b2", "b3"], [.. observed.Where(element => element.StartsWith('b'))]);
    }

    [Fact]
    public async Task PausingAMergeMapMidFlightHoldsWhatEverySequenceHasProduced()
    {
        RunnableGraph graph = Source.From([1, 2])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 2 }, value => Counting(8, value * 100))
            .To(TestSink.Probe<int>("out"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISinkProbe<int> sink = await run.GetValueAsync(graph.Control<ISinkProbe<int>>("out"), TestToken);

        // One element taken, so both sequences are certainly open and the pump is placing rather than
        // starting: whatever it does next, it does with a window in flight.
        _ = await sink.ReceiveAsync(TestToken);

        // The double pause: the first may be answered by an ordinary park on the way to a wait, so the
        // second is the wait's own. Quiescence is therefore reached with the pump holding what its sequences
        // produced — an element at the rendezvous and one in the other sequence's slot — rather than with
        // the pump merely between two elements.
        await Reaches(run.PauseAsync(TestToken), "the merge-map reaches quiescence with its window in flight");
        await Reaches(run.ResumeAsync(), "the run moves again");
        await Reaches(run.PauseAsync(TestToken), "the merge-map reaches quiescence a second time");

        Assert.True(run.IsPaused);

        await Reaches(run.ResumeAsync(), "the run moves again");

        // Nothing was lost or duplicated across the hold: the other fifteen elements are still there.
        List<int> observed = [];

        for (int received = 2; received <= 16; received++)
        {
            observed.Add(await sink.ReceiveAsync(TestToken));
        }

        await sink.ExpectCompletedAsync(TestToken);
        await run.Completion;

        Assert.Equal(15, observed.Count);
    }

    [Fact]
    public async Task ABufferInFrontOfAMergeMapIsItsInputChannel()
    {
        Gate held = new();
        TaskCompletionSource both = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int opened = 0;

        RunnableGraph graph = TestSource.Probe<int>("emitted")
            .Buffer(new BufferOptions { Capacity = 4 })
            .MergeMap(
                new ParallelismOptions { MaxConcurrency = 2 },
                value =>
                {
                    if (Interlocked.Increment(ref opened) == 2)
                    {
                        both.TrySetResult();
                    }

                    return Labelled(value.ToString(CultureInfo.InvariantCulture), held);
                })
            .To(s => s.Ignore());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISourceProbe<int> source = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);

        for (int element = 1; element <= 7; element++)
        {
            await source.EmitAsync(element, TestToken);
        }

        await Reaches(both.Task, "two sequences are open");

        // Seven absorbed rather than four: two open sequences, the four the declared buffer holds, and one
        // in the source segment's hand. The buffer is the merge-map's own input channel rather than a second
        // one behind an implicit handoff, which is the rule every boundary of this vocabulary follows and
        // what keeps "total memory is the sum of the declared capacities" literally true.
        Assert.Equal(2, Volatile.Read(ref opened));

        source.Complete();
        held.Open();

        await run.Completion;

        Assert.Equal(7, Volatile.Read(ref opened));
    }

    [Fact]
    public async Task TwoMergeMapsInARowAreTwoPumps()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([2, 3])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 2 }, value => Counting(value))
            .MergeMap(new ParallelismOptions { MaxConcurrency = 3 }, value => Counting(2, value))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Each of the five inner elements becomes two, and each merge-map is the head of its own segment, so
        // the second one's window is filled by the first one's emissions.
        Assert.Equal(10, observed.Count);
        Assert.Equal([2, 2, 3, 3, 3, 3, 3, 4, 4, 4], [.. observed.Order()]);
    }

    [Fact]
    public async Task AMergeMapStandsBelowAJunctionLikeAnyOtherStage()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1])
            .Merge(Source.From([2]))
            .MergeMap(new ParallelismOptions { MaxConcurrency = 2 }, value => Counting(value))
            .To(s => s.ForEach(element =>
            {
                lock (observed)
                {
                    observed.Add(element);
                }
            }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // A merge-map below a fan-in reads the junction's own output channel: a pump is a pump wherever the
        // graph puts it, and a merge promises the multiset rather than the interleaving.
        Assert.Equal([1, 2, 2], [.. observed.Order()]);
    }

    [Fact]
    public async Task AMergeMapFeedsAJunctionLikeAnyOtherStage()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 2 }, value => Counting(value))
            .Merge(Source.From([9]))
            .To(s => s.ForEach(element =>
            {
                lock (observed)
                {
                    observed.Add(element);
                }
            }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The merge-map's segment is closed at the junction exactly as any other chain's is, so what the
        // junction joins is one of this pump's outputs and one ordinary source.
        Assert.Equal([1, 2, 2, 9], [.. observed.Order()]);
    }

    [Fact]
    public async Task AStageThatNeedsTheRunWorksInsideAMergeMapSegment()
    {
        RunnableGraph graph = Source.From([1, 2, 3])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 3 }, value => Counting(value))
            .ScanAsync(0L, async (sum, value, _) =>
            {
                await Task.Yield();

                return sum + value;
            })
            .To(s => s.AggregateAsync(0L, (last, state, _) => Task.FromResult(state)), "last", out ResultSlot<long> last);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // A stage that needs the run is attached per segment whatever kind of pump heads it, and an
        // asynchronous fold inside a merge-map's segment blocks that pump's thread exactly as a slow
        // synchronous stage would. The sum is order-independent, which is the only thing a merge-map lets a
        // fold below it assert.
        Assert.Equal(14L, await run.GetValueAsync(last, TestToken));
    }

    [Fact]
    public async Task AStageBelowAMergeMapIsAskedForItsResidueWhenTheWindowEmpties()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 3 }, value => Counting(value))
            .Grouped(4)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Six elements grouped by four: the merge-map's stream ends when its last sequence does, and the
        // residue walk runs on that end exactly as it does on a source running out.
        Assert.Equal(2, observed.Count);
        Assert.Equal(4, observed[0].Count);
        Assert.Equal(2, observed[1].Count);
    }

    [Fact]
    public async Task AFailingOrdinaryInnerSequenceFailsTheRunWithItsOwnException()
    {
        InvalidOperationException failure = new("the ordinary sequence broke");

        RunnableGraph graph = Source.From([1])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 2 }, _ => Refusing(failure))
            .To(s => s.ForEach(_ => { }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // An ordinary sequence throws from its own step rather than answering a faulted task, and the pump
        // is what is standing there when it does.
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion));
    }

    [Fact]
    public async Task AnInnerSequenceWhoseReleaseThrowsFaultsAnOtherwiseSuccessfulRun()
    {
        InvalidOperationException failure = new("the release broke");

        RunnableGraph graph = Source.From([1])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 2 }, _ => new HostileSequence(failure))
            .To(s => s.Ignore());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // The rule the head enumerator already follows, read on an inner one: a release that throws is
        // reported when nothing else went wrong, and never in place of an author's own failure.
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion));
    }

    [Fact]
    public async Task APauseTakesEffectOnceEveryOpenSequenceHasAnsweredAndHoldsWhatTheyProduced()
    {
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingAsyncEnumerable<int> inner = new(1, 2);
        int delivered = 0;

        inner.PullBarrier = position =>
        {
            if (position != 1)
            {
                return null;
            }

            entered.TrySetResult();

            return released.Task;
        };

        RunnableGraph graph = Source.From([0])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 1 }, _ => inner)
            .To(s => s.ForEach(_ => Interlocked.Increment(ref delivered)));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(entered.Task, "the sequence is inside its second step");

        Task quiet = run.PauseAsync(TestToken);

        released.TrySetResult();

        // The step is an author's iterator running, so quiescence is reached when it answers and not while
        // it is running — the accounting an asynchronous callback gets, applied to an outstanding step. And
        // the element that step produced is *held*: the pump parks before it emits, so a paused merge-map
        // holds what its sequences produced rather than delivering it.
        await Reaches(quiet, "the pause takes effect once the step has answered");

        Assert.Equal(1, Volatile.Read(ref delivered));

        await Reaches(run.ResumeAsync(), "the run moves again");
        await run.Completion;

        Assert.Equal(2, Volatile.Read(ref delivered));
    }

    [Fact]
    public async Task ResumeThenRepauseStormsNeitherDeadlockNorLoseAnElement()
    {
        RunnableGraph graph = Source.From([1, 2, 3])
            .MergeMap(new ParallelismOptions { MaxConcurrency = 3 }, value => Counting(8, value * 100))
            .To(TestSink.Probe<int>("out"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISinkProbe<int> sink = await run.GetValueAsync(graph.Control<ISinkProbe<int>>("out"), TestToken);
        List<int> observed = [];

        // Once per element, with three sequences genuinely in flight, so every cycle asks for quiescence
        // from a different point in the pump's pass: between two of its sequences, in its wait, at the
        // rendezvous below it. A pass cut short by one of these and resumed before it looked again is the
        // state that would leave the pump waiting on a sequence it had asked nothing of, so a hole here
        // hangs rather than fails — which is what the deadline in Reaches turns into a report.
        for (int received = 0; received < 24; received++)
        {
            observed.Add(await sink.ReceiveAsync(TestToken));

            await Reaches(run.PauseAsync(TestToken), $"the pause takes effect after element {received}");
            await Reaches(run.ResumeAsync(), $"the run moves again after element {received}");
        }

        await sink.ExpectCompletedAsync(TestToken);
        await run.Completion;

        // An exact multiset rather than a sequence, because a merge-map promises the multiset: every element
        // of every sequence delivered once, unchanged, across twenty-four holds.
        Assert.Equal(
            [.. Enumerable.Range(100, 8).Concat(Enumerable.Range(200, 8)).Concat(Enumerable.Range(300, 8))],
            [.. observed.Order()]);
    }

    [Fact]
    public async Task MergeMapComposesInsideAReusableFlow()
    {
        Flow<int, int> exploded = Flow.For<int>()
            .MergeMap(new ParallelismOptions { MaxConcurrency = 2 }, value => Counting(value));
        List<int> first = [];
        List<int> second = [];

        RunnableGraph one = Source.From([1, 2]).Via(exploded).To(s => s.ForEach(first.Add));
        RunnableGraph two = Source.From([3]).Via(exploded).To(s => s.ForEach(second.Add));

        await using (RunHandle run = await Host.MaterializeAsync(one, TestToken))
        {
            await run.Completion;
        }

        await using (RunHandle run = await Host.MaterializeAsync(two, TestToken))
        {
            await run.Completion;
        }

        Assert.Equal([1, 2, 2], [.. first.Order()]);
        Assert.Equal([3, 3, 3], second);
    }

    /// <summary>An asynchronous sequence of one value, repeated.</summary>
    /// <param name="count">How many elements it produces.</param>
    /// <param name="value">The first element it produces, or the count itself when it is negative.</param>
    /// <returns>The sequence.</returns>
    private static async IAsyncEnumerable<int> Counting(int count, int value = -1)
    {
        for (int index = 0; index < count; index++)
        {
            await Task.Yield();

            yield return value < 0 ? count : value + index;
        }
    }

    /// <summary>An asynchronous sequence of three labelled elements, held at a gate before the first.</summary>
    /// <param name="name">The label every element carries.</param>
    /// <param name="gate">The gate the sequence waits at before its first element, or <see langword="null"/>.</param>
    /// <returns>The sequence.</returns>
    /// <remarks>
    /// The gate is waited for on a thread of its own, because a gate blocks and the first step of an inner
    /// sequence runs on the pump's thread until its first suspension: holding that thread would hold the
    /// window this test is about rather than one sequence in it.
    /// </remarks>
    private static async IAsyncEnumerable<string> Labelled(string name, Gate? gate)
    {
        if (gate is not null)
        {
            await Task.Run(gate.Wait);
        }

        for (int index = 1; index <= 3; index++)
        {
            await Task.Yield();

            yield return name + index.ToString(CultureInfo.InvariantCulture);
        }
    }

    /// <summary>An ordinary sequence that fails on its very first step.</summary>
    /// <param name="failure">The exception it raises.</param>
    /// <returns>The sequence.</returns>
    private static IEnumerable<int> Refusing(Exception failure)
    {
        throw failure;

#pragma warning disable CS0162 // Unreachable code detected: an iterator needs a yield to be one at all.
        yield break;
#pragma warning restore CS0162
    }

    /// <summary>A task that never completes, for the steps only a run's own token can end.</summary>
    /// <returns>The task.</returns>
    private static Task Never() => new TaskCompletionSource().Task;

    /// <summary>A pull barrier that reports reaching the second element and then never returns from it.</summary>
    /// <param name="reached">The source to signal when the sequence asks for its second element.</param>
    /// <returns>The barrier.</returns>
    /// <remarks>
    /// The first element passes so that the sequence is certainly being read, and the second is where it
    /// stops: a test that disposed a run before its sequences were open would be measuring nothing.
    /// </remarks>
    private static Func<int, Task?> Holding(TaskCompletionSource reached) =>
        position =>
        {
            if (position != 1)
            {
                return null;
            }

            reached.TrySetResult();

            return Never();
        };

    /// <summary>An asynchronous sequence that produces one element and then refuses to be released.</summary>
    /// <param name="failure">The exception its disposal raises.</param>
    private sealed class HostileSequence(Exception failure) : IAsyncEnumerable<int>, IAsyncEnumerator<int>
    {
        private bool _produced;

        /// <inheritdoc/>
        public int Current => 1;

        /// <inheritdoc/>
        public IAsyncEnumerator<int> GetAsyncEnumerator(CancellationToken cancellationToken = default) => this;

        /// <inheritdoc/>
        public ValueTask<bool> MoveNextAsync()
        {
            bool first = !_produced;

            _produced = true;

            return ValueTask.FromResult(first);
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => ValueTask.FromException(failure);
    }
}
