using System.Threading.Channels;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What pausing a run does: when the pause has taken effect, what a paused run is holding, and what
/// happens when something else asks the run to stop while it is held.
/// </summary>
/// <remarks>
/// <para>
/// Nothing here waits on a clock. The claim "the pause has not taken effect yet" is asserted only where it
/// is a fact rather than a hope — a run inside an author's delegate, or with a callback still running, is
/// one no pause can be quiescent for — and the claim "nothing moved while the run was held" is asserted by
/// what the run did afterwards, where the alternative would have produced a different sequence.
/// </para>
/// <para>
/// Every test resumes or stops the run it paused. A run left paused is a run whose completion never
/// arrives, which is the one way a test of this feature can hang; every await here is on something a
/// released or stopped run reaches.
/// </para>
/// </remarks>
public sealed class PauseTests
{
    [Fact]
    public async Task PauseTakesEffectOnlyWhenNoElementIsInsideAStage()
    {
        Gate gate = new();
        RecordingEnumerable<int> elements = new(1, 2, 3, 4);
        RunnableGraph graph = Summing(elements, _ => gate.Wait(), out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        Task paused = run.PauseAsync(TestToken);

        // A fact and not a hope: the run is inside the author's fold with an element in its hands, and a
        // pause that reported quiescence there would be reporting something untrue.
        Assert.False(paused.IsCompleted);

        gate.Open();
        await paused;

        Assert.True(run.IsPaused);

        // The element the run was holding was finished, and no other was started: the park point is
        // between elements on both sides.
        Assert.Equal(1, elements.Pulls);

        await run.ResumeAsync();
        await run.Completion;

        Assert.Equal(10L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(4, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task ResumingContinuesFromExactlyWhereTheRunParked()
    {
        Gate gate = new();
        List<int> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5);

        RunnableGraph graph = Summing(
            elements,
            value =>
            {
                observed.Add(value);
                gate.Wait();
            },
            out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        Task paused = run.PauseAsync(TestToken);

        gate.Open();
        await paused;

        Assert.Equal([1], observed);

        await run.ResumeAsync();
        await run.Completion;

        // The whole sequence, once each, in order: a pause loses no element and repeats none.
        Assert.Equal([1, 2, 3, 4, 5], observed);
        Assert.Equal(15L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task PausingTwiceAwaitsOneQuiescenceAndOneResumeReleasesIt()
    {
        Gate gate = new();
        List<int> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3);

        RunnableGraph graph = Summing(
            elements,
            value =>
            {
                observed.Add(value);
                gate.Wait();
            },
            out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        Task first = run.PauseAsync(TestToken);
        Task second = run.PauseAsync(TestToken);

        // Both callers are waiting for one moment, and neither of them is waiting for a second pause to
        // take effect after the first: the run is held once, however many times it was asked.
        Assert.False(first.IsCompleted);
        Assert.False(second.IsCompleted);

        gate.Open();
        await Task.WhenAll(first, second);

        Assert.Equal([1], observed);

        // One resume and not two: a pause is a state and not a counter, so the run moves again after the
        // first of them.
        await run.ResumeAsync();
        await run.Completion;

        Assert.Equal([1, 2, 3], observed);
        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task ResumingARunThatWasNeverPausedChangesNothing()
    {
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await run.ResumeAsync();
        await run.Completion;
        await run.ResumeAsync();

        Assert.False(run.IsPaused);
        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task PausingARunThatHasAlreadyEndedCompletesAtOnceAndHoldsNothing()
    {
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // A run with no segment left to park is quiescent by definition, so this is not an error and not a
        // state the run has to be released from.
        await run.PauseAsync(TestToken);

        Assert.False(run.IsPaused);
        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task PausingRacesTheRunsOwnEndingWithoutEitherHanging()
    {
        // Neither order is arranged: the pause may take effect while the run is still moving or arrive
        // after the last segment has ended. Both are answered, and the run that is resumed afterwards
        // reaches the same result either way.
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await run.PauseAsync(TestToken);
        await run.ResumeAsync();
        await run.Completion;

        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task ShutdownOfAPausedRunWinsAndDrainsWhatWasBuffered()
    {
        Gate gate = new();
        List<int> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3);

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 8 })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        observed.Add(value);
                        gate.Wait();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // The source is finished and its two remaining elements are in the buffer; the sink is holding the
        // first one. What a shutdown has to drain is therefore a fact rather than a race.
        await gate.Reached;
        await elements.Released;

        Task paused = run.PauseAsync(TestToken);

        Assert.False(paused.IsCompleted);

        gate.Open();
        await paused;

        Assert.Equal([1], observed);

        await run.ShutdownAsync();

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 2, 3], observed);
        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
        Assert.False(run.IsPaused);
    }

    [Fact]
    public async Task DisposingAPausedRunWinsAndCancelsIt()
    {
        Gate gate = new();
        RecordingEnumerable<int> elements = new(1, 2, 3);
        RunnableGraph graph = Summing(elements, _ => gate.Wait(), out ResultSlot<long> total);

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        Task paused = run.PauseAsync(TestToken);

        gate.Open();
        await paused;

        // The parked segment observes the cancellation at its park point, which is the only reason this
        // returns at all: a pause that could outlive a stop would hang every teardown.
        await run.DisposeAsync();

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
        await Assert.ThrowsAsync<TaskCanceledException>(() => run.GetValueAsync(total, TestToken));
        Assert.False(run.IsPaused);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task CancellingTheMaterializationTokenReleasesAPausedRun()
    {
        // The path no request method takes: the caller's own token cancels the run without ShutdownAsync
        // or DisposeAsync being called at all, so the gate has to be opened by the token itself.
        using CancellationTokenSource cancellation = new();
        Gate gate = new();
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), _ => gate.Wait(), out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);
        await gate.Reached;

        Task paused = run.PauseAsync(TestToken);

        gate.Open();
        await paused;

        await cancellation.CancelAsync();
        await run.DisposeAsync();

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
    }

    [Fact]
    public async Task ARunWaitingForAnElementThatIsNotComingIsStillPausable()
    {
        // The source parks on a kernel wait and produces nothing at all. It reaches no park point of its
        // own and never will, so a pause takes effect only because a wait this runtime owns is accounted
        // for as a segment at rest.
        RunnableGraph graph = Source.Never<int>().To(s => s.Count(), "counted", out ResultSlot<long> counted);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // Paused twice with a resume between them, because the first of the two may well have arrived
        // before the segment reached its source at all and been answered by an ordinary park. Once
        // released, the segment goes into the wait and can never come back to a park point — the run's own
        // stop is the only thing that returns from there — so the second pause has nothing but the wait to
        // be quiescent about.
        await run.PauseAsync(TestToken);
        await run.ResumeAsync();
        await run.PauseAsync(TestToken);

        Assert.True(run.IsPaused);

        await run.ResumeAsync();
        await run.ShutdownAsync();

        Assert.Equal(0L, await run.GetValueAsync(counted, TestToken));
    }

    [Fact]
    public async Task ARunWaitingAtEveryKindOfItsOwnWaitIsStillPausable()
    {
        // Three segments and three different waits, all of them this runtime's own and none of them ever
        // going to complete on its own: the queue's reader has nothing to read, the asynchronous stage has
        // nothing to admit, and the segment below the buffer has nothing to take. A pause that only
        // counted segments standing at their park points would wait here forever.
        RunnableGraph graph = Source.Queue<int>(new BufferOptions { Capacity = 4 }, "ingress")
            .SelectAsync(new ParallelismOptions { MaxConcurrency = 2 }, (value, _) => Task.FromResult(value * 2))
            .Buffer(new BufferOptions { Capacity = 4 })
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        ResultSlot<IIngressQueue<int>> control = graph.Control<IIngressQueue<int>>("ingress");

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        await run.PauseAsync(TestToken);

        Assert.True(run.IsPaused);

        await run.ResumeAsync();

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(1, TestToken));
        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(2, TestToken));

        queue.Complete();

        await run.Completion;

        Assert.Equal([2, 4], await run.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public async Task ARunWaitingOnAnEmptyChannelIsStillPausable()
    {
        // The same claim for the other source that waits on something outside the run. Paused twice with a
        // resume between them for the reason the source that never produces is: the first pause may well
        // have been answered before the segment reached its channel at all.
        Channel<int> channel = Channel.CreateUnbounded<int>();

        RunnableGraph graph = Source.FromChannel(channel.Reader)
            .To(s => s.Collect(new CollectOptions { MaxElements = 4 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await run.PauseAsync(TestToken);
        await run.ResumeAsync();
        await run.PauseAsync(TestToken);

        Assert.True(run.IsPaused);

        await run.ResumeAsync();
        await channel.Writer.WriteAsync(1, TestToken);

        channel.Writer.Complete();

        await run.Completion;

        Assert.Equal([1], await run.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public async Task TheTokenOfAPauseStopsTheWaitAndNotTheRequest()
    {
        Gate gate = new();
        RecordingEnumerable<int> elements = new(1, 2, 3);
        RunnableGraph graph = Summing(elements, _ => gate.Wait(), out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        using CancellationTokenSource giveUp = new();

        Task paused = run.PauseAsync(giveUp.Token);

        await giveUp.CancelAsync();

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => paused);

        // The caller stopped waiting and the run is still being held: withdrawing a pause is what resuming
        // is for, and a token that could withdraw one would make "I stopped watching" mean "carry on".
        gate.Open();
        await run.PauseAsync(TestToken);

        Assert.True(run.IsPaused);
        Assert.Equal(1, elements.Pulls);

        await run.ResumeAsync();
        await run.Completion;

        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task ARunHeldAtAFullBufferIsStillPausable()
    {
        // The deadlock a pause could have introduced. The sink parks at its own park point and stops
        // taking, so the source's offer into a buffer of one can never complete; if a segment waiting for
        // room were not counted as at rest, this pause would never take effect and the test would hang
        // rather than fail.
        Gate gate = new();
        List<int> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5);
        TaskCompletionSource third = new(TaskCreationOptions.RunContinuationsAsynchronously);

        elements.Pulled = pulls =>
        {
            if (pulls == 3)
            {
                third.TrySetResult();
            }
        };

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 1 })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        observed.Add(value);
                        gate.Wait();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await gate.Reached;
        await third.Task;

        Task paused = run.PauseAsync(TestToken);

        gate.Open();
        await paused;

        // One element folded, one held by the buffer, one in the source's hands, and nothing else pulled:
        // total memory is the sum of the declared bounds however long the run is held here.
        Assert.Equal([1], observed);
        Assert.Equal(3, elements.Pulls);

        await run.ResumeAsync();
        await run.Completion;

        Assert.Equal([1, 2, 3, 4, 5], observed);
        Assert.Equal(15L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task ARunHeldAtAFullChannelSinkIsStillPausable()
    {
        // The mirror of the buffer case, on the far side of the graph, and a hole this suite did not have
        // until checkpoint 5 looked for one. A channel sink's write is this runtime's own wait on a channel
        // the author owns — the exact counterpart of the wait a channel *source* takes on an empty reader,
        // which has reported itself since it was written — and a wait that says nothing leaves a pause
        // waiting forever on a segment that will take no step until a consumer makes room. Before the fix
        // this test did not fail; it hung.
        Channel<int> channel = Channel.CreateBounded<int>(1);
        RecordingEnumerable<int> elements = new(1, 2, 3, 4);

        RunnableGraph graph = Source.From(elements).To(Sink.ToChannel(channel.Writer));

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // One element taken out of the channel and one written into it leaves the sink holding a third
        // with nowhere to put it, so the segment is inside the write and nothing but a reader can free it.
        Assert.Equal(1, await channel.Reader.ReadAsync(TestToken));

        await run.PauseAsync(TestToken);

        Assert.True(run.IsPaused);

        await run.ResumeAsync();

        // Everything the source had, once each and in order: the element held across the pause is the one
        // the run writes when it moves again.
        List<int> written = [];

        for (int element = 0; element < 3; element++)
        {
            written.Add(await channel.Reader.ReadAsync(TestToken));
        }

        await run.Completion;
        await run.DisposeAsync();

        Assert.Equal([2, 3, 4], written);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task PauseWaitsForTheCallbacksInFlightAndAdmitsNothingNew()
    {
        TaskCompletionSource<long>[] callbacks = Sources(4);
        TaskCompletionSource[] entered = Signals(4);
        List<long> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(0, 1, 2, 3))
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 2 },
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

        await Task.WhenAll(entered[0].Task, entered[1].Task);

        Task paused = run.PauseAsync(TestToken);

        // A callback in flight is an author's code executing, and a pause has not taken effect while one
        // is: however parked the segments around it are, the run is still doing something.
        Assert.False(paused.IsCompleted);

        callbacks[0].TrySetResult(100L);
        callbacks[1].TrySetResult(200L);

        await paused;

        // The two callbacks finished, and their results stayed in the window rather than travelling on;
        // nothing new was admitted behind them.
        Assert.Empty(observed);
        Assert.False(entered[2].Task.IsCompleted);
        Assert.False(entered[3].Task.IsCompleted);

        await run.ResumeAsync();

        await Task.WhenAll(entered[2].Task, entered[3].Task);

        callbacks[2].TrySetResult(300L);
        callbacks[3].TrySetResult(400L);

        await run.Completion;

        Assert.Equal([100L, 200L, 300L, 400L], observed);
        Assert.Equal(1000L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task APausedAsynchronousStageHoldsTheResultsItHasAlreadyComputed()
    {
        // The park point of an asynchronous segment, and the one shape that proves it is there. The segment
        // is inside the author's fold when the pause is asked for, so it has to come back to its loop to be
        // quiescent at all; a segment that carried on from there instead of parking would emit the result
        // waiting behind the one it just delivered and admit two more elements, and the pause would then be
        // waiting for callbacks the test has not released — which is a run that never comes to rest rather
        // than a wrong answer.
        Gate gate = new();
        TaskCompletionSource<long>[] callbacks = Sources(4);
        TaskCompletionSource[] entered = Signals(4);
        List<long> observed = [];

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(0, 1, 2, 3))
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 2 },
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
                        gate.Wait();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Task.WhenAll(entered[0].Task, entered[1].Task);

        callbacks[0].TrySetResult(100L);

        await gate.Reached;

        Task paused = run.PauseAsync(TestToken);

        Assert.False(paused.IsCompleted);

        // The second callback finishes while the run is being held. Its result is computed and waiting for
        // its turn, which is exactly what a paused run may hold and may not deliver.
        callbacks[1].TrySetResult(200L);

        gate.Open();
        await paused;

        Assert.Equal([100L], observed);
        Assert.False(entered[2].Task.IsCompleted);
        Assert.False(entered[3].Task.IsCompleted);

        await run.ResumeAsync();
        await Task.WhenAll(entered[2].Task, entered[3].Task);

        callbacks[2].TrySetResult(300L);
        callbacks[3].TrySetResult(400L);

        await run.Completion;

        Assert.Equal([100L, 200L, 300L, 400L], observed);
        Assert.Equal(1000L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task AQueueOfAPausedRunKeepsAnsweringByItsOwnPolicy()
    {
        // The queue stands upstream of the segment a pause parks, so pausing changes nothing about what an
        // offer answers: the policy the author declared is applied to the queue's own state, and a run
        // that is not reading is exactly a run whose queue fills up.
        Gate gate = new();
        List<int> observed = [];

        RunnableGraph graph = Source.Queue<int>(
                new BufferOptions { Capacity = 2, OverflowPolicy = OverflowPolicy.DropNewest },
                "ingress")
            .To(s => s.ForEach(value =>
            {
                observed.Add(value);
                gate.Wait();
            }));

        ResultSlot<IIngressQueue<int>> control = graph.Control<IIngressQueue<int>>("ingress");

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(1, TestToken));
        await gate.Reached;

        Task paused = run.PauseAsync(TestToken);

        gate.Open();
        await paused;

        // The segment is parked at the top of its loop and will not take another element, so the queue's
        // contents from here on are decided by the offers alone.
        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(2, TestToken));
        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(3, TestToken));
        Assert.Equal(QueueOfferOutcome.Dropped, await queue.OfferAsync(4, TestToken));

        await run.ResumeAsync();

        queue.Complete();

        await run.Completion;

        Assert.Equal([1, 2, 3], observed);
    }

    /// <summary>Creates the callback sources a test completes by hand, one per element.</summary>
    /// <param name="count">How many to create.</param>
    /// <returns>The sources.</returns>
    private static TaskCompletionSource<long>[] Sources(int count) =>
        [.. Enumerable.Range(0, count).Select(_ => new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously))];

    /// <summary>Creates the signals a test awaits, one per element.</summary>
    /// <param name="count">How many to create.</param>
    /// <returns>The signals.</returns>
    private static TaskCompletionSource[] Signals(int count) =>
        [.. Enumerable.Range(0, count).Select(_ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously))];
}
