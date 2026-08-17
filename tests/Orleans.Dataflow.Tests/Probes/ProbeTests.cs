using Orleans.Dataflow.Testing;
using Xunit;

namespace Orleans.Dataflow.Tests.Probes;

/// <summary>
/// What the demand-aware probes prove about a run: that the test cannot outrun it, that it delivers
/// nothing nobody asked for, and that no wait of a probe survives the run it belongs to.
/// </summary>
/// <remarks>
/// <para>
/// The probes exist to turn claims about demand into assertions, so the assertions here are about demand
/// and not about timing. "The run has not taken the next element" is a fact whenever the run is holding one
/// at a sink nobody has asked, because the only thing that could free it is a receive; "the run delivered
/// nothing" is a fact whenever every segment is parked. Nothing here waits on a clock, and every test ends
/// the run it started.
/// </para>
/// <para>
/// The bound worth reading is <c>PullsObserved &lt;= emitted + 1</c>. It holds for every graph in this file
/// whatever buffers stand in it, because a runtime that pulled a second element before it had done
/// anything with the first would exceed it — which is the difference between demand and optimism.
/// </para>
/// </remarks>
public sealed class ProbeTests
{
    /// <summary>Gets the host every probe test materializes through.</summary>
    private static LocalDataflowHost Host { get; } = new();

    /// <summary>Gets the running test's own cancellation token.</summary>
    private static CancellationToken TestToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AnEmitCompletesOnlyWhenTheRunHasTakenTheElement()
    {
        RunnableGraph graph = TestSource.Probe<int>("emitted").To(TestSink.Probe<int>("received"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        (ISourceProbe<int> source, ISinkProbe<int> sink) = await Probes(run, graph);

        await source.EmitAsync(1, TestToken);

        // The element is at the sink and nobody has asked for it, so the run cannot take another: this
        // emit stays outstanding until a receive releases the one before it.
        Task second = source.EmitAsync(2, TestToken).AsTask();

        Assert.False(second.IsCompleted);
        Assert.Equal(1L, source.PullsObserved);

        Assert.Equal(1, await sink.ReceiveAsync(TestToken));

        await second;

        Assert.Equal(2, await sink.ReceiveAsync(TestToken));

        source.Complete();

        await sink.ExpectCompletedAsync(TestToken);
        await run.Completion;

        // Two elements emitted and three pulls: the run was always asking for exactly one more than it had
        // been given, which is a credit of one and not a prefetch.
        Assert.Equal(3L, source.PullsObserved);
    }

    [Fact]
    public async Task TheRunReceivesNothingThatWasNotEmitted()
    {
        RunnableGraph graph = TestSource.Probe<int>("emitted").To(TestSink.Probe<int>("received"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        (ISourceProbe<int> source, ISinkProbe<int> sink) = await Probes(run, graph);

        Task<int> received = sink.ReceiveAsync(TestToken).AsTask();

        Assert.False(received.IsCompleted);

        await source.EmitAsync(7, TestToken);

        Assert.Equal(7, await received);

        source.Complete();
        await sink.ExpectCompletedAsync(TestToken);
    }

    [Fact]
    public async Task WithNothingReceivedTheRunParksAfterExactlyOneElement()
    {
        // No buffer anywhere, so the declared bound is the credit of one: one element reaches the sink, one
        // more emit is outstanding, and the source is asked for nothing else at all.
        RunnableGraph graph = TestSource.Probe<int>("emitted").To(TestSink.Probe<int>("received"));

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        (ISourceProbe<int> source, ISinkProbe<int> _) = await Probes(run, graph);

        await source.EmitAsync(1, TestToken);

        Task second = source.EmitAsync(2, TestToken).AsTask();

        Assert.False(second.IsCompleted);
        Assert.Equal(1L, source.PullsObserved);

        await run.DisposeAsync();

        // An emit the run can no longer answer fails rather than waiting for a run that has ended.
        _ = await Assert.ThrowsAsync<ProbeTerminatedException>(async () => await second);
    }

    [Fact]
    public async Task ABufferRaisesTheBoundByTheCapacityItDeclares()
    {
        RunnableGraph graph = TestSource.Probe<int>("emitted")
            .Buffer(new BufferOptions { Capacity = 2 })
            .To(TestSink.Probe<int>("received"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        (ISourceProbe<int> source, ISinkProbe<int> sink) = await Probes(run, graph);

        // Three elements travel with nothing received: two of them fit the buffer the author declared and
        // the third is the one the source is holding. Without the buffer only the first would have.
        await source.EmitAsync(1, TestToken);
        await source.EmitAsync(2, TestToken);
        await source.EmitAsync(3, TestToken);

        // At most one more pull than elements given, whatever the segment below happened to have taken by
        // now: the buffer raises how much the run will accept and not how far ahead it reads.
        Assert.InRange(source.PullsObserved, 3L, 4L);

        Assert.Equal(1, await sink.ReceiveAsync(TestToken));
        Assert.Equal(2, await sink.ReceiveAsync(TestToken));
        Assert.Equal(3, await sink.ReceiveAsync(TestToken));

        source.Complete();

        await sink.ExpectCompletedAsync(TestToken);
        await run.Completion;
    }

    [Fact]
    public async Task ElementsTravelInLockstepThroughABufferedAndAsynchronousChain()
    {
        RunnableGraph graph = TestSource.Probe<int>("emitted")
            .Buffer(new BufferOptions { Capacity = 2 })
            .SelectAsync(new ParallelismOptions { MaxConcurrency = 1 }, (value, _) => Task.FromResult(value * 2))
            .Where(value => value > 0)
            .To(TestSink.Probe<int>("received"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        (ISourceProbe<int> source, ISinkProbe<int> sink) = await Probes(run, graph);

        for (int value = 1; value <= 4; value++)
        {
            await source.EmitAsync(value, TestToken);

            Assert.Equal(value * 2, await sink.ReceiveAsync(TestToken));
        }

        source.Complete();

        await sink.ExpectCompletedAsync(TestToken);
        await run.Completion;
    }

    [Fact]
    public async Task APendingReceiveFailsWhenTheRunCompletesInstead()
    {
        RunnableGraph graph = TestSource.Probe<int>("emitted").To(TestSink.Probe<int>("received"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        (ISourceProbe<int> source, ISinkProbe<int> sink) = await Probes(run, graph);

        Task<int> received = sink.ReceiveAsync(TestToken).AsTask();

        source.Complete();

        ProbeTerminatedException terminated =
            await Assert.ThrowsAsync<ProbeTerminatedException>(async () => await received);

        Assert.Contains("completed", terminated.Message, StringComparison.Ordinal);
        Assert.Null(terminated.InnerException);

        await run.Completion;
    }

    [Fact]
    public async Task APendingReceiveCarriesTheFailureTheRunEndedWith()
    {
        InvalidOperationException failure = new("the producer gave up");
        RunnableGraph graph = TestSource.Probe<int>("emitted").To(TestSink.Probe<int>("received"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        (ISourceProbe<int> source, ISinkProbe<int> sink) = await Probes(run, graph);

        Task<int> received = sink.ReceiveAsync(TestToken).AsTask();

        source.Fail(failure);

        ProbeTerminatedException terminated =
            await Assert.ThrowsAsync<ProbeTerminatedException>(async () => await received);

        Assert.Same(failure, terminated.InnerException);

        // The expectation returns the failure rather than throwing it, so a test asserts about the
        // exception it asked for instead of catching its way to it.
        Assert.Same(failure, await sink.ExpectFailedAsync(TestToken));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
    }

    [Fact]
    public async Task ExpectingTheWrongEndingIsReportedRatherThanAwaitedForever()
    {
        RunnableGraph graph = TestSource.Probe<int>("emitted").To(TestSink.Probe<int>("received"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        (ISourceProbe<int> source, ISinkProbe<int> sink) = await Probes(run, graph);

        source.Complete();

        ProbeTerminatedException terminated =
            await Assert.ThrowsAsync<ProbeTerminatedException>(async () => await sink.ExpectFailedAsync(TestToken));

        Assert.Contains("expected the run to have failed", terminated.Message, StringComparison.Ordinal);
        Assert.Contains("completed successfully instead", terminated.Message, StringComparison.Ordinal);

        await sink.ExpectCompletedAsync(TestToken);
    }

    [Fact]
    public async Task ExpectingACompletionOfAFailedRunNamesTheFailure()
    {
        InvalidOperationException failure = new("the producer gave up");
        RunnableGraph graph = TestSource.Probe<int>("emitted").To(TestSink.Probe<int>("received"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        (ISourceProbe<int> source, ISinkProbe<int> sink) = await Probes(run, graph);

        source.Fail(failure);

        ProbeTerminatedException terminated =
            await Assert.ThrowsAsync<ProbeTerminatedException>(async () => await sink.ExpectCompletedAsync(TestToken));

        Assert.Same(failure, terminated.InnerException);

        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);
    }

    [Fact]
    public async Task ProbesComposeWithAnEarlyCompletionAndNoWaitSurvivesIt()
    {
        RunnableGraph graph = TestSource.Probe<int>("emitted")
            .Take(2)
            .To(TestSink.Probe<int>("received"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        (ISourceProbe<int> source, ISinkProbe<int> sink) = await Probes(run, graph);

        await source.EmitAsync(1, TestToken);

        Assert.Equal(1, await sink.ReceiveAsync(TestToken));

        await source.EmitAsync(2, TestToken);

        Assert.Equal(2, await sink.ReceiveAsync(TestToken));

        // The take has had everything it asked for and the run is ending; a receive issued into that very
        // moment is answered rather than left pending, whichever side of the race it lands on.
        Task<int> third = sink.ReceiveAsync(TestToken).AsTask();

        _ = await Assert.ThrowsAsync<ProbeTerminatedException>(async () => await third);
        _ = await Assert.ThrowsAsync<ProbeTerminatedException>(async () => await source.EmitAsync(3, TestToken));

        await sink.ExpectCompletedAsync(TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
    }

    [Fact]
    public async Task APausedRunHandsOverNothingAndResumingReleasesIt()
    {
        RunnableGraph graph = TestSource.Probe<int>("emitted").To(TestSink.Probe<int>("received"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        (ISourceProbe<int> source, ISinkProbe<int> sink) = await Probes(run, graph);

        await source.EmitAsync(1, TestToken);
        await run.PauseAsync(TestToken);

        Assert.True(run.IsPaused);

        // The demand reaches a run that is being held, and a held run hands over nothing: the element is
        // at the sink and stays there until the run is moving again.
        Task<int> received = sink.ReceiveAsync(TestToken).AsTask();

        Assert.False(received.IsCompleted);

        await run.ResumeAsync();

        Assert.Equal(1, await received);

        source.Complete();

        await sink.ExpectCompletedAsync(TestToken);
    }

    [Fact]
    public async Task AnElementEmittedIntoAPausedRunWaitsAtTheSource()
    {
        // The park point a pushed source needs and a pulled one does not. A run waiting on an empty queue
        // is inside its source when the pause takes effect, so the element that arrives afterwards is
        // produced by a call that began before the pause: without a second look at the gate on the way out
        // of that call, a paused run would deliver it.
        List<int> observed = [];
        TaskCompletionSource delivered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        RunnableGraph graph = TestSource.Probe<int>("emitted")
            .To(s => s.ForEach(value =>
            {
                observed.Add(value);
                delivered.TrySetResult();
            }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        ISourceProbe<int> source = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);

        await source.EmitAsync(1, TestToken);
        await delivered.Task;

        // The first element is through and the run has gone back for another, so what the pause catches is
        // a run inside its own source rather than one standing at a park point.
        await run.PauseAsync(TestToken);

        Task second = source.EmitAsync(2, TestToken).AsTask();

        Assert.Equal([1], observed);

        await run.ResumeAsync();
        await second;

        source.Complete();
        await run.Completion;

        // Once each and in order: the element the source was holding across the pause is the one it
        // delivers when the run moves again.
        Assert.Equal([1, 2], observed);
    }

    [Fact]
    public async Task AProbeHandsOverOneElementAtATime()
    {
        RunnableGraph graph = TestSource.Probe<int>("emitted").To(TestSink.Probe<int>("received"));

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        (ISourceProbe<int> source, ISinkProbe<int> _) = await Probes(run, graph);

        await source.EmitAsync(1, TestToken);

        Task outstanding = source.EmitAsync(2, TestToken).AsTask();

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await source.EmitAsync(3, TestToken));

        Assert.Contains("one element at a time", refused.Message, StringComparison.Ordinal);

        await run.DisposeAsync();

        _ = await Assert.ThrowsAsync<ProbeTerminatedException>(async () => await outstanding);
    }

    [Fact]
    public async Task AShutdownReleasesASinkProbeNobodyIsReceivingFrom()
    {
        // The one case a probe sink discards rather than delivers, and the reason it does: the consumer is
        // the test, and a graceful stop that waited for a receive from a test that has stopped receiving
        // would be a hang rather than a stop.
        RunnableGraph graph = TestSource.Probe<int>("emitted").To(TestSink.Probe<int>("received"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        (ISourceProbe<int> source, ISinkProbe<int> sink) = await Probes(run, graph);

        await source.EmitAsync(1, TestToken);
        await run.ShutdownAsync();

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        await sink.ExpectCompletedAsync(TestToken);
    }

    [Fact]
    public async Task ACancelledRunIsReportedAsCancelledToEveryWait()
    {
        RunnableGraph graph = TestSource.Probe<int>("emitted").To(TestSink.Probe<int>("received"));

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        (ISourceProbe<int> _, ISinkProbe<int> sink) = await Probes(run, graph);

        await run.DisposeAsync();

        ProbeTerminatedException received =
            await Assert.ThrowsAsync<ProbeTerminatedException>(async () => await sink.ReceiveAsync(TestToken));

        Assert.Contains("was cancelled", received.Message, StringComparison.Ordinal);

        ProbeTerminatedException completed =
            await Assert.ThrowsAsync<ProbeTerminatedException>(async () => await sink.ExpectCompletedAsync(TestToken));

        Assert.Contains("was cancelled instead", completed.Message, StringComparison.Ordinal);

        // A cancelled run is not a failed one either: an expectation that accepted a cancellation as a
        // failure would let a test that meant to assert about an exception pass without one.
        ProbeTerminatedException failed =
            await Assert.ThrowsAsync<ProbeTerminatedException>(async () => await sink.ExpectFailedAsync(TestToken));

        Assert.Contains("was cancelled instead", failed.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TwoRunsOfOneGraphHaveProbesOfTheirOwn()
    {
        RunnableGraph graph = TestSource.Probe<int>("emitted").To(TestSink.Probe<int>("received"));

        await using RunHandle first = await Host.MaterializeAsync(graph, TestToken);
        await using RunHandle second = await Host.MaterializeAsync(graph, TestToken);

        (ISourceProbe<int> firstSource, ISinkProbe<int> firstSink) = await Probes(first, graph);
        (ISourceProbe<int> secondSource, ISinkProbe<int> secondSink) = await Probes(second, graph);

        Assert.NotSame(firstSource, secondSource);
        Assert.NotSame(firstSink, secondSink);

        await firstSource.EmitAsync(1, TestToken);
        await secondSource.EmitAsync(2, TestToken);

        Assert.Equal(1, await firstSink.ReceiveAsync(TestToken));
        Assert.Equal(2, await secondSink.ReceiveAsync(TestToken));

        // One element each, and each run asking for at most one more than it was given: two runs of one
        // graph share no queue, no demand, and no meter.
        Assert.InRange(firstSource.PullsObserved, 1L, 2L);
        Assert.InRange(secondSource.PullsObserved, 1L, 2L);

        firstSource.Complete();
        secondSource.Complete();

        await firstSink.ExpectCompletedAsync(TestToken);
        await secondSink.ExpectCompletedAsync(TestToken);
    }

    [Fact]
    public async Task AProbeSinkComposesWithAResultBearingGraphsOwnControlsByName()
    {
        // The probes are ordinary controls: a graph declares them under the names the author wrote, and a
        // name that no graph declares, or one asked for as the wrong type, is a diagnostic rather than a
        // run that resolves nothing.
        RunnableGraph graph = TestSource.Probe<int>("emitted").To(TestSink.Probe<int>("received"));

        Assert.True(graph.TryGetControl("emitted", out ResultSlot<ISourceProbe<int>> _));
        Assert.True(graph.TryGetControl("received", out ResultSlot<ISinkProbe<int>> _));
        Assert.False(graph.TryGetControl("emitted", out ResultSlot<ISinkProbe<int>> _));

        ArgumentException missing = Assert.Throws<ArgumentException>(() => graph.Control<ISinkProbe<int>>("absent"));

        Assert.Contains("declares no runtime control named 'absent'", missing.Message, StringComparison.Ordinal);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        (ISourceProbe<int> source, ISinkProbe<int> sink) = await Probes(run, graph);

        source.Complete();

        await sink.ExpectCompletedAsync(TestToken);
    }

    /// <summary>Resolves both probes of one run.</summary>
    /// <param name="run">The run.</param>
    /// <param name="graph">The graph it is a run of.</param>
    /// <returns>The source probe and the sink probe.</returns>
    /// <remarks>
    /// Both controls resolve at the start of a run, so this never waits: it is written as one call because
    /// every test needs both and neither is interesting to resolve by hand.
    /// </remarks>
    private static async Task<(ISourceProbe<int> Source, ISinkProbe<int> Sink)> Probes(
        RunHandle run,
        RunnableGraph graph) =>
        (await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken),
            await run.GetValueAsync(graph.Control<ISinkProbe<int>>("received"), TestToken));
}
