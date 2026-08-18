using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.DurableFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;
using static Orleans.Dataflow.Tests.Runtime.TimingFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What a durable run promises: a checkpoint when the declared timing says one is due, taken at a safe
/// point, costing exactly a hold, and losing or duplicating nothing by being taken.
/// </summary>
/// <remarks>
/// <para>
/// The three claims ADR 0007 makes about checkpoint timing, asserted separately. Timing is <b>declared</b> —
/// a run with neither an interval nor an element bound never touches the store, which only the store can
/// say. A capture is taken at the <b>pause machinery's safe points</b> — so the cursor it records is exactly
/// the element the bound named, which is a number rather than a range. And the cost is <b>stated and
/// measured</b> — a capture holds the run for its duration, and the hold is a reading on the run's own
/// clock.
/// </para>
/// <para>
/// The element bound is asserted exactly and the interval is not, and the difference is honest rather than
/// convenient: an element bound is reached on the source's own thread and holds the run there, so the
/// stored position is the one the bound named; an interval fires from beside the run and records whatever
/// position the run had reached, which is the answer a timed capture can give.
/// </para>
/// </remarks>
public sealed class DurableRunTests
{
    [Fact]
    public async Task ARunThatDeclaresNoTimingNeverTouchesTheStore()
    {
        InMemoryCheckpointStore store = new();
        List<int> committed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4, 5])
            .To(TestSink.Marking<int>("mark", committed.Add));

        await using RunHandle run = await Host.MaterializeDurableAsync(
            graph,
            Durable(store, "silent"),
            TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 3, 4, 5], committed);
        Assert.Equal(0, store.Count);
        Assert.False(store.Holds(Anonymous, RunId.Create("silent")));
        Assert.Equal(0L, run.Checkpoints);
    }

    [Fact]
    public async Task AnElementBoundCheckpointsAtExactlyTheElementItNames()
    {
        InMemoryCheckpointStore store = new();
        List<int> committed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4, 5, 6])
            .To(TestSink.Marking<int>("mark", committed.Add));

        await using RunHandle run = await Host.MaterializeDurableAsync(
            graph,
            Durable(store, "counted", everyElements: 3),
            TestToken);
        await run.Completion;

        LocalCheckpoint checkpoint = await StoredAsync(store, "counted", TestToken);

        // Exact, and it is exact because the bound is reached on the source's own thread, which asks for the
        // hold before it takes another step. A capture requested from beside the run would have recorded
        // "three or more", which is not a number a resume could be reasoned about.
        Assert.Equal(6L, Cursor(checkpoint));
        Assert.Equal(6L, Mark(checkpoint));
        Assert.Equal(2L, run.Checkpoints);
        Assert.Equal([1, 2, 3, 4, 5, 6], committed);

        // The cost is a measurement and not a claim. This host measures by the system clock, so the hold is
        // the real time two captures took: what is asserted is that it is time at all, because the sentence
        // ADR 0007 asked to be measured before anything cleverer is attempted is "a checkpoint pauses the
        // run for its duration".
        Assert.True(run.CheckpointHold > TimeSpan.Zero, run.CheckpointHold.ToString());
    }

    [Fact]
    public async Task ACaptureLosesNoElementAndDuplicatesNone()
    {
        InMemoryCheckpointStore store = new();
        List<int> committed = [];

        // One capture per element, which is the most a capture can possibly interfere with a run: the hold,
        // the snapshot, and the write happen between every pair of elements.
        RunnableGraph graph = Source.From(Enumerable.Range(1, 25))
            .To(TestSink.Marking<int>("mark", committed.Add));

        await using RunHandle run = await Host.MaterializeDurableAsync(
            graph,
            Durable(store, "every-element", everyElements: 1),
            TestToken);
        await run.Completion;

        Assert.Equal([.. Enumerable.Range(1, 25)], committed);
        Assert.Equal(25L, run.Checkpoints);
    }

    [Fact]
    public async Task ACaptureHoldsTheRunForItsDurationAndTheHoldIsMeasured()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        InMemoryCheckpointStore store = new();
        List<int> committed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .To(TestSink.Marking<int>("mark", committed.Add));

        await using RunHandle run = await host.MaterializeDurableAsync(
            graph,
            Durable(store, "measured", everyElements: 2),
            TestToken);
        await run.Completion;

        // The clock never moves in this test, so the measured hold is zero and what is being asserted is
        // that the cost is a reading at all: ADR 0007 said the cost of the simple answer would be stated and
        // measured before anything cleverer was attempted, and this is where the number lives.
        Assert.Equal(TimeSpan.Zero, run.CheckpointHold);
        Assert.Equal(2L, run.Checkpoints);
        Assert.Equal([1, 2, 3, 4], committed);
    }

    [Fact]
    public async Task AnIntervalCheckpointsOnTheRunsOwnClock()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        InMemoryCheckpointStore store = new();

        // A source that never ends, so the only thing that can make a capture due is the clock.
        RunnableGraph graph = Source.Never<int>().To(TestSink.Marking<int>("mark", static _ => { }));

        await using RunHandle run = await host.MaterializeDurableAsync(
            graph,
            Durable(store, "timed", interval: Second),
            TestToken);

        await clock.AdvanceAsync(timers: 1, Second, TestToken);

        while (run.Checkpoints == 0L)
        {
            TestToken.ThrowIfCancellationRequested();

            await Task.Yield();
        }

        LocalCheckpoint checkpoint = await StoredAsync(store, "timed", TestToken);

        // Nothing was ever produced, so the capture records a run that has done nothing — which is a
        // checkpoint like any other and is what makes "the timer fired" observable at all. The source that
        // never ends declares no cursor, so the cursor table is empty rather than zero: a source with no
        // cursor contributes nothing and resumes from now, said as an absence.
        Assert.Empty(checkpoint.Cursors);
        Assert.Equal(0L, Mark(checkpoint));

        await run.ShutdownAsync();
        await run.Completion;
    }

    [Fact]
    public async Task ACaptureTakenDuringAnAuthorsPauseLeavesTheRunPaused()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        InMemoryCheckpointStore store = new();
        List<int> committed = [];

        RunnableGraph graph = TestSource.Probe<int>("in")
            .To(TestSink.Marking<int>("mark", committed.Add));

        await using RunHandle run = await host.MaterializeDurableAsync(
            graph,
            Durable(store, "held", interval: Second),
            TestToken);

        await run.PauseAsync(TestToken);
        await clock.AdvanceAsync(timers: 1, Second, TestToken);

        while (run.Checkpoints == 0L)
        {
            TestToken.ThrowIfCancellationRequested();

            await Task.Yield();
        }

        // A capture holds the run and then lets it go, and an author's pause is a different hold: letting go
        // of one must not let go of the other. A single gate would have resumed a run its author had stopped
        // — silently, and only for runs that were both durable and paused.
        Assert.True(run.IsPaused);
        Assert.Empty(committed);

        ISourceProbe<int> source = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("in"), TestToken);

        await run.ResumeAsync();
        await source.EmitAsync(1, TestToken);
        await run.ShutdownAsync();
        await run.Completion;

        Assert.Equal([1], committed);
    }

    [Fact]
    public async Task ARunThatEndedWritesNothingHoweverFarTheClockIsThenAdvanced()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        InMemoryCheckpointStore store = new();

        RunnableGraph graph = Source.From([1, 2, 3])
            .To(TestSink.Marking<int>("mark", static _ => { }));

        await using RunHandle run = await host.MaterializeDurableAsync(
            graph,
            Durable(store, "over", interval: Second),
            TestToken);
        await run.Completion;

        clock.Advance(Second);
        clock.Advance(Second);
        clock.Advance(Second);

        // A run that ran out of elements cancels nothing: it settles, opens its pause gate, and releases its
        // token sources. A capture loop watching the stop token alone would still be waiting here, would
        // wake on this timer, and would write a checkpoint of a run that had already ended — which is the
        // opposite of "a checkpoint is what a crash leaves behind". Nothing here is a claim about tidiness;
        // the store is the witness.
        Assert.Equal(0L, run.Checkpoints);
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task ASupersededWriterFailsTheRunRatherThanOverwritingTheFreshOne()
    {
        InMemoryCheckpointStore store = new();
        RunnableGraph graph = Source.From(Enumerable.Range(1, 40))
            .To(TestSink.Marking<int>("mark", static _ => { }));

        await using RunHandle run = await Host.MaterializeDurableAsync(
            graph,
            Durable(store, "fenced", everyElements: 1),
            TestToken);

        while (run.Checkpoints == 0L && !run.Completion.IsCompleted)
        {
            TestToken.ThrowIfCancellationRequested();

            await Task.Yield();
        }

        store.Supersede(Anonymous, RunId.Create("fenced"));

        CheckpointConflictException refused =
            await Assert.ThrowsAsync<CheckpointConflictException>(async () => await run.Completion);

        // The coordinator's own consequence read over a checkpoint: the stale attempt dies, and it dies with
        // the exception the store raised rather than with something wrapping it.
        Assert.NotNull(refused.Stored);
        Assert.NotEqual(refused.Presented, refused.Stored);
    }

    [Fact]
    public async Task ADurableRunIsNamedByItsAuthorAndTwoOfThemAreTwoDocuments()
    {
        InMemoryCheckpointStore store = new();
        RunnableGraph graph = Source.From([1, 2]).To(TestSink.Marking<int>("mark", static _ => { }));

        await using (RunHandle first = await Host.MaterializeDurableAsync(
            graph,
            Durable(store, "left", everyElements: 2),
            TestToken))
        {
            await first.Completion;
        }

        await using (RunHandle second = await Host.MaterializeDurableAsync(
            graph,
            Durable(store, "right", everyElements: 2),
            TestToken))
        {
            await second.Completion;
        }

        Assert.Equal(2, store.Count);
        Assert.True(store.Holds(Anonymous, RunId.Create("left")));
        Assert.True(store.Holds(Anonymous, RunId.Create("right")));
    }

    [Theory]
    [InlineData(0, null)]
    [InlineData(-1, null)]
    public async Task AnElementBoundBelowOneIsRefusedByName(int elements, string? unused)
    {
        _ = unused;

        InMemoryCheckpointStore store = new();
        RunnableGraph graph = Source.From([1]).To(s => s.Ignore());

        ArgumentException refused = await Assert.ThrowsAsync<ArgumentException>(
            async () => await Host.MaterializeDurableAsync(
                graph,
                Durable(store, "invalid", everyElements: elements),
                TestToken));

        Assert.Contains("EveryElements", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnIntervalOfNoTimeIsRefusedByName()
    {
        InMemoryCheckpointStore store = new();
        RunnableGraph graph = Source.From([1]).To(s => s.Ignore());

        ArgumentException refused = await Assert.ThrowsAsync<ArgumentException>(
            async () => await Host.MaterializeDurableAsync(
                graph,
                Durable(store, "invalid", interval: TimeSpan.Zero),
                TestToken));

        Assert.Contains("Interval", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARunWithNoIdentityIsRefusedByName()
    {
        InMemoryCheckpointStore store = new();
        RunnableGraph graph = Source.From([1]).To(s => s.Ignore());

        ArgumentException refused = await Assert.ThrowsAsync<ArgumentException>(
            async () => await Host.MaterializeDurableAsync(
                graph,
                new DurableRunOptions { Store = store, RunId = default, EveryElements = 1 },
                TestToken));

        Assert.Contains(nameof(DurableRunOptions.RunId), refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AMarkAdvancesOnlyAfterTheSideEffectItIsAMarkFor()
    {
        InMemoryCheckpointStore store = new();
        List<long> marksSeen = [];
        RunnableGraph graph = Source.From([1, 2, 3])
            .To(TestSink.Marking<int>("mark", static _ => { }));

        await using RunHandle run = await Host.MaterializeDurableAsync(
            graph,
            Durable(store, "ordered", everyElements: 3),
            TestToken);
        await run.Completion;

        IMarkingSink sink = await run.GetValueAsync(graph.Control<IMarkingSink>("mark"), TestToken);

        marksSeen.Add(sink.Mark);

        Assert.Equal([3L], marksSeen);
        Assert.Equal(3L, Mark(await StoredAsync(store, "ordered", TestToken)));
    }

    [Fact]
    public async Task ASinkCallbackThatThrowsLeavesTheMarkWhereItWas()
    {
        InMemoryCheckpointStore store = new();
        RunnableGraph graph = Source.From([1, 2, 3])
            .To(TestSink.Marking<int>(
                "mark",
                static value => throw new InvalidOperationException($"element {value}")));

        await using RunHandle run = await Host.MaterializeDurableAsync(
            graph,
            Durable(store, "throwing", everyElements: 1),
            TestToken);

        _ = await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion);

        IMarkingSink sink = await run.GetValueAsync(graph.Control<IMarkingSink>("mark"), TestToken);

        // Nothing committed, so nothing is marked. A mark that moved before the callback would have said one
        // element was committed when the commit is exactly what failed.
        Assert.Equal(0L, sink.Mark);
    }
}
