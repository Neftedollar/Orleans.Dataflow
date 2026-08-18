using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;
using static Orleans.Dataflow.Tests.Runtime.TimingFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What each timing operator promises, measured on a clock the test moves by hand.
/// </summary>
/// <remarks>
/// <para>
/// Every assertion here is about a moment, so every one of them records the clock's own reading at the sink
/// rather than counting elements: an operator that emitted the right elements at the wrong time would pass a
/// count. The clock never advances by itself, so "not one tick before its deadline" is a claim these tests
/// can actually make — the run is advanced to one tick short of the moment, asserted to have done nothing,
/// and then advanced the last tick.
/// </para>
/// <para>
/// The two halves of the discipline a virtual clock needs are in <see cref="TimingFixtures"/>: wait for the
/// run to have armed its wait before advancing past it, and wait for the run's own thread to act on what the
/// advance released. Nothing here sleeps for wall-clock time.
/// </para>
/// </remarks>
public sealed class TimingTests
{
    [Fact]
    public async Task DelayEmitsEveryElementExactlyItsDelayLater()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        DateTimeOffset start = clock.GetUtcNow();
        List<(int Value, DateTimeOffset At)> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Delay(Second, new BufferOptions { Capacity = 4 })
            .To(s => s.ForEach(value => observed.Add((value, clock.GetUtcNow()))));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        // Three elements are admitted at once, because the holdback is wider than the stream, so all three
        // are waiting out the same delay from the same moment.
        await clock.AdvanceAsync(3, Second - Instant, TestToken);

        Assert.Empty(observed);

        clock.Advance(Instant);

        await run.Completion;

        Assert.Equal([(1, start + Second), (2, start + Second), (3, start + Second)], observed);
    }

    [Fact]
    public async Task DelayShiftsABurstRatherThanPacingIt()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        DateTimeOffset start = clock.GetUtcNow();
        List<(int Value, DateTimeOffset At)> observed = [];

        RunnableGraph graph = TestSource.Probe<int>("emitted")
            .Delay(Second, new BufferOptions { Capacity = 4 })
            .To(s => s.ForEach(value => observed.Add((value, clock.GetUtcNow()))));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);
        ISourceProbe<int> probe = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);

        await probe.EmitAsync(1, TestToken);
        await clock.WaitForTimersAsync(1, TestToken);

        clock.Advance(TimeSpan.FromMilliseconds(400));

        await probe.EmitAsync(2, TestToken);
        await clock.AdvanceAsync(2, TimeSpan.FromMilliseconds(600), TestToken);
        await Reaches(() => observed.Count == 1, "the first element leaving the delay", TestToken);

        clock.Advance(TimeSpan.FromMilliseconds(400));

        await Reaches(() => observed.Count == 2, "the second element leaving the delay", TestToken);

        probe.Complete();

        await run.Completion;

        // The gap the two elements arrived with is the gap they leave with, which is what separates a delay
        // from a throttle: a stage holding one element at a time would have emitted the second a whole
        // delay after the first rather than four hundred milliseconds after it.
        Assert.Equal(
            [(1, start + Second), (2, start + Second + TimeSpan.FromMilliseconds(400))],
            observed);
    }

    [Fact]
    public async Task DelayHoldsNoMoreThanItsDeclaredHoldbackAndOneHandoff()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8);

        RunnableGraph graph = Source.From(elements)
            .Delay(Second, new BufferOptions { Capacity = 2 })
            .To(s => s.Count(), "counted", out ResultSlot<long> counted);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        // Two elements are waiting out their delay, one more is in the handoff in front of them, and one
        // is in the source's own hand at a boundary with no room — the accounting every bounded-memory test
        // in this suite makes. A fifth pull would mean the delay had read ahead of what it declared, and
        // nothing can move until the clock does, so the number this settles at is the peak.
        await clock.WaitForTimersAsync(2, TestToken);
        await Reaches(() => elements.Pulls >= 4, "the delay filling its holdback", TestToken);

        Assert.Equal(4, elements.Pulls);

        await clock.AdvanceUntilAsync(() => run.Completion.IsCompleted, Second, TestToken);
        await run.Completion;

        Assert.Equal(8L, await run.GetValueAsync(counted, TestToken));
    }

    [Fact]
    public async Task ADelayUnderADroppingHoldbackDropsRatherThanPacingItsSource()
    {
        LocalDataflowHost host = Timed(out TestClock clock);

        RunnableGraph graph = Source.From([1, 2, 3, 4, 5, 6, 7, 8])
            .Delay(Second, new BufferOptions { Capacity = 2, OverflowPolicy = OverflowPolicy.DropNewest })
            .To(s => s.Count(), "counted", out ResultSlot<long> counted);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        // The source runs to its end without waiting for the clock, because the holdback answers every
        // element it has no room for by dropping it. Every element is therefore accounted for: delivered or
        // counted as a drop.
        await Reaches(() => run.DroppedElements > 0, "the holdback dropping an element", TestToken);
        await clock.AdvanceAsync(1, Second, TestToken);
        await run.Completion;

        long delivered = await run.GetValueAsync(counted, TestToken);

        Assert.Equal(8L, delivered + run.DroppedElements);
        Assert.True(delivered < 8L, $"nothing was dropped: {delivered} of 8 were delivered");
    }

    [Fact]
    public async Task InitialDelayHoldsTheFirstElementAndNothingAfterIt()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        DateTimeOffset start = clock.GetUtcNow();
        List<(int Value, DateTimeOffset At)> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .InitialDelay(Second)
            .To(s => s.ForEach(value => observed.Add((value, clock.GetUtcNow()))));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await clock.AdvanceAsync(1, Second - Instant, TestToken);

        Assert.Empty(observed);

        clock.Advance(Instant);

        await run.Completion;

        // One wait and not three: the elements behind the first are not delayed at all, which is the whole
        // difference between delaying a stream and delaying its elements.
        Assert.Equal([(1, start + Second), (2, start + Second), (3, start + Second)], observed);
        Assert.Equal(0, clock.PendingTimers);
    }

    [Fact]
    public async Task InitialDelayDoesNotDelayAStreamWhoseFirstElementIsLate()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        DateTimeOffset start = clock.GetUtcNow();
        List<(int Value, DateTimeOffset At)> observed = [];

        RunnableGraph graph = TestSource.Probe<int>("emitted")
            .InitialDelay(Second)
            .To(s => s.ForEach(value => observed.Add((value, clock.GetUtcNow()))));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);
        ISourceProbe<int> probe = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);

        clock.Advance(Second * 3);

        await probe.EmitAsync(1, TestToken);
        await Reaches(() => observed.Count == 1, "the element passing an elapsed initial delay", TestToken);

        probe.Complete();

        await run.Completion;

        // The wait is for a moment rather than for a duration, so a stream that starts after that moment is
        // not held at all.
        Assert.Equal([(1, start + (Second * 3))], observed);
    }

    [Fact]
    public async Task SkipWithinDropsWhatArrivesInsideItsWindowAndPassesTheRest()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<int> observed = [];

        RunnableGraph graph = TestSource.Probe<int>("emitted")
            .SkipWithin(Second)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);
        ISourceProbe<int> probe = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);

        await probe.EmitAsync(1, TestToken);

        clock.Advance(Second - Instant);

        await probe.EmitAsync(2, TestToken);

        clock.Advance(Instant);

        await probe.EmitAsync(3, TestToken);
        await probe.EmitAsync(4, TestToken);

        probe.Complete();

        await run.Completion;

        // The boundary is exactly the window: the element one tick short of it is dropped and the one at it
        // is not. Nothing is held — an element inside the window is answered the moment it arrives.
        Assert.Equal([3, 4], observed);
        Assert.Equal(0, clock.PendingTimers);
    }

    [Fact]
    public async Task TakeWithinKeepsWhatArrivedBeforeItsDeadlineAndEndsTheStream()
    {
        LocalDataflowHost host = Timed(out TestClock clock);

        List<long> observed = [];

        RunnableGraph graph = Source.Tick(Second, Second)
            .TakeWithin((Second * 2) + TimeSpan.FromMilliseconds(500))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        // Both timers before every advance: the window's, armed when the run starts, and the tick source's,
        // armed at its first pull and re-armed after every tick. Advancing while the source has not yet
        // armed its next wait would move time past a tick the source had not asked for, and a missed tick
        // is a skipped tick — which is the source's contract and would be this test's flake.
        for (int tick = 1; tick <= 2; tick++)
        {
            await clock.AdvanceAsync(2, Second, TestToken);
            await Reaches(() => observed.Count == tick, $"tick {tick - 1} reaching the sink", TestToken);
        }

        await clock.AdvanceAsync(2, Second, TestToken);

        await run.Completion;

        // Ticks zero and one are inside the window and tick two is not: it arrives at three seconds, half a
        // second past the deadline, and ends the stream instead of being emitted. The run ends successfully,
        // the way reaching a Take bound ends one.
        Assert.Equal([0L, 1L], observed);
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
    }

    [Fact]
    public async Task TakeWithinEndsItsOwnStreamAtTheDeadlineWithNoElementToEndItOn()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<long> observed = [];

        // The window closes while this stage is asleep waiting for an element, which is the case the
        // operator exists for: what ends the stream is its own timer and not an arrival. The buffer is what
        // puts the stage in a segment of its own, so that its wait really is a wait for an element.
        RunnableGraph graph = Source.Tick(Second * 10, Second * 10)
            .Buffer(new BufferOptions { Capacity = 1 })
            .TakeWithin(Second * 3)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await clock.AdvanceAsync(1, Second * 3, TestToken);

        // The stream at that stage has ended, and the source above it has not learned yet: it is asleep in
        // a wait of this runtime's own, which a completion below does not release — the same rule that
        // leaves a source parked on an empty channel where it is. It learns at its next tick, when the
        // channel it offers into refuses the element.
        Assert.False(run.Completion.IsCompleted);

        clock.Advance(Second * 7);

        await run.Completion;

        Assert.Empty(observed);
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
    }

    [Fact]
    public async Task TimeoutFailsARunWhoseStreamNeverProducesAnything()
    {
        LocalDataflowHost host = Timed(out TestClock clock);

        RunnableGraph graph = Source.Never<int>()
            .Timeout(Second)
            .To(s => s.Count(), "counted", out ResultSlot<long> counted);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await clock.AdvanceAsync(1, Second - Instant, TestToken);

        Assert.False(run.Completion.IsCompleted);

        clock.Advance(Instant);

        StreamTimeoutException failure =
            await Assert.ThrowsAsync<StreamTimeoutException>(() => run.Completion);

        // The gap before the first element is a gap: a run whose source never produces fails rather than
        // hanging, which is the case a timeout is written for.
        Assert.Contains("of the run starting", failure.Message, StringComparison.Ordinal);
        Assert.Same(
            failure,
            (await Assert.ThrowsAsync<StreamTimeoutException>(async () => await run.GetValueAsync(counted, TestToken))));
    }

    [Fact]
    public async Task TimeoutFailsARunWhoseStreamGoesQuietAfterSomeElements()
    {
        LocalDataflowHost host = Timed(out TestClock clock);

        RunnableGraph graph = TestSource.Probe<int>("emitted")
            .Timeout(Second)
            .To(s => s.Count(), "counted", out ResultSlot<long> _);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);
        ISourceProbe<int> probe = await run.GetValueAsync(graph.Control<ISourceProbe<int>>("emitted"), TestToken);

        await probe.EmitAsync(1, TestToken);

        clock.Advance(Second - Instant);

        await probe.EmitAsync(2, TestToken);

        // The gap is measured from the element before it and not from the run's start, so the two arrivals
        // above cost nothing at all: the watchdog re-arms on each of them.
        Assert.False(run.Completion.IsCompleted);

        clock.Advance(Second);

        StreamTimeoutException failure =
            await Assert.ThrowsAsync<StreamTimeoutException>(() => run.Completion);

        Assert.Contains("after 2 of them", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TimeoutNeverFiresWhileElementsKeepArriving()
    {
        LocalDataflowHost host = Timed(out TestClock clock);

        List<long> observed = [];

        RunnableGraph graph = Source.Tick(TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(500))
            .Timeout(Second)
            .Take(4)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        // Two timers before every advance: the watchdog and the source's own wait. Waiting for both is what
        // keeps the test from moving time past a tick the source had not yet asked for.
        for (int tick = 1; tick <= 4; tick++)
        {
            await clock.AdvanceAsync(2, TimeSpan.FromMilliseconds(500), TestToken);
            await Reaches(() => observed.Count == tick, $"tick {tick - 1} reaching the sink", TestToken);
        }

        await run.Completion;

        // Half a second of silence is not a second of it, so the watchdog re-arms four times and never
        // fires: a stream that keeps its promise is not interrupted by the operator that measures it.
        Assert.Equal([0L, 1L, 2L, 3L], observed);
    }

    [Fact]
    public async Task AWindowLongerThanTheSystemTimerAllowsStillRuns()
    {
        // The system clock's timers refuse a due time past about forty-nine days, and a window or a gap of
        // months is an ordinary thing for an author to write. The stages arm for what the clock accepts and
        // re-examine when it fires, so this materializes and runs on the real clock rather than throwing
        // an argument exception from inside a timer nobody asked about.
        LocalDataflowHost host = new();
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Timeout(TimeSpan.FromDays(400))
            .TakeWithin(TimeSpan.FromDays(400))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 3], observed);
    }

    [Fact]
    public async Task ADelayHeadingABranchStillAnswersWithItsDeclaredPolicy()
    {
        // The channel a delay reads at the head of a branch is the one the junction above it writes, so the
        // policy the delay declared has to be that channel's. Without that, a dropping holdback written on
        // a leg would silently backpressure the split instead — which is the same rule a buffer written
        // immediately below a junction already follows, read for a stage that is not only a channel.
        LocalDataflowHost host = Timed(out TestClock clock);

        RunnableGraph graph = Source.From([1, 2, 3, 4, 5, 6, 7, 8])
            .AlsoTo(
                Flow.For<int>()
                    .Delay(Second, new BufferOptions { Capacity = 1, OverflowPolicy = OverflowPolicy.DropNewest })
                    .To(s => s.Count(), "delayed", out ResultSlot<long> delayed))
            .To(s => s.Count(), "counted", out ResultSlot<long> counted);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        // The other leg runs to its end without the clock moving at all, which it could not do if the
        // delayed leg were pacing the broadcast above it.
        await Reaches(() => run.DroppedElements > 0, "the holdback on the leg dropping an element", TestToken);
        await clock.AdvanceUntilAsync(() => run.Completion.IsCompleted, Second, TestToken);
        await run.Completion;

        Assert.Equal(8L, await run.GetValueAsync(counted, TestToken));
        Assert.Equal(8L, await run.GetValueAsync(delayed, TestToken) + run.DroppedElements);
    }

    [Fact]
    public async Task EveryRunOfATimingGraphStartsItsOwnClockAgain()
    {
        // The state a timing stage carries — an initial delay's one hold, a throttle's bucket, a window's
        // deadline — is allocated per materialization like every other stage's, and the zero it measures
        // from is its own run's. Two runs of one graph therefore each delay their own first element.
        LocalDataflowHost host = Timed(out TestClock clock);
        List<DateTimeOffset> observed = [];
        DateTimeOffset start = clock.GetUtcNow();

        RunnableGraph graph = Source.From([1])
            .InitialDelay(Second)
            .To(s => s.ForEach(_ => observed.Add(clock.GetUtcNow())));

        await using RunHandle first = await host.MaterializeAsync(graph, TestToken);

        await clock.AdvanceAsync(1, Second, TestToken);
        await first.Completion;

        await using RunHandle second = await host.MaterializeAsync(graph, TestToken);

        await clock.AdvanceAsync(1, Second, TestToken);
        await second.Completion;

        Assert.Equal([start + Second, start + (Second * 2)], observed);
    }

    [Fact]
    public async Task EveryTimingOperatorReadsTheHostsClockAndNotTheSystemOne()
    {
        // The claim ADR 0005 rests on, made as one assertion: a run whose host was given a clock that never
        // moves does not emit, however long real time takes. Every operator here would otherwise have to be
        // trusted one by one to have reached for the right clock.
        LocalDataflowHost host = Timed(out TestClock clock);
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .InitialDelay(Second)
            .Delay(Second, new BufferOptions { Capacity = 4 })
            .Throttle(new ThrottleOptions { Elements = 1, Per = Second })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await Task.Delay(TimeSpan.FromMilliseconds(50), TestToken);

        Assert.Empty(observed);
        Assert.False(run.Completion.IsCompleted);

        await clock.AdvanceUntilAsync(() => run.Completion.IsCompleted, Second, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 3], observed);
    }
}
