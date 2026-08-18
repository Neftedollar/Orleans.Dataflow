using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;
using static Orleans.Dataflow.Tests.Runtime.TimingFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What a pause, a shutdown, and a cancellation do to a run that is waiting on a clock.
/// </summary>
/// <remarks>
/// <para>
/// The checkpoint-5 discipline applied to the waits this milestone adds: a wait that does not report itself
/// is a hole in quiescence, so every new wait is asked the question directly — can a run parked in it be
/// paused without the clock moving, and does a stop release it. The answers are not all the same, and where
/// they differ the difference is the operator's own contract rather than an oversight.
/// </para>
/// <para>
/// The one that differs is the delay, and deliberately: its elements are in an asynchronous window rather
/// than in a segment's hand, so a pause waits for them and a shutdown drains them exactly as both do for an
/// author's callback in flight. Every other clock wait — an initial delay's, a throttle's, a tick source's —
/// is the segment's own and is released at once.
/// </para>
/// </remarks>
public sealed class TimingControlTests
{
    [Fact]
    public async Task PausingARunWaitingOnAThrottleReachesQuiescenceWithoutTheClockMoving()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Throttle(new ThrottleOptions { Elements = 1, Per = Second })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await clock.WaitForTimersAsync(1, TestToken);

        // The clock never moves in this test until after the pause has taken effect. A throttle's wait is
        // one of this runtime's own, so it says so to the pause gate and the run comes to rest inside it.
        await run.PauseAsync(TestToken).WaitAsync(TimeSpan.FromSeconds(30), TestToken);

        Assert.True(run.IsPaused);
        Assert.Equal([1], observed);

        await run.ResumeAsync();
        await clock.AdvanceUntilAsync(() => run.Completion.IsCompleted, Second, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 3], observed);
    }

    [Fact]
    public async Task AThrottleDeliversNothingWhileTheRunIsPausedHoweverFarTheClockMoves()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Throttle(new ThrottleOptions { Elements = 1, Per = Second })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await clock.WaitForTimersAsync(1, TestToken);
        await run.PauseAsync(TestToken).WaitAsync(TimeSpan.FromSeconds(30), TestToken);

        // The budget the second element was waiting for arrives while the run is held. The wait ends, and
        // the element stays in the stage's hand: a paused run takes no step, and the park after the wait is
        // what makes that true of a wait that finished during the pause.
        clock.Advance(Second * 10);

        await Task.Delay(TimeSpan.FromMilliseconds(50), TestToken);

        Assert.Equal([1], observed);

        await run.ResumeAsync();
        await clock.AdvanceUntilAsync(() => run.Completion.IsCompleted, Second, TestToken);
        await run.Completion;

        // Everything it was holding is delivered once, unchanged, and in order.
        Assert.Equal([1, 2, 3], observed);
    }

    [Fact]
    public async Task PausingARunWaitingOnATickSourceReachesQuiescenceWithoutTheClockMoving()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<long> observed = [];

        RunnableGraph graph = Source.Tick(Second, Second)
            .Take(2)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await clock.WaitForTimersAsync(1, TestToken);
        await run.PauseAsync(TestToken).WaitAsync(TimeSpan.FromSeconds(30), TestToken);

        Assert.True(run.IsPaused);
        Assert.Empty(observed);

        await run.ResumeAsync();

        for (int tick = 1; tick <= 2; tick++)
        {
            await clock.AdvanceAsync(1, Second, TestToken);
            await Reaches(() => observed.Count == tick, $"tick {tick - 1}", TestToken);
        }

        await run.Completion;

        Assert.Equal([0L, 1L], observed);
    }

    [Fact]
    public async Task PausingARunWithAnElementInADelayWaitsForThatDelayToElapse()
    {
        LocalDataflowHost host = Timed(out TestClock clock);

        RunnableGraph graph = Source.From([1, 2, 3])
            .Delay(Second, new BufferOptions { Capacity = 4 })
            .To(s => s.Count(), "counted", out ResultSlot<long> counted);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await clock.WaitForTimersAsync(3, TestToken);

        Task paused = run.PauseAsync(TestToken);

        await Task.Delay(TimeSpan.FromMilliseconds(50), TestToken);

        // The documented consequence of a delay being a window rather than a hold: the elements inside it
        // are in flight the way an author's callback is, so quiescence is not reached until they finish.
        // The wait is bounded by the delay itself, and under a controlled clock that means the pause takes
        // effect when the test advances.
        Assert.False(paused.IsCompleted);

        clock.Advance(Second);

        await paused.WaitAsync(TimeSpan.FromSeconds(30), TestToken);

        Assert.True(run.IsPaused);

        await run.ResumeAsync();
        await run.Completion;

        Assert.Equal(3L, await run.GetValueAsync(counted, TestToken));
    }

    [Fact]
    public async Task ShuttingDownARunWaitingOnAThrottleDeliversTheElementItWasHolding()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3])
            .Throttle(new ThrottleOptions { Elements = 1, Per = Second })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await clock.WaitForTimersAsync(1, TestToken);
        await run.ShutdownAsync();
        await run.Completion;

        // The clock never moved. Two rules meet here and both hold: a stop releases this runtime's own
        // wait, so the element in the stage's hand is kept rather than held back for a rate that no longer
        // paces anything; and a shutdown stops the pull, so the third element — which the source still had
        // — is not admitted at all. That is "stop pulling and keep what you have" read at a throttle.
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 2], observed);
    }

    [Fact]
    public async Task ShuttingDownARunWithElementsInADelayWaitsThemOutAndDeliversThem()
    {
        LocalDataflowHost host = Timed(out TestClock clock);

        RunnableGraph graph = Source.From([1, 2, 3])
            .Delay(Second, new BufferOptions { Capacity = 4 })
            .To(s => s.Count(), "counted", out ResultSlot<long> counted);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await clock.WaitForTimersAsync(3, TestToken);

        ValueTask stopping = run.ShutdownAsync();

        await Task.Delay(TimeSpan.FromMilliseconds(50), TestToken);

        Assert.False(run.Completion.IsCompleted);

        clock.Advance(Second);

        await stopping;
        await run.Completion;

        // The drain of a window is the drain of an asynchronous stage: what was admitted is waited out and
        // delivered, and nothing is discarded. It is the one clock wait a stop does not cut short.
        Assert.Equal(3L, await run.GetValueAsync(counted, TestToken));
    }

    [Fact]
    public async Task ShuttingDownATickingRunEndsItAsRunningOutOfElementsWould()
    {
        LocalDataflowHost host = Timed(out TestClock clock);

        RunnableGraph graph = Source.Tick(Second, Second)
            .To(s => s.Count(), "counted", out ResultSlot<long> counted);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await clock.AdvanceAsync(1, Second, TestToken);
        await Reaches(() => clock.PendingTimers == 1, "the source waiting for its second tick", TestToken);
        await run.ShutdownAsync();
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(1L, await run.GetValueAsync(counted, TestToken));
    }

    [Theory]
    [InlineData("throttle")]
    [InlineData("delay")]
    [InlineData("tick")]
    [InlineData("initial-delay")]
    public async Task CancellingARunWaitingOnTheClockAbandonsItAndReleasesEverything(string waiting)
    {
        LocalDataflowHost host = Timed(out TestClock clock);

        RunnableGraph graph = waiting switch
        {
            "throttle" => Source.From([1, 2, 3])
                .Throttle(new ThrottleOptions { Elements = 1, Per = Second })
                .To(s => s.Count(), "counted", out ResultSlot<long> _),
            "delay" => Source.From([1, 2, 3])
                .Delay(Second, new BufferOptions { Capacity = 4 })
                .To(s => s.Count(), "counted", out ResultSlot<long> _),
            "initial-delay" => Source.From([1, 2, 3])
                .InitialDelay(Second)
                .To(s => s.Count(), "counted", out ResultSlot<long> _),
            _ => Source.Tick(Second, Second).To(s => s.Count(), "counted", out ResultSlot<long> _),
        };

        RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await clock.WaitForTimersAsync(1, TestToken);

        // The claim is that it returns: disposal waits for every segment to leave its loop, so a wait that
        // could not be woken by a cancellation would hang here rather than fail an assertion. The clock
        // never moves, so nothing but the cancellation can release it.
        await run.DisposeAsync();

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);

        // And the timers are released with the run rather than left on the clock: a timer that outlived its
        // run would be a run's work continuing after the run was over.
        Assert.Equal(0, clock.PendingTimers);
    }

    [Fact]
    public async Task ATimeoutsWatchdogIsReleasedWithTheRunItBelongsTo()
    {
        LocalDataflowHost host = Timed(out TestClock clock);

        RunnableGraph graph = Source.From([1, 2, 3])
            .Timeout(Second)
            .To(s => s.Count(), "counted", out ResultSlot<long> counted);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(3L, await run.GetValueAsync(counted, TestToken));
        Assert.Equal(0, clock.PendingTimers);

        // Nothing fires afterwards, however far the clock moves: a watchdog that outlived its run would
        // fail a run that had already succeeded.
        clock.Advance(Second * 100);

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
    }

    [Fact]
    public async Task APausedRunIsStillMeasuredByTheClockAndATimeoutFiresThrough()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        Gate gate = new();

        RunnableGraph graph = Source.From([1, 2, 3])
            .Timeout(Second)
            .To(s => s.ForEach(_ => gate.Wait()));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await gate.Reached.WaitAsync(TimeSpan.FromSeconds(30), TestToken);

        Task paused = run.PauseAsync(TestToken);

        try
        {
            // A pause holds the elements and not the clock. This is the consequence, stated as a test
            // rather than left to be discovered: a run held for longer than a timeout's gap fails, because
            // the gap between two elements really was that long.
            clock.Advance(Second);
        }
        finally
        {
            gate.Open();
        }

        _ = await Assert.ThrowsAsync<StreamTimeoutException>(() => run.Completion);
        await paused.WaitAsync(TimeSpan.FromSeconds(30), TestToken);
    }
}
