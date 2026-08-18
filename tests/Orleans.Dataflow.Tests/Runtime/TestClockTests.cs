using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the test clock itself promises, asserted before anything is asserted through it.
/// </summary>
/// <remarks>
/// A clock a whole suite's timing claims rest on is an instrument, and an instrument that has not been
/// checked measures nothing. What matters is exactly what the operators depend on: a reading that moves only
/// when a test moves it, timers that fire in due order at their own moment rather than at the end of an
/// advance, a timer armed by a callback inside the same advance firing within it, disposal that really
/// releases, and the interoperation with <see cref="Task.Delay(TimeSpan, TimeProvider, CancellationToken)"/>
/// that every wait in this runtime is built on.
/// </remarks>
public sealed class TestClockTests
{
    [Fact]
    public void TheClockStandsStillUntilItIsAdvanced()
    {
        TestClock clock = new();
        DateTimeOffset start = clock.GetUtcNow();

        Thread.Sleep(TimeSpan.FromMilliseconds(20));

        Assert.Equal(start, clock.GetUtcNow());

        clock.Advance(TimeSpan.FromSeconds(5));

        Assert.Equal(start + TimeSpan.FromSeconds(5), clock.GetUtcNow());
        Assert.Equal(TimeSpan.FromSeconds(5), clock.GetElapsedTime(start.UtcTicks));
    }

    [Fact]
    public void ATimerFiresAtItsOwnMomentAndNotAtTheEndOfTheAdvance()
    {
        TestClock clock = new();
        List<DateTimeOffset> fired = [];

        using ITimer timer = clock.CreateTimer(
            _ => fired.Add(clock.GetUtcNow()),
            state: null,
            TimeSpan.FromSeconds(1),
            Timeout.InfiniteTimeSpan);

        clock.Advance(TimeSpan.FromSeconds(10));

        // The whole point of moving to each due moment in turn: a callback that reads the clock sees the
        // moment it was due at, so an operator re-deriving a deadline from it cannot be thrown off by how
        // far the test happened to advance.
        Assert.Equal([DateTimeOffset.UnixEpoch + TimeSpan.FromSeconds(1)], fired);
        Assert.Equal(DateTimeOffset.UnixEpoch + TimeSpan.FromSeconds(10), clock.GetUtcNow());
    }

    [Fact]
    public void TimersFireInDueOrderAndThenInTheOrderTheyWereArmed()
    {
        TestClock clock = new();
        List<string> fired = [];

        using ITimer late = clock.CreateTimer(
            _ => fired.Add("late"),
            state: null,
            TimeSpan.FromSeconds(3),
            Timeout.InfiniteTimeSpan);
        using ITimer first = clock.CreateTimer(
            _ => fired.Add("first"),
            state: null,
            TimeSpan.FromSeconds(1),
            Timeout.InfiniteTimeSpan);
        using ITimer second = clock.CreateTimer(
            _ => fired.Add("second"),
            state: null,
            TimeSpan.FromSeconds(1),
            Timeout.InfiniteTimeSpan);

        clock.Advance(TimeSpan.FromSeconds(5));

        Assert.Equal(["first", "second", "late"], fired);
    }

    [Fact]
    public void ATimerArmedByACallbackFiresInsideTheSameAdvance()
    {
        TestClock clock = new();
        List<TimeSpan> fired = [];
        ITimer? chained = null;

        using ITimer timer = clock.CreateTimer(
            _ =>
            {
                fired.Add(clock.GetUtcNow() - DateTimeOffset.UnixEpoch);
                chained = clock.CreateTimer(
                    _ => fired.Add(clock.GetUtcNow() - DateTimeOffset.UnixEpoch),
                    state: null,
                    TimeSpan.FromSeconds(1),
                    Timeout.InfiniteTimeSpan);
            },
            state: null,
            TimeSpan.FromSeconds(1),
            Timeout.InfiniteTimeSpan);

        clock.Advance(TimeSpan.FromSeconds(5));
        chained?.Dispose();

        // A timeout's watchdog re-arms itself exactly this way, so an advance that skipped the second fire
        // would make that operator look correct while the clock was hiding the second half of its work.
        Assert.Equal([TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)], fired);
    }

    [Fact]
    public void APeriodicTimerFiresOncePerPeriodOfTheAdvance()
    {
        TestClock clock = new();
        int fired = 0;

        using ITimer timer = clock.CreateTimer(
            _ => fired++,
            state: null,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1));

        clock.Advance(TimeSpan.FromSeconds(4));

        Assert.Equal(4, fired);
    }

    [Fact]
    public void ADisposedTimerNeverFiresAndIsNoLongerHeld()
    {
        TestClock clock = new();
        int fired = 0;

        ITimer timer = clock.CreateTimer(
            _ => fired++,
            state: null,
            TimeSpan.FromSeconds(1),
            Timeout.InfiniteTimeSpan);

        Assert.Equal(1, clock.PendingTimers);

        timer.Dispose();

        Assert.Equal(0, clock.PendingTimers);

        clock.Advance(TimeSpan.FromSeconds(10));

        Assert.Equal(0, fired);
    }

    [Fact]
    public async Task ADelayOnThisClockCompletesWhenTheClockPassesItAndNotBefore()
    {
        TestClock clock = new();
        Task delayed = Task.Delay(TimeSpan.FromSeconds(1), clock, TestToken);

        clock.Advance(TimeSpan.FromSeconds(1) - TimeSpan.FromTicks(1));

        Assert.False(delayed.IsCompleted);

        clock.Advance(TimeSpan.FromTicks(1));

        await delayed;

        // Everything every timing stage of this runtime waits on is this call, so a clock that could not
        // complete one would leave the whole suite asserting nothing.
        Assert.True(delayed.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task ADelayOnThisClockIsCancelledByItsToken()
    {
        TestClock clock = new();
        using CancellationTokenSource cancellation = new();
        Task delayed = Task.Delay(TimeSpan.FromSeconds(1), clock, cancellation.Token);

        await cancellation.CancelAsync();

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => delayed);

        // And the clock is no longer holding it: a cancelled wait that stayed armed would make every later
        // WaitForTimersAsync in the same test answer for a wait that no longer exists.
        await TimingFixtures.Reaches(() => clock.PendingTimers == 0, "the cancelled delay being released", TestToken);
    }

    [Fact]
    public async Task WaitingForTimersAnswersWhenTheyAreArmedAndAtOnceWhenTheyAlreadyAre()
    {
        TestClock clock = new();
        Task waiting = clock.WaitForTimersAsync(2, TestToken);

        Assert.False(waiting.IsCompleted);

        using ITimer first = clock.CreateTimer(
            static _ => { },
            state: null,
            TimeSpan.FromSeconds(1),
            Timeout.InfiniteTimeSpan);

        Assert.False(waiting.IsCompleted);

        using ITimer second = clock.CreateTimer(
            static _ => { },
            state: null,
            TimeSpan.FromSeconds(1),
            Timeout.InfiniteTimeSpan);

        await waiting;
        await clock.WaitForTimersAsync(1, TestToken);

        Assert.Equal(2, clock.PendingTimers);
    }

    [Fact]
    public void RewindingIsRefusedRatherThanAnswered()
    {
        TestClock clock = new();

        _ = Assert.Throws<ArgumentOutOfRangeException>(() => clock.Advance(TimeSpan.FromSeconds(-1)));
    }
}
