using Orleans.Dataflow.Testing;
using Xunit;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What every test of a clock-reading stage needs: a controlled clock, a host measuring by it, and the
/// durations the assertions are written in.
/// </summary>
/// <remarks>
/// <para>
/// A host per test rather than one shared instance, because the clock is the host's: two tests sharing one
/// would share the moment as well, and a test that advanced time would move every other test's deadlines.
/// The host is cheap — it is stateless and holds no run.
/// </para>
/// <para>
/// The durations are named rather than written inline so that a test reads as "one tick after the deadline"
/// rather than as arithmetic. They are large and round because a virtual clock costs nothing to advance:
/// nothing here waits for wall-clock time, so a second of virtual time is as cheap as a millisecond and far
/// easier to read in a failing assertion.
/// </para>
/// </remarks>
internal static class TimingFixtures
{
    /// <summary>The ordinary duration a test's operators are configured by.</summary>
    internal static readonly TimeSpan Second = TimeSpan.FromSeconds(1);

    /// <summary>The smallest amount of time this clock can move.</summary>
    /// <remarks>
    /// One tick, which is what makes "not one tick before its deadline" a claim a test can actually make:
    /// the clock's own resolution is a tick, so an assertion that advances by this and finds nothing has
    /// shown that the operator's boundary is exactly where it says it is.
    /// </remarks>
    internal static readonly TimeSpan Instant = TimeSpan.FromTicks(1);

    /// <summary>Builds a controlled clock and a host that measures every run by it.</summary>
    /// <param name="clock">When this method returns, the clock the test advances.</param>
    /// <returns>The host.</returns>
    internal static LocalDataflowHost Timed(out TestClock clock)
    {
        clock = new TestClock();

        return new LocalDataflowHost(clock);
    }

    /// <summary>Waits until the run is holding a given number of timers, then advances the clock.</summary>
    /// <param name="clock">The clock to advance.</param>
    /// <param name="timers">How many armed timers to wait for first.</param>
    /// <param name="delta">How far to advance once they exist.</param>
    /// <param name="cancellationToken">The running test's own token.</param>
    /// <returns>A task that completes when the clock has been advanced.</returns>
    /// <remarks>
    /// The idiom every timing test needs and the one thing a virtual clock makes a test responsible for:
    /// advancing time before the run has reached its wait would arm that wait after the moment it was
    /// waiting for, and the run would then sit there until the test advanced again — a flake that reads as
    /// a hang. Waiting for the timer to exist first turns that into an ordinary ordering.
    /// </remarks>
    internal static async Task AdvanceAsync(
        this TestClock clock,
        int timers,
        TimeSpan delta,
        CancellationToken cancellationToken)
    {
        await clock.WaitForTimersAsync(timers, cancellationToken);

        clock.Advance(delta);
    }

    /// <summary>Advances the clock in steps until the run has reached a state.</summary>
    /// <param name="clock">The clock to advance.</param>
    /// <param name="reached">The state to advance towards.</param>
    /// <param name="step">How far to advance per round.</param>
    /// <param name="cancellationToken">The running test's own token.</param>
    /// <returns>A task that completes when the state holds.</returns>
    /// <remarks>
    /// For the tests whose claim is <i>what</i> a run produces rather than <i>when</i>: advancing past
    /// several operators' deadlines in the right order is bookkeeping that says nothing, and a test that got
    /// it wrong would report a hang rather than a defect. Tests whose claim is about a moment advance by
    /// hand instead, one deadline at a time.
    /// </remarks>
    internal static async Task AdvanceUntilAsync(
        this TestClock clock,
        Func<bool> reached,
        TimeSpan step,
        CancellationToken cancellationToken)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(30);

        while (!reached())
        {
            Assert.True(DateTime.UtcNow < deadline, "The run never reached the state the clock was advanced towards.");

            clock.Advance(step);

            await Task.Delay(TimeSpan.FromMilliseconds(2), cancellationToken);
        }
    }

    /// <summary>Waits for a condition the run reaches on its own thread.</summary>
    /// <param name="reached">The condition.</param>
    /// <param name="what">What the condition is, for the failure message.</param>
    /// <param name="cancellationToken">The running test's own token.</param>
    /// <returns>A task that completes when the condition holds.</returns>
    /// <remarks>
    /// A virtual clock makes waiting cheap and nothing else: the segments of a run are real threads, so a
    /// test that has advanced the clock still has to wait for the run to act on it. The poll is short and
    /// the deadline is generous, because what is being waited for is a thread being scheduled rather than
    /// time passing; a test that fails here reports what it was waiting for rather than timing out blankly.
    /// </remarks>
    internal static async Task Reaches(Func<bool> reached, string what, CancellationToken cancellationToken)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(30);

        while (!reached())
        {
            Assert.True(DateTime.UtcNow < deadline, $"The run never reached {what}.");

            await Task.Delay(TimeSpan.FromMilliseconds(2), cancellationToken);
        }
    }
}
