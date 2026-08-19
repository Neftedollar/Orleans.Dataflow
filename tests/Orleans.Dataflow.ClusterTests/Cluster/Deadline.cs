using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// The bound every wait in the failover tests carries, so that a thing that never happens fails one test
/// with a sentence rather than hanging the suite.
/// </summary>
/// <remarks>
/// <para>
/// The same reasoning as <c>TestSignals</c>'s budget and <c>Poll</c>'s turn limit, applied to the one wait
/// neither of them covers: a run's completion. A handle polls until the run reaches a terminal state, and
/// a run that never reaches one — because a silo died at the wrong moment, or because a drain stalled —
/// makes that an await with no end. Unbounded is fine in production, where somebody is watching; in a
/// suite it turns a regression into a timeout somewhere else, minutes later, with the diagnosis discarded.
/// </para>
/// <para>
/// A minute, chosen the way the signal budget was: dozens of times longer than the slowest legitimate wait
/// here, which is a graceful drain of a handful of elements, and short enough that a hang is reported
/// while a person is still looking.
/// </para>
/// </remarks>
internal static class Deadline
{
    /// <summary>The longest any wait in these tests lasts before it reports that nothing happened.</summary>
    internal static readonly TimeSpan Budget = TimeSpan.FromMinutes(1);

    /// <summary>Awaits a task under the budget, naming what was expected if it does not arrive.</summary>
    /// <param name="work">The task to wait for.</param>
    /// <param name="expectation">What the caller is waiting for, for the message if it never happens.</param>
    /// <returns>A task that completes when <paramref name="work"/> does.</returns>
    /// <exception cref="TimeoutException"><paramref name="work"/> did not settle within the budget.</exception>
    /// <remarks>
    /// A task that has already faulted settles the wait immediately with its own exception, so wrapping a
    /// wait in this changes nothing about which exception a test observes — which is what lets an assertion
    /// about the exception a run ended with be written around it.
    /// </remarks>
    internal static async Task Within(Task work, string expectation)
    {
        ArgumentNullException.ThrowIfNull(work);

        try
        {
            await work.WaitAsync(Budget, TestContext.Current.CancellationToken);
        }
        catch (TimeoutException)
        {
            throw new TimeoutException(
                $"Waited {Budget} and {expectation} never happened. This wait is reporting that, not causing it.");
        }
    }

    /// <summary>Awaits a task under the budget and hands back what it produced.</summary>
    /// <typeparam name="T">What the task answers with.</typeparam>
    /// <param name="work">The task to wait for.</param>
    /// <param name="expectation">What the caller is waiting for, for the message if it never arrives.</param>
    /// <returns>The task's own value.</returns>
    /// <exception cref="TimeoutException"><paramref name="work"/> did not settle within the budget.</exception>
    /// <remarks>
    /// The same wait with an answer, for the calls that have one. It exists because a grain call that
    /// deadlocks is indistinguishable from a slow one until something puts a bound on it, and a test about
    /// two grains not waiting for each other is exactly the kind that must fail rather than hang.
    /// </remarks>
    internal static async Task<T> Within<T>(Task<T> work, string expectation)
    {
        ArgumentNullException.ThrowIfNull(work);

        try
        {
            return await work.WaitAsync(Budget, TestContext.Current.CancellationToken);
        }
        catch (TimeoutException)
        {
            throw new TimeoutException(
                $"Waited {Budget} and {expectation} never happened. This wait is reporting that, not causing it.");
        }
    }
}
