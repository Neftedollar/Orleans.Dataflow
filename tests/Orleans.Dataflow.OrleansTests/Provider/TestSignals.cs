using System.Collections.Concurrent;

namespace Orleans.Dataflow.OrleansTests.Provider;

/// <summary>
/// The rendezvous a test uses to know that a run has got somewhere, without asking it and without waiting
/// for a length of time.
/// </summary>
/// <remarks>
/// <para>
/// A test that wants to stop a run partway has to know the run has produced something first, and the two
/// honest ways to know that are to ask the run or to have the run say so. Sleeping is neither: it makes a
/// test that passes on a fast machine and fails on a loaded one, and it makes the number it asserts a
/// guess. The source raises a signal after its last element and the test awaits that signal, so what the
/// run had produced when the test acted is a fact rather than a probability.
/// </para>
/// <para>
/// Static because the cluster is in-process: a silo and the test share a process, so a static table is the
/// simplest thing that is also true. It would be a lie in a multi-process cluster, which is one more reason
/// this lives in the test project and not in a shipped package.
/// </para>
/// </remarks>
internal static class TestSignals
{
    private static readonly ConcurrentDictionary<string, TaskCompletionSource> Raised = new(StringComparer.Ordinal);

    /// <summary>Raises one signal, releasing everyone waiting for it.</summary>
    /// <param name="name">The signal's name.</param>
    /// <remarks>Raising a signal twice is not an error; the second raise releases nobody new.</remarks>
    internal static void Raise(string name) => Source(name).TrySetResult();

    /// <summary>Waits until one signal has been raised.</summary>
    /// <param name="name">The signal's name.</param>
    /// <returns>A task that completes when the signal is raised, and at once when it already was.</returns>
    internal static Task Reached(string name) => Source(name).Task;

    /// <summary>Gets the completion source behind one signal, creating it on first mention.</summary>
    /// <param name="name">The signal's name.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// Created on first mention from either side, so a test may wait for a signal before the run that
    /// raises it has started. The continuations run asynchronously, so a raise from inside a run's own
    /// thread never runs the test's continuation on that thread.
    /// </remarks>
    private static TaskCompletionSource Source(string name) =>
        Raised.GetOrAdd(name, static _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
}
