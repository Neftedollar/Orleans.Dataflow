namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// A one-shot hold a test puts inside a stage, so that a run can be stopped at a known point and released
/// again without any test ever waiting on a clock.
/// </summary>
/// <remarks>
/// <para>
/// Every runtime test that needs a run "held in the middle" uses this, and none uses a delay. A delay
/// would assert that something had not happened yet by hoping it had not happened yet; the gate makes the
/// same claim a fact: <see cref="Reached"/> completes only once the run is inside the stage, and the run
/// stays there until <see cref="Open"/> is called.
/// </para>
/// <para>
/// <see cref="Wait"/> blocks the calling thread on purpose. A local stage is a synchronous author
/// delegate, so a stage that takes a long time is a stage that blocks, and holding the run any other way
/// would be testing something the runtime does not do. It blocks the run's own dedicated thread and no
/// other.
/// </para>
/// <para>
/// One-shot: once opened, the gate never holds again, so a test can put it in a folder that runs for every
/// element and still hold only the first one.
/// </para>
/// </remarks>
internal sealed class Gate
{
    private readonly TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _open = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Gets the task that completes when a run first reaches this gate.</summary>
    internal Task Reached => _reached.Task;

    /// <summary>Holds the calling thread until the gate is opened.</summary>
    /// <remarks>Returns immediately once <see cref="Open"/> has been called, however many times it is called after that.</remarks>
    internal void Wait()
    {
        _reached.TrySetResult();
        _open.Task.GetAwaiter().GetResult();
    }

    /// <summary>Opens the gate, releasing whoever is held and everyone after them.</summary>
    internal void Open() => _open.TrySetResult();
}
