namespace Orleans.Dataflow;

/// <summary>
/// The runtime control of one valve of one run: the switch an author flips to hold a stream and to let it
/// go again.
/// </summary>
/// <remarks>
/// <para>
/// Resolved by name from <see cref="RunHandle.GetValueAsync{TResult}"/> like every other control, and
/// available as soon as the run exists rather than when it ends — a control is a thing an author uses
/// <i>while</i> the run is running, which is what separates it from a result.
/// </para>
/// <para>
/// Flipping is immediate and never waits: what waits is the run. Closing a valve holds the element the
/// stage has in its hand and backpressures everything above it, exactly as a full buffer does; nothing is
/// dropped and nothing is buffered, because a valve has no capacity of its own — the elements accumulate in
/// whatever boundaries the author declared above it, under the policies they declared there.
/// </para>
/// <para>
/// A valve of a run that has stopped is inert: the run's own stop releases the element it was holding and
/// no later flip does anything, because there is no longer a stream to hold. Every member is safe to call
/// from any thread at any point in the run's life, including before the first element and after the last.
/// </para>
/// </remarks>
public interface IValve
{
    /// <summary>Gets a value indicating whether elements are passing through.</summary>
    /// <value><see langword="true"/> when the valve is open.</value>
    /// <remarks>
    /// Observational and best-effort, exactly as <see cref="RunHandle.IsPaused"/> is: it answers for a
    /// moment that may already have passed by the time a caller acts on it.
    /// </remarks>
    bool IsOpen { get; }

    /// <summary>Lets elements through, releasing whatever the valve was holding.</summary>
    /// <remarks>Idempotent: opening an open valve changes nothing.</remarks>
    void Open();

    /// <summary>Holds elements at the valve.</summary>
    /// <remarks>
    /// Idempotent, and it takes effect at the next element rather than retroactively: an element that has
    /// already passed the valve is downstream and is not called back.
    /// </remarks>
    void Close();
}
