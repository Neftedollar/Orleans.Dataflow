namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One valve of one run: the gate its stage waits at, and the switch its author flips.
/// </summary>
/// <remarks>
/// <para>
/// The same shape every control of this runtime has — one object per materialization, handed to the author
/// through a named slot as soon as the run exists — and the simplest of them, because a valve has no
/// element type: the runtime object <i>is</i> the control, so there is no facade to build and nothing about
/// the author's types to recover.
/// </para>
/// <para>
/// The gate is a task rather than a flag, so that a closed valve costs a waiting thread and no processor
/// time at all, and so that opening it is one <c>TrySetResult</c> from whatever thread the author called on.
/// A fresh task is made on every close, which is what lets a valve be flipped any number of times in one
/// run; the continuations are asynchronous, so an author's <see cref="Open"/> never runs a run's work on
/// their own thread.
/// </para>
/// </remarks>
internal sealed class LocalValve : IValve
{
    private readonly Lock _gate = new();
    private TaskCompletionSource _open;

    /// <summary>Initializes a new instance of the <see cref="LocalValve"/> class.</summary>
    /// <param name="mode">The state this run's valve starts in, as its document declares.</param>
    internal LocalValve(ValveMode mode)
    {
        _open = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        if (mode is ValveMode.Open)
        {
            _open.SetResult();
        }
    }

    /// <inheritdoc/>
    public bool IsOpen
    {
        get
        {
            lock (_gate)
            {
                return _open.Task.IsCompleted;
            }
        }
    }

    /// <summary>Gets the task that completes when this valve is open.</summary>
    /// <value>An already-completed task while the valve is open, and a pending one while it is closed.</value>
    /// <remarks>
    /// Read by the stage and by nothing else. It is read under the lock and awaited outside it, so a close
    /// arriving between the two makes the stage wait for the <i>next</i> opening rather than deadlocking on
    /// a task nobody holds.
    /// </remarks>
    internal Task Opened
    {
        get
        {
            lock (_gate)
            {
                return _open.Task;
            }
        }
    }

    /// <inheritdoc/>
    public void Open()
    {
        lock (_gate)
        {
            _ = _open.TrySetResult();
        }
    }

    /// <inheritdoc/>
    public void Close()
    {
        lock (_gate)
        {
            if (_open.Task.IsCompleted)
            {
                _open = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }
    }

    /// <summary>Returns a one-line diagnostic summary of this valve.</summary>
    /// <returns>The literal <c>valve (open)</c> or <c>valve (closed)</c>.</returns>
    /// <remarks>Never throws, and answers for a moment that may already have passed.</remarks>
    public override string ToString() => IsOpen ? "valve (open)" : "valve (closed)";
}
