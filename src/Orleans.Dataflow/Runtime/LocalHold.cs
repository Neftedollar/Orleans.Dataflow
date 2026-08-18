namespace Orleans.Dataflow.Runtime;

/// <summary>
/// Who is holding a run at its safe points.
/// </summary>
/// <remarks>
/// A pause has exactly two callers and they mean different things by it, which is why the gate has to know
/// which of them is asking: an author stops the run and decides when it goes again, and a checkpoint stops
/// it for as long as one snapshot takes. Without the distinction a capture finishing would resume a run its
/// author had paused, which is a run moving when somebody was told it would not.
/// </remarks>
internal enum LocalHold
{
    /// <summary>The author, through <see cref="RunHandle.PauseAsync"/>.</summary>
    Author,

    /// <summary>The run's own capture loop, for the duration of one checkpoint.</summary>
    Checkpoint,
}
