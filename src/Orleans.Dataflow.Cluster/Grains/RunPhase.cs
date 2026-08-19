namespace Orleans.Dataflow.Grains;

/// <summary>
/// Where one run of a pipeline is: not started, running, or ended in one of the three ways a run ends.
/// </summary>
/// <remarks>
/// <para>
/// The three terminal values are the local engine's three outcomes, unchanged by the network in between:
/// a run that reached the end of its stream or was gracefully shut down is <see cref="Completed"/>, a run
/// whose stage or source threw is <see cref="Faulted"/>, and a run that was cancelled is
/// <see cref="Canceled"/>. That shutdown lands on <see cref="Completed"/> rather than on a state of its
/// own is the drain contract stated in the vocabulary: a graceful stop ends the stream, it does not
/// abandon it.
/// </para>
/// <para>
/// <see cref="NotStarted"/> means two different things and says so honestly: a run grain that has not been
/// told to start yet, and a run grain that was deactivated while running and has come back empty. An
/// ordinary run is not resumed across a deactivation, so the second case is the loss of that attempt, and a
/// client that saw <see cref="Running"/> and then sees <see cref="NotStarted"/> reports the loss rather
/// than waiting forever for a run that no longer exists.
/// </para>
/// </remarks>
[GenerateSerializer]
public enum RunPhase
{
    /// <summary>No run is active in the grain addressed.</summary>
    NotStarted = 0,

    /// <summary>The run is executing and has not reached a terminal state.</summary>
    Running = 1,

    /// <summary>The stream ended, whether it ran out or was gracefully drained, and results are settled.</summary>
    Completed = 2,

    /// <summary>A stage, a source, or a sink threw, and the exception is what ended the run.</summary>
    Faulted = 3,

    /// <summary>The run was cancelled; nothing it declared resolves.</summary>
    Canceled = 4,
}
