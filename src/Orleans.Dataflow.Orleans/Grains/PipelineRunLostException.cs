namespace Orleans.Dataflow.Grains;

/// <summary>
/// A run that was executing is gone, because the activation hosting it was recycled.
/// </summary>
/// <remarks>
/// <para>
/// The honest phase-1 answer to a deactivation mid-run. A run grain holds its engine in memory and
/// persists nothing about the run's progress, so an activation that goes away takes the attempt with it;
/// the fresh activation has no run and reports <see cref="RunPhase.NotStarted"/>. A client that had
/// already seen the run executing translates that into this exception rather than waiting forever for a
/// terminal state that will never arrive.
/// </para>
/// <para>
/// This is a lost attempt and not a failed pipeline. Nothing here retries and nothing here resumes:
/// durable resume is the checkpoint work of a later milestone, and promising it now by quietly restarting
/// would produce a second execution of every side effect the lost attempt had already performed.
/// </para>
/// </remarks>
[GenerateSerializer]
public sealed class PipelineRunLostException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="PipelineRunLostException"/> class.</summary>
    public PipelineRunLostException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PipelineRunLostException"/> class.</summary>
    /// <param name="message">The message describing the loss.</param>
    public PipelineRunLostException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PipelineRunLostException"/> class.</summary>
    /// <param name="message">The message describing the loss.</param>
    /// <param name="innerException">The exception that caused this one.</param>
    public PipelineRunLostException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
