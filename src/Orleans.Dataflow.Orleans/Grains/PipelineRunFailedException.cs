namespace Orleans.Dataflow.Grains;

/// <summary>
/// A run ended because a stage, a source, or a sink threw, reported to a caller on the other side of a
/// grain boundary.
/// </summary>
/// <remarks>
/// <para>
/// The local runtime rethrows the very exception instance the author's code threw. Across a hop that is
/// not possible in general: the exception is the author's own type, and carrying the object would require
/// every failure type in every pipeline to be Orleans-serializable, so a run whose stage threw an
/// unprepared exception would fail to report that it failed. Reporting the type name and the message
/// instead makes every failure reportable. What is lost is the stack, the instance identity, and the
/// ability to catch by the original type; that is stated here rather than implied.
/// </para>
/// <para>
/// The exception itself crosses the boundary — a caller may be reading a run through a coordinator on
/// another silo — so it carries its own serializer annotations.
/// </para>
/// </remarks>
[GenerateSerializer]
public sealed class PipelineRunFailedException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="PipelineRunFailedException"/> class.</summary>
    public PipelineRunFailedException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PipelineRunFailedException"/> class.</summary>
    /// <param name="message">The message describing the failure.</param>
    public PipelineRunFailedException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PipelineRunFailedException"/> class.</summary>
    /// <param name="message">The message describing the failure.</param>
    /// <param name="innerException">The exception that caused this one.</param>
    public PipelineRunFailedException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PipelineRunFailedException"/> class.</summary>
    /// <param name="failureType">The CLR type name of the exception the run failed with.</param>
    /// <param name="failureMessage">The message of the exception the run failed with.</param>
    /// <param name="runId">The identity of the run that failed.</param>
    public PipelineRunFailedException(string? failureType, string? failureMessage, string? runId)
        : base($"The run '{runId}' failed with {failureType ?? "an exception of an unreported type"}: {failureMessage}")
    {
        FailureType = failureType;
        FailureMessage = failureMessage;
        RunId = runId;
    }

    /// <summary>Gets the CLR type name of the exception the run failed with.</summary>
    [Id(0)]
    public string? FailureType { get; init; }

    /// <summary>Gets the message of the exception the run failed with.</summary>
    [Id(1)]
    public string? FailureMessage { get; init; }

    /// <summary>Gets the identity of the run that failed.</summary>
    [Id(2)]
    public string? RunId { get; init; }
}
