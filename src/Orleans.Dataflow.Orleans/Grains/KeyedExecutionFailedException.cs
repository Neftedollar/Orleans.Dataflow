namespace Orleans.Dataflow.Grains;

/// <summary>
/// A distributed keyed call failed inside its executor, reported to the run on the other side of the hop.
/// </summary>
/// <remarks>
/// <para>
/// The same trade <see cref="PipelineRunFailedException"/> makes, one hop earlier and for the same reason.
/// A registered keyed call throws the author's own exception type, and carrying that object across a grain
/// boundary would require every failure type in every pipeline to be Orleans-serializable — so a keyed
/// stage whose grain threw an unprepared exception would fail to report that it had failed, replacing the
/// diagnosis with a codec error. The type name and the message travel as text instead, and every failure is
/// therefore reportable. What is lost is the stack, the instance, and catching by the original type; that is
/// stated here rather than implied, and it is the one behavioural difference between running a keyed stage
/// distributed and running it inside the run.
/// </para>
/// <para>
/// No inner exception is attached, per the wire discipline every grain-thrown refusal in this package
/// follows: Orleans serializes an exception's whole chain, and a chain is only as serializable as its least
/// prepared link.
/// </para>
/// <para>
/// The executor's own address is carried because it is the one piece of context nothing downstream can
/// reconstruct. It is the key of the grain that ran the call — <c>{graph}/{run}/{node}/{key}</c> — so a
/// failure names the run, the occurrence, and the partition that produced it rather than saying only that
/// something keyed went wrong.
/// </para>
/// </remarks>
[GenerateSerializer]
internal sealed class KeyedExecutionFailedException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="KeyedExecutionFailedException"/> class.</summary>
    public KeyedExecutionFailedException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="KeyedExecutionFailedException"/> class.</summary>
    /// <param name="message">The message describing the failure.</param>
    public KeyedExecutionFailedException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="KeyedExecutionFailedException"/> class.</summary>
    /// <param name="message">The message describing the failure.</param>
    /// <param name="innerException">The exception that caused this one.</param>
    public KeyedExecutionFailedException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="KeyedExecutionFailedException"/> class.</summary>
    /// <param name="executor">The key of the executor grain that ran the call.</param>
    /// <param name="call">The name of the registered call that threw.</param>
    /// <param name="failureType">The CLR type name of the exception the call threw.</param>
    /// <param name="failureMessage">The message of the exception the call threw.</param>
    public KeyedExecutionFailedException(
        string? executor,
        string? call,
        string? failureType,
        string? failureMessage)
        : base($"The keyed call '{call}' failed in the executor '{executor}' with {failureType ?? "an exception of an unreported type"}: {failureMessage}")
    {
        Executor = executor;
        Call = call;
        FailureType = failureType;
        FailureMessage = failureMessage;
    }

    /// <summary>Gets the key of the executor grain that ran the call.</summary>
    [Id(0)]
    public string? Executor { get; init; }

    /// <summary>Gets the name of the registered call that threw.</summary>
    [Id(1)]
    public string? Call { get; init; }

    /// <summary>Gets the CLR type name of the exception the call threw.</summary>
    [Id(2)]
    public string? FailureType { get; init; }

    /// <summary>Gets the message of the exception the call threw.</summary>
    [Id(3)]
    public string? FailureMessage { get; init; }
}
