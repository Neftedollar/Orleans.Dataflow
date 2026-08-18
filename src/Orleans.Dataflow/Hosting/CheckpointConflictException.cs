using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// The failure a checkpoint store raises when the writer presenting an ETag is no longer the one the store
/// belongs to.
/// </summary>
/// <remarks>
/// <para>
/// The coordinator store's consequence, generalized with it: a superseded writer's write fails, and the
/// failure kills the stale attempt rather than corrupting the fresh one (ADR 0007). A type of its own
/// rather than a general-purpose exception, for the reason every other named failure in this package has
/// one — a caller that wants to tell "I have been fenced out" apart from "the store is unreachable" has to
/// be able to write the <c>catch</c>, and the two answers are opposites: the first means stop, the second
/// means try again.
/// </para>
/// <para>
/// A run whose capture is refused this way fails with this exception. It is deliberately not swallowed and
/// deliberately not retried: retrying with the fresh ETag would overwrite a live attempt's truth with a
/// snapshot of a run that owns nothing, which is exactly the corruption the ETag exists to prevent.
/// </para>
/// </remarks>
public sealed class CheckpointConflictException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="CheckpointConflictException"/> class.</summary>
    public CheckpointConflictException()
        : base("A checkpoint write presented an ETag the store no longer holds, so this writer has been superseded.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CheckpointConflictException"/> class.</summary>
    /// <param name="message">The message that describes the conflict.</param>
    public CheckpointConflictException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CheckpointConflictException"/> class.</summary>
    /// <param name="message">The message that describes the conflict.</param>
    /// <param name="innerException">The exception that caused this one.</param>
    public CheckpointConflictException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Gets the ETag the refused writer presented.</summary>
    /// <value>The text, or <see langword="null"/> when the writer believed the store held nothing.</value>
    public string? Presented { get; private init; }

    /// <summary>Gets the ETag the store actually holds.</summary>
    /// <value>The text, or <see langword="null"/> when the store holds nothing for the pair.</value>
    public string? Stored { get; private init; }

    /// <summary>Builds the exception a store raises when a presented ETag is not the one it holds.</summary>
    /// <param name="graph">The graph identity of the refused write.</param>
    /// <param name="run">The run identity of the refused write.</param>
    /// <param name="presented">What the writer presented, or <see langword="null"/> for a first write.</param>
    /// <param name="stored">What the store holds, or <see langword="null"/> when it holds nothing.</param>
    /// <returns>The exception.</returns>
    /// <remarks>
    /// Both ETags are in the message and on the exception, because the two answer different questions: the
    /// message is what a person reads in a log, and the properties are what a test asserts on. "None" is
    /// spelled out rather than left blank, so a first write racing a first write reads as what it is.
    /// </remarks>
    public static CheckpointConflictException Superseded(
        GraphId graph,
        RunId run,
        string? presented,
        string? stored) =>
        new($"The checkpoint write for the run '{run}' of the graph '{graph}' presents the ETag '{presented ?? "<none>"}' and the store holds '{stored ?? "<none>"}'. Somebody else wrote this checkpoint after this writer read it, so this attempt no longer owns the run and its snapshot is refused rather than applied.")
        {
            Presented = presented,
            Stored = stored,
        };
}
