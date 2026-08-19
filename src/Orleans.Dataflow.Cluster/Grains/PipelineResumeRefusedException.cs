namespace Orleans.Dataflow.Grains;

/// <summary>
/// A durable run could not be continued, because what the cluster was asked to continue is not what the
/// checkpoint describes.
/// </summary>
/// <remarks>
/// <para>
/// The resume rule is <b>same document, same revision</b>, and this is that rule refusing. It
/// is a type of its own rather than a rejected start or a lost attempt, because it means something neither
/// of those does: the run exists, its position is on disk, and the thing that cannot continue it is the
/// document it was handed. A caller reads that as "reconcile the document or start a new run", which is a
/// different action from "fix the deployment" and from "the attempt is gone".
/// </para>
/// <para>
/// Two paths reach it and both are the same refusal seen from different sides. A declaration of a run
/// identity that already carries a different document is refused before anything starts, which is where an
/// author who edited a pipeline and kept its run name meets it. A resumed activation whose checkpoint was
/// taken of another fingerprint or another revision is refused at the poll that woke it, which is where a
/// checkpoint written by somebody else meets it.
/// </para>
/// <para>
/// <b>It carries no inner exception</b>, for the reason every refusal this package throws across a grain
/// boundary carries none: Orleans serializes the whole chain, and an inner exception with no codec replaces
/// the diagnosis with a codec error. Both fingerprints are in the message and on the exception, because the
/// message is what a person reads in a log and the properties are what a test asserts on.
/// </para>
/// </remarks>
[GenerateSerializer]
public sealed class PipelineResumeRefusedException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="PipelineResumeRefusedException"/> class.</summary>
    public PipelineResumeRefusedException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PipelineResumeRefusedException"/> class.</summary>
    /// <param name="message">The message describing the refusal.</param>
    public PipelineResumeRefusedException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PipelineResumeRefusedException"/> class.</summary>
    /// <param name="message">The message describing the refusal.</param>
    /// <param name="innerException">The exception that caused this one.</param>
    public PipelineResumeRefusedException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Gets the identity of the document the checkpoint was taken of.</summary>
    /// <value>The canonical text form of its fingerprint, or <see langword="null"/> when none was read.</value>
    [Id(0)]
    public string? StoredFingerprint { get; init; }

    /// <summary>Gets the identity of the document the cluster was asked to continue the run with.</summary>
    /// <value>The canonical text form of its fingerprint, or <see langword="null"/> when none was read.</value>
    [Id(1)]
    public string? DeclaredFingerprint { get; init; }

    /// <summary>Builds the refusal of a document that is not the one a run's checkpoint describes.</summary>
    /// <param name="run">What the run is called.</param>
    /// <param name="stored">The fingerprint the checkpoint or the register holds.</param>
    /// <param name="declared">The fingerprint of the document offered instead.</param>
    /// <returns>The exception.</returns>
    public static PipelineResumeRefusedException Mismatched(string run, string stored, string declared) =>
        new($"The durable run '{run}' belongs to the document {stored} and this is an attempt to continue it with {declared}. A resume continues the very graph its checkpoint describes — the same fingerprint at the same revision only — because a stored position names nodes of the document it was measured in and means nothing in another. No silo will migrate a checkpoint across a changed document. Reconcile the document with the one the run belongs to, run the new one under a run identity of its own, or call ReplaceDurableRunAsync to discard the stored position and start the new document from the beginning under this name.")
        {
            StoredFingerprint = stored,
            DeclaredFingerprint = declared,
        };
}
