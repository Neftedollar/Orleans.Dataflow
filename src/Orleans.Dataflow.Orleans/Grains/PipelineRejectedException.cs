namespace Orleans.Dataflow.Grains;

/// <summary>
/// A silo refused a document before starting anything from it.
/// </summary>
/// <remarks>
/// <para>
/// The refusal envelope, and it is an exception rather than a returned failure state on purpose. A start
/// either produces a ticket or produces nothing: there is no partially started run, no identity to hand
/// back, and nothing a caller could poll. Returning a union would make every caller unpack a value whose
/// failure branch carries no run — and would let a caller ignore the failure and address a run that does
/// not exist. The exception makes the refusal the only path.
/// </para>
/// <para>
/// Three things reach a caller through it, and the message says which: the bytes are not a canonical graph
/// document; the document is for a different pipeline than the coordinator addressed; or the document does
/// not validate against this silo's catalog, in which case every compiler diagnostic is in the message
/// rather than only the first. A rolling upgrade that removed a stage produces the third, which is exactly
/// the case where a caller needs the whole report.
/// </para>
/// <para>
/// The message carries the diagnostics as text. That is what survives a hop: a validation report is a
/// model of this library's own and serializing it across the boundary would publish a wire format for
/// diagnostics that phase 1 has no need to pin.
/// </para>
/// <para>
/// <b>A refusal thrown by a grain never carries an inner exception</b>, and the rule is load-bearing
/// rather than stylistic. Orleans serializes an exception's whole chain, so an inner exception of a type
/// with no codec — <c>GraphDocumentFormatException</c>, for one — makes the refusal itself unserializable
/// and the caller receives a codec error instead of the diagnosis. Everything worth reading is folded
/// into the message before it is thrown, which is why the message is composed rather than borrowed.
/// </para>
/// </remarks>
[GenerateSerializer]
public sealed class PipelineRejectedException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="PipelineRejectedException"/> class.</summary>
    public PipelineRejectedException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PipelineRejectedException"/> class.</summary>
    /// <param name="message">The message describing the refusal.</param>
    public PipelineRejectedException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PipelineRejectedException"/> class.</summary>
    /// <param name="message">The message describing the refusal.</param>
    /// <param name="innerException">The exception that caused this one.</param>
    public PipelineRejectedException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
