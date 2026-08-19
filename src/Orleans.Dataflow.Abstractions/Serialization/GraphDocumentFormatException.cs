namespace Orleans.Dataflow.Serialization;

/// <summary>
/// The error raised when bytes are not the canonical serialization of a graph document.
/// </summary>
/// <remarks>
/// <para>
/// A graph document has exactly one canonical byte form, so a reader accepts exactly the bytes
/// the writer produces and rejects everything else. This exception is that rejection, and it is the only
/// one <see cref="GraphDocumentSerializer.Deserialize"/> raises for input it will not accept: a malformed
/// or non-canonical document never surfaces as a raw parser error, and it is never repaired on a
/// best-effort basis.
/// </para>
/// <para>
/// The message names what was found, the JSON path it was found at, such as
/// <c>$.nodes[2].stageRef.majorVersion</c>, and the rule that rejects it. When the rejection originates
/// in a lower layer, such as the payload canonicalizer or a structural invariant of the document model,
/// the original error is carried as <see cref="Exception.InnerException"/> rather than being flattened
/// into text.
/// </para>
/// <para>
/// The type carries no custom state, so it needs no serialization surface of its own.
/// </para>
/// </remarks>
public sealed class GraphDocumentFormatException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="GraphDocumentFormatException"/> class.
    /// </summary>
    public GraphDocumentFormatException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="GraphDocumentFormatException"/> class.
    /// </summary>
    /// <param name="message">The message naming what was found, where, and which rule rejects it.</param>
    public GraphDocumentFormatException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="GraphDocumentFormatException"/> class.
    /// </summary>
    /// <param name="message">The message naming what was found, where, and which rule rejects it.</param>
    /// <param name="innerException">The lower-layer error that caused this rejection.</param>
    public GraphDocumentFormatException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
