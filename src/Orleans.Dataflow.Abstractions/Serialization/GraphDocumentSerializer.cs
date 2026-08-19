using Orleans.Dataflow.Definition;

namespace Orleans.Dataflow.Serialization;

/// <summary>
/// Converts a <see cref="GraphDocument"/> to and from its canonical byte form.
/// </summary>
/// <remarks>
/// <para>
/// One document has exactly one byte form and one byte form decodes to exactly one document.
/// That is what makes <see cref="Fingerprint"/> an identity rather than a checksum, what makes golden
/// fixtures able to pin compatibility, and what lets two silos compare documents by digest without
/// exchanging them.
/// </para>
/// <para>
/// The two laws this type upholds, in both directions:
/// </para>
/// <list type="bullet">
/// <item>
/// <description>
/// <c>Serialize</c> is a function of the document alone. Construction order, hash-table iteration order,
/// culture, runtime version, and process identity do not appear in the output, because
/// <see cref="GraphDocument.Create"/> has already put every collection in canonical order and every
/// number is written in the invariant culture.
/// </description>
/// </item>
/// <item>
/// <description>
/// <c>Serialize(Deserialize(bytes))</c> is byte-identical to <c>bytes</c> for every input
/// <c>Deserialize</c> accepts. The reader is strict for exactly this reason: bytes that are merely
/// equivalent JSON would decode to a document whose serialization is a different byte string, and a
/// document with two byte forms has two identities.
/// </description>
/// </item>
/// </list>
/// <para>
/// The envelope is canonical JSON with fixed schema property order and is deliberately not a
/// <see cref="CanonicalJsonValue"/>, which sorts object keys. The embedded payloads are canonical values
/// and keep their own ordinal key order. The two disciplines are documented separately and never mix.
/// </para>
/// </remarks>
public static class GraphDocumentSerializer
{
    /// <summary>
    /// Serializes a document to its canonical byte form.
    /// </summary>
    /// <param name="document">The document to serialize.</param>
    /// <returns>
    /// A fresh array holding minified UTF-8 without a byte order mark, owned by the caller.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="document"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// A stage node carries the JSON null value as its parameter or execution policy payload. Format
    /// version 1 spells an absent execution policy as the literal <c>null</c> at a payload position, so
    /// it has no byte form for a payload that is itself null; the document is refused rather than written
    /// as bytes <see cref="Deserialize"/> would reject. The message names the node and the member.
    /// </exception>
    /// <remarks>
    /// Collections are written in the order the document stores them.
    /// <see cref="GraphDocument.Create"/> is the single authority on canonical order, and re-sorting or
    /// re-checking the order here would create a second opinion about it.
    /// </remarks>
    public static byte[] Serialize(GraphDocument document)
    {
        ArgumentNullException.ThrowIfNull(document);

        return GraphEnvelopeWriter.Write(document);
    }

    /// <summary>
    /// Deserializes a document from its canonical byte form.
    /// </summary>
    /// <param name="canonicalEnvelope">The bytes to read.</param>
    /// <returns>The decoded, structurally valid document.</returns>
    /// <exception cref="GraphDocumentFormatException">
    /// <paramref name="canonicalEnvelope"/> is not the canonical serialization of a graph document. The
    /// message names what was found, the JSON path it was found at, and the rule that rejects it.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The reader accepts exactly the bytes <see cref="Serialize"/> produces for some document and
    /// nothing else. A byte order mark, insignificant whitespace, a missing, unknown, or reordered
    /// property, a wrong JSON type, an omitted optional value, a non-minimal number, an escaped
    /// identifier, a payload that is not in canonical form, a collection out of canonical order, and any
    /// trailing content are all rejected rather than normalized.
    /// </para>
    /// <para>
    /// An unknown format version is rejected before every other rule and with no mention of any other
    /// property, because a document from a later version may be entirely well formed under its own rules.
    /// </para>
    /// <para>
    /// The decoded values are rebuilt through the model's own factories, so every structural invariant is
    /// re-enforced and bytes that were hand-edited into an impossible shape fail on the same invariant an
    /// authored document would fail on.
    /// </para>
    /// </remarks>
    public static GraphDocument Deserialize(ReadOnlySpan<byte> canonicalEnvelope)
    {
        GraphDocument document = GraphEnvelopeReader.Read(canonicalEnvelope);

        EnsureRoundTrips(document, canonicalEnvelope);

        return document;
    }

    /// <summary>
    /// Computes the identity of a document's canonical byte form.
    /// </summary>
    /// <param name="document">The document to fingerprint.</param>
    /// <returns>The SHA-256 of <see cref="Serialize"/> applied to <paramref name="document"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="document"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The fingerprint is defined over bytes, so it is computed by serializing rather than by walking the
    /// object graph. A digest taken over object state would have to restate the canonical rules a second
    /// time and could drift from them.
    /// </remarks>
    public static GraphFingerprint Fingerprint(GraphDocument document) =>
        GraphFingerprint.OfSerialized(Serialize(document));

    /// <summary>
    /// Enforces that the decoded document serializes back to exactly the bytes it was decoded from.
    /// </summary>
    /// <param name="document">The decoded document.</param>
    /// <param name="canonicalEnvelope">The bytes it was decoded from.</param>
    /// <exception cref="GraphDocumentFormatException">The bytes are not the document's canonical form.</exception>
    /// <remarks>
    /// The field-level rules of the reader are meant to accept exactly the writer's output, and each of
    /// them reports precisely what it rejected. This check states the law those rules serve, directly:
    /// whatever the rules happen to cover, an accepted byte string is the one canonical form of the
    /// document it decodes to. It is a guard rather than a diagnostic, so it says only that, and it
    /// cannot reject a canonical document, because a canonical document is by definition what the writer
    /// emits.
    /// </remarks>
    private static void EnsureRoundTrips(GraphDocument document, ReadOnlySpan<byte> canonicalEnvelope)
    {
        if (!GraphEnvelopeWriter.Write(document).AsSpan().SequenceEqual(canonicalEnvelope))
        {
            throw GraphEnvelopeSchema.Violation(
                GraphEnvelopeSchema.RootPath,
                "the bytes decode to a document whose canonical form is a different byte string, so they are not that document's canonical form");
        }
    }
}
