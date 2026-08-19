using Orleans.Dataflow.Definition;

namespace Orleans.Dataflow.Serialization;

/// <summary>
/// Converts a <see cref="StageCatalog"/> to its canonical byte form and to the identity of those bytes.
/// </summary>
/// <remarks>
/// <para>
/// One catalog has exactly one byte form. That is what makes <see cref="Fingerprint"/> an
/// identity rather than a checksum, what lets a golden fixture pin compatibility, and what will let two
/// silos agree that they resolve stages the same way by exchanging 32 bytes instead of a catalog.
/// </para>
/// <para>
/// <c>Serialize</c> is a function of the catalog alone. Registration order, hash-table iteration order,
/// culture, runtime version, and process identity do not appear in the output, because
/// <see cref="StageCatalog.Create"/> and the <see cref="StageSpecification"/> factory have already put
/// every collection in canonical order and every number is written in the invariant culture.
/// </para>
/// <para>
/// There is deliberately no reader: this type serializes a catalog and never decodes one. A catalog is
/// registered by deployment code that holds the specifications already, so nothing needs to decode one,
/// and a reader belongs with cross-silo catalog negotiation, together with the strictness rules and the
/// format diagnostics that would make decoding worth having. Serializing without a reader is honest
/// about that: the bytes exist to be compared and digested, not yet to be parsed back.
/// </para>
/// <para>
/// A parameter validator is behavior and is never serialized. Two catalogs whose specifications agree but
/// whose validators differ produce identical bytes and share a fingerprint. What a validator accepts is a
/// property of the deployment that registered it, not of the stage contract the catalog publishes, and
/// this limit is stated rather than hidden.
/// </para>
/// <para>
/// The envelope is canonical JSON with fixed schema property order and is deliberately not a
/// <see cref="CanonicalJsonValue"/>, which sorts object keys. The two canonical disciplines are
/// documented separately and never mix.
/// </para>
/// </remarks>
public static class StageCatalogSerializer
{
    /// <summary>
    /// The catalog envelope format version this library writes.
    /// </summary>
    /// <remarks>
    /// The version is a property of the envelope, not of the catalog: a <see cref="StageCatalog"/> holds
    /// specifications and no version number, and every catalog serialized by this library is written
    /// under this version. It is numbered independently of
    /// <see cref="GraphDocument.CurrentFormatVersion"/>, because a catalog format and a document format
    /// are allowed to evolve at different times.
    /// </remarks>
    public const int CurrentFormatVersion = 1;

    /// <summary>
    /// Serializes a catalog to its canonical byte form.
    /// </summary>
    /// <param name="catalog">The catalog to serialize.</param>
    /// <returns>
    /// A fresh array holding minified UTF-8 without a byte order mark, owned by the caller.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="catalog"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// Collections are written in the order the catalog and its specifications store them. The model is
    /// the single authority on canonical order, and re-sorting or re-checking the order here would create
    /// a second opinion about it.
    /// </remarks>
    public static byte[] Serialize(StageCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(catalog);

        return CatalogEnvelopeWriter.Write(catalog, CurrentFormatVersion);
    }

    /// <summary>
    /// Computes the identity of a catalog's canonical byte form.
    /// </summary>
    /// <param name="catalog">The catalog to fingerprint.</param>
    /// <returns>The SHA-256 of <see cref="Serialize"/> applied to <paramref name="catalog"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="catalog"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The fingerprint is defined over bytes, so it is computed by serializing rather than by walking the
    /// object graph. A digest taken over object state would have to restate the canonical rules a second
    /// time and could drift from them.
    /// </remarks>
    public static CatalogFingerprint Fingerprint(StageCatalog catalog) =>
        CatalogFingerprint.OfSerialized(Serialize(catalog));
}
