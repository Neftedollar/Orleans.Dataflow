namespace Orleans.Dataflow.Grains;

/// <summary>
/// What a coordinator hands back when it accepts a pipeline and starts a run of it.
/// </summary>
/// <remarks>
/// <para>
/// A ticket is the whole of what a client needs to address the run afterwards: which run it is, which
/// ownership epoch its control calls must carry, and — as a record rather than as an instruction — which
/// document and which vocabulary the silo accepted. The two fingerprints travel as text because that is
/// what they are on the wire; nothing on this type is an Orleans-serialized identity value, which is
/// deliberate, since an identity is a compile-time concept of the authoring side and a string is what
/// survives a hop.
/// </para>
/// <para>
/// <see cref="Epoch"/> is assigned by the coordinator and is monotonic within one pipeline. Every control
/// call to the run carries it, and the run rejects any other value loudly: a caller holding a ticket from
/// before some other start is holding a stale claim to ownership, and finding that out at the call is the
/// entire point of an epoch.
/// </para>
/// </remarks>
[GenerateSerializer]
public sealed class PipelineRunTicket
{
    /// <summary>Gets or sets the identity of the graph lineage the run belongs to.</summary>
    /// <value>The pipeline's graph identifier, as text.</value>
    [Id(0)]
    public string GraphId { get; set; } = string.Empty;

    /// <summary>Gets or sets the identity of this run.</summary>
    /// <value>The run identifier the coordinator assigned, as text.</value>
    [Id(1)]
    public string RunId { get; set; } = string.Empty;

    /// <summary>Gets or sets the ownership epoch every control call to this run must carry.</summary>
    /// <value>A positive number, monotonic within the pipeline.</value>
    [Id(2)]
    public long Epoch { get; set; }

    /// <summary>Gets or sets the identity of the document the silo accepted.</summary>
    /// <value>The canonical text form of the document's fingerprint, such as <c>sha256:9f86d081…</c>.</value>
    /// <remarks>
    /// Recorded so that a client can assert the bytes it sent are the bytes the silo read. That the two
    /// agree is what makes a fingerprint an identity rather than a checksum of something that happened to
    /// travel intact.
    /// </remarks>
    [Id(3)]
    public string GraphFingerprint { get; set; } = string.Empty;

    /// <summary>Gets or sets the identity of the catalog the silo validated the document against.</summary>
    /// <value>The canonical text form of the catalog's fingerprint.</value>
    /// <remarks>
    /// A record of which vocabulary accepted the run, which is the one cross-silo fact the definition
    /// plane cannot check on its own. It is reported and not enforced: a client that cares which catalog
    /// its runs land on compares this value across runs itself.
    /// </remarks>
    [Id(4)]
    public string CatalogFingerprint { get; set; } = string.Empty;
}
