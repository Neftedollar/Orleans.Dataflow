using System.Globalization;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow;

/// <summary>
/// A closed graph under a real identity: deployable, addressable across revisions, and free of anything
/// that lives only in the process that authored it.
/// </summary>
/// <remarks>
/// <para>
/// A pipeline definition is what a <see cref="RunnableGraph"/> becomes when it can honestly claim a
/// durable identity. Every stage of it resolves from a catalog and every occurrence carries an
/// author-stable name, which is exactly what the two capability tokens
/// <see cref="CapabilityToken.Nondeployable"/> and <see cref="CapabilityToken.EphemeralIdentity"/> deny,
/// and why <see cref="RunnableGraph.AsPipeline"/> rejects a document that declares either.
/// </para>
/// <para>
/// <see cref="Id"/> and <see cref="Revision"/> are read from <see cref="Document"/> rather than stored
/// beside it, so the pipeline's identity and its document's cannot disagree. <see cref="Fingerprint"/> is
/// the fingerprint of that same re-identified document, which is why it differs from the fingerprint of
/// the anonymous graph it was made from: the identity is document content, and a pipeline's fingerprint is
/// the fingerprint of the deployable document rather than of a placeholder one.
/// </para>
/// <para>
/// Slots of a pipeline bind by fingerprint and by <see cref="Id"/>-plus-revision lineage, without the
/// per-instance authoring nonce a nondeployable graph's slots need (ADR 0004 section 4): registered
/// behavior is in the document, so content identity means something. Materializing a pipeline is the M3
/// Orleans host's concern and nothing here starts anything; this checkpoint produces the document and
/// stops.
/// </para>
/// </remarks>
public sealed class PipelineDefinition
{
    /// <summary>Initializes a new instance of the <see cref="PipelineDefinition"/> class.</summary>
    /// <param name="document">The re-identified, validated document.</param>
    /// <param name="fingerprint">The fingerprint of that document's canonical bytes.</param>
    /// <remarks>
    /// Internal because a pipeline is only ever produced by <see cref="RunnableGraph.AsPipeline"/>, which
    /// is where the deployability rules are enforced. A public constructor would be a way to declare a
    /// pipeline over a document that breaks them.
    /// </remarks>
    internal PipelineDefinition(GraphDocument document, GraphFingerprint fingerprint)
    {
        Document = document;
        Fingerprint = fingerprint;
    }

    /// <summary>Gets the identity of the graph lineage this pipeline belongs to.</summary>
    /// <value>The identity the author gave it, which is also its document's.</value>
    public GraphId Id => Document.Id;

    /// <summary>Gets the revision this pipeline is.</summary>
    /// <value>The revision the author gave it, which is also its document's.</value>
    public GraphRevision Revision => Document.Revision;

    /// <summary>Gets the durable description of this pipeline.</summary>
    /// <value>The closed document, which is the only representation that could ever be persisted.</value>
    public GraphDocument Document { get; }

    /// <summary>Gets the identity of this pipeline's document.</summary>
    /// <value>The SHA-256 fingerprint of the document's canonical bytes.</value>
    public GraphFingerprint Fingerprint { get; }

    /// <summary>Returns a one-line diagnostic summary of this pipeline.</summary>
    /// <returns>
    /// Text of the form <c>pipeline orders@r3 sha256:9f86d081... (3 nodes, 1 result slot)</c>, with each
    /// count singular or plural as it reads.
    /// </returns>
    /// <remarks>The counts are formatted with the invariant culture, and the method never throws.</remarks>
    public override string ToString()
    {
        int nodes = Document.Nodes.Count;
        int slots = Document.ResultSlots.Count;

        return string.Create(
            CultureInfo.InvariantCulture,
            $"pipeline {Id}@r{Revision} {Fingerprint} ({nodes} {(nodes == 1 ? "node" : "nodes")}, {slots} {(slots == 1 ? "result slot" : "result slots")})");
    }
}
