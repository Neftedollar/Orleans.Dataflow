using System.Globalization;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;

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

    /// <summary>Recovers the typed declaration of one result this pipeline exposes.</summary>
    /// <typeparam name="TResult">The type this process binds to the slot's result contract.</typeparam>
    /// <param name="name">The slot name, as the author declared it when closing the graph.</param>
    /// <param name="contract">The result contract the caller asserts the slot carries.</param>
    /// <returns>The slot, bound to this pipeline's document by fingerprint.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="contract"/> is the default value, <paramref name="name"/> is not a valid slot
    /// identifier, this pipeline declares no slot of that name, or the declared slot's contract is not
    /// <paramref name="contract"/>'s. The mismatch message names both contracts, because a caller holding
    /// the wrong one needs to see what the document says as well as what they asserted.
    /// </exception>
    /// <remarks>
    /// <para>
    /// A pipeline is a value that survives its authoring process — that is what deployability means — so
    /// the slot a run resolves has to be recoverable from the pipeline alone rather than only from the
    /// <c>To</c> call that closed the graph. This is that recovery, and the contract argument is what
    /// makes it typed: the document stores a contract reference and no CLR type, so the caller asserts
    /// the binding and this method checks the assertion against what the document declares.
    /// </para>
    /// <para>
    /// The returned slot carries this pipeline's <see cref="Fingerprint"/> and no authoring nonce, per
    /// ADR 0004 section 4: registered behavior is in the document, so two pipelines that share a
    /// fingerprint are the same pipeline and a per-instance identity would distinguish nothing. A slot of
    /// a <see cref="RunnableGraph"/> does carry one, and the two are not interchangeable — a run of a
    /// pipeline refuses a graph's slot and a run of a graph refuses a pipeline's, each naming which world
    /// the slot came from.
    /// </para>
    /// <para>
    /// Nothing is checked against a catalog here and nothing could be: a pipeline holds a document, and
    /// whether some host knows the stage that produces the slot is that host's question, answered when
    /// the pipeline is materialized.
    /// </para>
    /// </remarks>
    public ResultSlot<TResult> ResultSlot<TResult>(string name, ResultContract<TResult> contract)
    {
        ArgumentNullException.ThrowIfNull(name);

        if (contract.IsDefault)
        {
            throw new ArgumentException(
                $"Recovering a slot requires a declared {nameof(ResultContract<TResult>)}; the default value names no contract, and the contract is what makes the recovered slot typed. Declare one with {nameof(Orleans.Dataflow.ResultContract)}.{nameof(Orleans.Dataflow.ResultContract.For)}.",
                nameof(contract));
        }

        ResultSlotDefinition? declared = null;

        if (ResultSlotId.TryCreate(name, out ResultSlotId id))
        {
            foreach (ResultSlotDefinition candidate in Document.ResultSlots)
            {
                if (candidate.Id == id)
                {
                    declared = candidate;

                    break;
                }
            }
        }

        if (declared is null)
        {
            throw new ArgumentException(
                Document.ResultSlots.Count == 0
                    ? $"The pipeline '{Id}' declares no result slots at all, so it has none named '{name}'."
                    : $"The pipeline '{Id}' declares no result slot named '{name}'. The slots it declares are {string.Join(", ", Document.ResultSlots.Select(static slot => $"'{slot.Id}'"))}.",
                nameof(name));
        }

        if (declared.ResultContract != contract.Reference)
        {
            throw new ArgumentException(
                $"The pipeline '{Id}' declares the slot '{id}' with the result contract '{declared.ResultContract}', and the contract asserted here is '{contract.Reference}' bound to {typeof(TResult).Name}. A slot resolves under the contract its document declares; recovering it under another one would read a value of one shape as another.",
                nameof(contract));
        }

        return Orleans.Dataflow.ResultSlot<TResult>.Create(id, Fingerprint, PipelineMaterializer.PipelineNonce);
    }

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
