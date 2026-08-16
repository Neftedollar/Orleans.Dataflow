using System.Globalization;
using System.Text;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow;

/// <summary>
/// A closed graph: everything is connected, the document is built, and nothing has started.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="RunnableGraph"/> is not generic over its results, per ADR 0004 section 1. A generic parameter
/// shortens exactly one program, collapses into tuple threading as soon as a graph has two results, and
/// does not prevent the cross-graph mistake it would exist to prevent, because two graphs with the same
/// result type stay interchangeable. Results are named instead: <see cref="ResultSlots"/> lists them and a
/// <see cref="ResultSlot{TResult}"/> carries the type.
/// </para>
/// <para>
/// Building a graph starts no work. A host materializes it, and materializing the same graph twice yields
/// two independent runs.
/// </para>
/// <para>
/// A graph built from lambda stages keeps its delegates beside the document, never inside it: the document
/// names <c>local</c> stages, and the delegates live in an internal table keyed by node identifier for the
/// local runtime to bind at materialization. That is the concrete meaning of the <c>nondeployable</c>
/// capability every such document declares. What a stage can say about itself in numbers rather than in
/// code — a buffer's capacity and overflow policy, an asynchronous stage's concurrency bound — is in the
/// document's parameter payloads, and every other stage carries the empty object there.
/// </para>
/// <para>
/// Two graphs are the same graph exactly when their <see cref="Fingerprint"/> values are equal; the type
/// itself uses reference equality, because a graph is a built artifact rather than a value one would
/// compare.
/// </para>
/// </remarks>
public sealed class RunnableGraph
{
    /// <summary>Initializes a new instance of the <see cref="RunnableGraph"/> class.</summary>
    /// <param name="document">The closed, validated document.</param>
    /// <param name="fingerprint">The fingerprint of that document's canonical bytes.</param>
    /// <param name="localBindings">The authoring-side behavior of every node, keyed by node identifier.</param>
    /// <remarks>
    /// The constructor takes the fingerprint rather than computing it, so that the caller that closed the
    /// document also decides when it is serialized; the two are always derived from the same value.
    /// </remarks>
    internal RunnableGraph(
        GraphDocument document,
        GraphFingerprint fingerprint,
        IReadOnlyDictionary<NodeId, LocalStageDescriptor> localBindings)
    {
        Document = document;
        Fingerprint = fingerprint;
        LocalBindings = localBindings;
        AuthoringNonce = Guid.NewGuid();

        ResultSlotId[] slots = new ResultSlotId[document.ResultSlots.Count];

        for (int index = 0; index < document.ResultSlots.Count; index++)
        {
            slots[index] = document.ResultSlots[index].Id;
        }

        ResultSlots = Array.AsReadOnly(slots);
    }

    /// <summary>Gets the durable description of this graph.</summary>
    /// <value>The closed graph document, which is the only representation that could ever be persisted.</value>
    public GraphDocument Document { get; }

    /// <summary>Gets the identity of this graph's document.</summary>
    /// <value>The SHA-256 fingerprint of the document's canonical bytes.</value>
    /// <remarks>
    /// This is the value a <see cref="ResultSlot{TResult}"/> binds to and a run checks against. It is
    /// equal to <see cref="Serialization.GraphDocumentSerializer.Fingerprint(GraphDocument)"/> of
    /// <see cref="Document"/>, and is computed once when the graph is closed.
    /// </remarks>
    public GraphFingerprint Fingerprint { get; }

    /// <summary>Gets the names of the results this graph declares.</summary>
    /// <value>
    /// The slot identifiers in the document's canonical order, which is empty for a graph that declares no
    /// result.
    /// </value>
    /// <remarks>
    /// The names are what the document declares; the typed handle to each is the
    /// <see cref="ResultSlot{TResult}"/> that closing the graph produced, because only the author's code
    /// knows the result type.
    /// </remarks>
    public IReadOnlyList<ResultSlotId> ResultSlots { get; }

    /// <summary>Gets the behavior bound to each node of this graph.</summary>
    /// <value>The authoring-side binding table, keyed by node identifier.</value>
    /// <remarks>
    /// Internal, and deliberately not reachable from the public surface: this is the half of a local graph
    /// that is not durable topology. The public statement about a node is <see cref="Document"/>.
    /// </remarks>
    internal IReadOnlyDictionary<NodeId, LocalStageDescriptor> LocalBindings { get; }

    /// <summary>Gets the per-instance identity of this built graph.</summary>
    /// <value>A nonce allocated when the graph was closed.</value>
    /// <remarks>
    /// The document fingerprint identifies shape, not behavior: two lambda graphs of one shape share a
    /// fingerprint whatever their delegates compute, because a delegate never enters the document. A slot
    /// of a nondeployable graph therefore binds to this nonce as well, so resolving it against a run of a
    /// different instance fails loudly instead of silently reading a graph that merely looks the same.
    /// The nonce never enters the document and plays no part in serialization or fingerprints.
    /// </remarks>
    internal Guid AuthoringNonce { get; }

    /// <summary>Declares this graph as one revision of one durable pipeline.</summary>
    /// <param name="id">The identity of the graph lineage this pipeline belongs to.</param>
    /// <param name="revision">The revision this pipeline is.</param>
    /// <returns>The pipeline definition, whose document carries the given identity.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="id"/> or <paramref name="revision"/> is the default value, or this graph's document
    /// declares a capability that denies it a durable identity. The deployability message is a numbered
    /// list of every violation found, so one call names every reason rather than one reason per call.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The content is re-closed under the real identity rather than relabelled: the document model
    /// revalidates every structural invariant, and the identity is document content, so the pipeline's
    /// <see cref="PipelineDefinition.Fingerprint"/> differs from this graph's. That difference is the
    /// point — a pipeline's fingerprint is the fingerprint of the deployable document, not of the
    /// placeholder identity an anonymous graph carries.
    /// </para>
    /// <para>
    /// Two capabilities are refused, and both are refusals of the same thing. <c>nondeployable</c> says a
    /// stage's behavior lives in this process, so nothing outside it could ever materialize the document;
    /// <c>ephemeral-identity</c> says the node identifiers are positional, so nothing could anchor a
    /// checkpoint, an upgrade, or a resume to them. Neither is stripped: a graph that has them is not a
    /// pipeline with a caveat, it is a different kind of graph, and a fully registered and fully named
    /// chain never had them in the first place.
    /// </para>
    /// <para>
    /// Every other declared capability travels into the pipeline untouched, and whether some target
    /// deployment knows those tokens is deliberately not checked here: no catalog is in scope, and a
    /// capability check against the wrong catalog would be worse than none. That check belongs to the M3
    /// negotiation between a pipeline and the host asked to run it, which is where the target catalog and
    /// its fingerprint actually exist.
    /// </para>
    /// </remarks>
    public PipelineDefinition AsPipeline(GraphId id, GraphRevision revision)
    {
        if (id.IsDefault)
        {
            throw new ArgumentException(
                $"A {nameof(PipelineDefinition)} requires a created {nameof(GraphId)}; the default {nameof(GraphId)} names no graph, and the identity is what a pipeline's revisions are a lineage of.",
                nameof(id));
        }

        if (revision.IsDefault)
        {
            throw new ArgumentException(
                $"A {nameof(PipelineDefinition)} requires a created {nameof(GraphRevision)}; the default {nameof(GraphRevision)} names no revision.",
                nameof(revision));
        }

        // Reported in the document's own canonical order — ordinal over the token text, which puts
        // 'ephemeral-identity' before 'nondeployable' — so that the list reads in the order the document
        // declares them rather than in the order this method happens to test them.
        List<string> violations = [];

        if (Document.Capabilities.Contains(CapabilityToken.EphemeralIdentity))
        {
            violations.Add(
                $"it declares the capability '{CapabilityToken.EphemeralIdentity}', which says its node identifiers are positions rather than names, so nothing durable could be anchored to them; every occurrence of a pipeline is named by its author");
        }

        if (Document.Capabilities.Contains(CapabilityToken.Nondeployable))
        {
            violations.Add(
                $"it declares the capability '{CapabilityToken.Nondeployable}', which says a stage's behavior is bound in this process and reaches no document, so nothing else could ever materialize it; every stage of a pipeline resolves from a catalog");
        }

        if (violations.Count > 0)
        {
            throw new ArgumentException(FormatViolations(violations));
        }

        GraphDocument deployable = GraphDocument.Create(
            id,
            revision,
            Document.Capabilities,
            Document.Nodes,
            Document.Edges,
            Document.ResultSlots);

        return new PipelineDefinition(deployable, GraphDocumentSerializer.Fingerprint(deployable));
    }

    /// <summary>Returns a one-line diagnostic summary of this graph.</summary>
    /// <returns>
    /// Text of the form <c>graph sha256:9f86d081... (4 nodes, 1 result slot)</c>, with each count
    /// singular or plural as it reads.
    /// </returns>
    /// <remarks>The counts are formatted with the invariant culture, and the method never throws.</remarks>
    public override string ToString()
    {
        int nodes = Document.Nodes.Count;
        int slots = ResultSlots.Count;

        return string.Create(
            CultureInfo.InvariantCulture,
            $"graph {Fingerprint} ({nodes} {(nodes == 1 ? "node" : "nodes")}, {slots} {(slots == 1 ? "result slot" : "result slots")})");
    }

    /// <summary>Renders the collected deployability violations as one numbered list.</summary>
    /// <param name="violations">The violations, in the order they were found.</param>
    /// <returns>A message whose first line states the count and whose remaining lines are numbered.</returns>
    /// <remarks>
    /// The exception carries no parameter name because the violations are properties of this graph rather
    /// than of either argument, and the shape is the one every aggregate report in this codebase uses, so
    /// a reader does not have to know which type produced the message to read it.
    /// </remarks>
    private static string FormatViolations(List<string> violations)
    {
        StringBuilder message = new();

        message.Append(CultureInfo.InvariantCulture, $"This graph cannot become a {nameof(PipelineDefinition)} because it breaks {violations.Count} deployability ");
        message.Append(violations.Count == 1 ? "invariant:" : "invariants:");

        for (int index = 0; index < violations.Count; index++)
        {
            message.Append(Environment.NewLine)
                .Append(CultureInfo.InvariantCulture, $"{index + 1}. {violations[index]}.");
        }

        return message.ToString();
    }
}
