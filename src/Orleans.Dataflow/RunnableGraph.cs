using System.Globalization;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

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
}
