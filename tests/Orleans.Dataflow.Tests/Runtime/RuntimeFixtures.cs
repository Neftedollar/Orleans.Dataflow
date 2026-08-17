using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The graphs, the host, and the hand-built documents the local runtime tests are written against.
/// </summary>
/// <remarks>
/// <para>
/// The authored graphs here are the ordinary ones: a sequence of numbers summed into a slot. Tests that
/// need their own delegates build their own graphs; this type carries only what more than one test would
/// otherwise repeat.
/// </para>
/// <para>
/// The hand-built documents are the interesting half. Every shape the runtime refuses is unreachable
/// through the authoring API, whose generic signatures and linear composition make it so, which means the
/// only way to test those refusals is to build the document and the binding table directly. That is what
/// <see cref="Graph"/> and its helpers exist for, and, like the rest of this suite, they re-derive every
/// identity from its text rather than echoing the production constants back at themselves.
/// </para>
/// </remarks>
internal static class RuntimeFixtures
{
    /// <summary>Gets the host every runtime test materializes through.</summary>
    /// <remarks>The host is stateless and holds no run, so one instance serves the whole suite.</remarks>
    internal static LocalDataflowHost Host { get; } = new();

    /// <summary>Gets the running test's own cancellation token.</summary>
    /// <remarks>
    /// Passed to every runtime call that accepts one, so that a run left holding a thread by a failing test
    /// is torn down with the test rather than surviving it. Tests that make a claim about a particular
    /// token pass that one instead.
    /// </remarks>
    internal static CancellationToken TestToken => TestContext.Current.CancellationToken;

    /// <summary>Builds the ordinary graph: sum a sequence of numbers into a slot named <c>total</c>.</summary>
    /// <param name="elements">The sequence to sum.</param>
    /// <param name="total">When this method returns, the slot the sum resolves.</param>
    /// <returns>The closed graph.</returns>
    internal static RunnableGraph Summing(IEnumerable<int> elements, out ResultSlot<long> total) =>
        Source.From(elements).To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out total);

    /// <summary>Builds the ordinary graph with an observer on every folded element.</summary>
    /// <param name="elements">The sequence to sum.</param>
    /// <param name="folding">Called with each element as the fold reaches it, before the state is updated.</param>
    /// <param name="total">When this method returns, the slot the sum resolves.</param>
    /// <returns>The closed graph.</returns>
    /// <remarks>
    /// The observer is how a test holds a run at a known element and how it records which elements a run
    /// actually reached: the fold is the last thing that happens to an element, so an element the observer
    /// saw is an element the run finished with.
    /// </remarks>
    internal static RunnableGraph Summing(
        IEnumerable<int> elements,
        Action<int> folding,
        out ResultSlot<long> total) =>
        Source.From(elements)
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        folding(value);

                        return sum + value;
                    }),
                "total",
                out total);

    /// <summary>Builds a graph directly from a document and a binding table.</summary>
    /// <param name="document">The document.</param>
    /// <param name="bindings">The behavior of each node, keyed by node identifier.</param>
    /// <param name="controls">
    /// The type of each runtime control the graph declares, keyed by name; omitted for the graphs that
    /// declare none.
    /// </param>
    /// <returns>The graph, fingerprinted the way closing one would have fingerprinted it.</returns>
    /// <remarks>
    /// This is the back door the authoring API deliberately does not have. It exists so that the runtime's
    /// defenses against a document it did not build can be tested at all. The control registry is supplied
    /// the same way the binding table is, and for the same reason: a CLR type is not durable topology, so a
    /// document alone never says what type a control is.
    /// </remarks>
    internal static RunnableGraph Graph(
        GraphDocument document,
        IReadOnlyDictionary<NodeId, LocalStageDescriptor> bindings,
        IReadOnlyDictionary<ResultSlotId, Type>? controls = null) =>
        new(document, GraphDocumentSerializer.Fingerprint(document), bindings, controls);

    /// <summary>Builds a document over local nodes with the capabilities every local document declares.</summary>
    /// <param name="nodes">The nodes.</param>
    /// <param name="edges">The edges.</param>
    /// <returns>The document, declaring no result.</returns>
    internal static GraphDocument Document(IEnumerable<StageNode> nodes, IEnumerable<GraphEdge> edges) =>
        GraphDocument.Create(
            GraphId.Create("anonymous"),
            GraphRevision.Create(GraphRevision.FirstRevisionNumber),
            [CapabilityToken.Nondeployable, CapabilityToken.EphemeralIdentity],
            nodes,
            edges,
            []);

    /// <summary>Builds one node of the local vocabulary whose behavior is only a delegate.</summary>
    /// <param name="id">The node identifier text, such as <c>stage-1</c>.</param>
    /// <param name="stage">The stage identifier text, such as <c>from-enumerable</c>.</param>
    /// <returns>The node, carrying the empty payload under the delegate-only parameter contract.</returns>
    internal static StageNode Node(string id, string stage) =>
        Node(id, stage, "local-parameters", "{}");

    /// <summary>Builds one node of the local vocabulary with a parameter payload of its own.</summary>
    /// <param name="id">The node identifier text, such as <c>stage-2</c>.</param>
    /// <param name="stage">The stage identifier text, such as <c>buffer</c>.</param>
    /// <param name="contract">The parameter contract identifier text.</param>
    /// <param name="parameters">The parameter payload as JSON text.</param>
    /// <returns>The node.</returns>
    /// <remarks>
    /// The contract and the payload are spelled out rather than derived, because the payloads worth
    /// building by hand are the ones the authoring API cannot produce: a capacity of zero, a policy no
    /// member declares, a member this vocabulary never wrote.
    /// </remarks>
    internal static StageNode Node(string id, string stage, string contract, string parameters) =>
        StageNode.Create(
            NodeId.Create(id),
            StageRef.Create(ProviderId.Create("local"), StageId.Create(stage), 1),
            ContractReference.Create(ContractId.Create(contract), 1),
            CanonicalJsonValue.Parse(parameters));

    /// <summary>Builds the edge from one node's output port to another's input port.</summary>
    /// <param name="from">The producing node's identifier text.</param>
    /// <param name="to">The consuming node's identifier text.</param>
    /// <returns>The edge.</returns>
    internal static GraphEdge Edge(string from, string to) =>
        GraphEdge.Create(
            PortAddress.Create(NodeId.Create(from), PortId.Create("out")),
            PortAddress.Create(NodeId.Create(to), PortId.Create("in")));

    /// <summary>Builds a binding table from node identifier texts and their occurrences.</summary>
    /// <param name="bindings">The pairs, in any order.</param>
    /// <returns>The table.</returns>
    internal static IReadOnlyDictionary<NodeId, LocalStageDescriptor> Bindings(
        params (string Node, LocalStageDescriptor Stage)[] bindings) =>
        bindings.ToDictionary(binding => NodeId.Create(binding.Node), binding => binding.Stage);
}
