using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Tests.Authoring;

/// <summary>
/// The parts the fragment algebra tests are written against.
/// </summary>
/// <remarks>
/// <para>
/// Every node here carries the same stage reference and the same payload, because no rule of the algebra
/// depends on either: the algebra is catalog-free, so a node is nothing but an identity to it. The one
/// exception is <see cref="PolicyNode"/>, which exists so that a test can prove an import carries the
/// optional execution policy across instead of quietly dropping it.
/// </para>
/// <para>
/// The linear shapes are named after the roles the specification gives them: a source has one open output,
/// a flow one of each, and a sink one open input.
/// </para>
/// </remarks>
internal static class FragmentFixtures
{
    /// <summary>The stage reference every fixture node carries.</summary>
    internal static readonly StageRef Stage =
        StageRef.Create(ProviderId.Create("orleans-core"), StageId.Create("map-async"), 1);

    /// <summary>The parameter contract every fixture node declares.</summary>
    internal static readonly ContractReference ParameterContract =
        ContractReference.Create(ContractId.Create("map-parameters"), 1);

    /// <summary>The execution policy contract a fixture node declares when it declares one.</summary>
    internal static readonly ContractReference PolicyContract =
        ContractReference.Create(ContractId.Create("map-policy"), 2);

    /// <summary>The result contract every fixture result slot declares.</summary>
    internal static readonly ContractReference ResultContract =
        ContractReference.Create(ContractId.Create("fold-result"), 1);

    /// <summary>The parameter payload every fixture node carries.</summary>
    internal static readonly CanonicalJsonValue Parameters = CanonicalJsonValue.Parse("""{"parallelism":4}""");

    /// <summary>The execution policy payload a fixture node carries when it declares one.</summary>
    internal static readonly CanonicalJsonValue Policy = CanonicalJsonValue.Parse("""{"retries":2}""");

    /// <summary>Builds a node that takes the provider default execution policy.</summary>
    /// <param name="id">The node identifier path.</param>
    /// <returns>The node.</returns>
    internal static StageNode Node(string id) =>
        StageNode.Create(NodeId.Parse(id), Stage, ParameterContract, Parameters);

    /// <summary>Builds a node that declares an explicit execution policy.</summary>
    /// <param name="id">The node identifier path.</param>
    /// <returns>The node.</returns>
    internal static StageNode PolicyNode(string id) =>
        StageNode.Create(NodeId.Parse(id), Stage, ParameterContract, Parameters, PolicyContract, Policy);

    /// <summary>Builds a port address from its two identifier texts.</summary>
    /// <param name="node">The node identifier path.</param>
    /// <param name="port">The port identifier segment.</param>
    /// <returns>The address.</returns>
    internal static PortAddress Port(string node, string port) =>
        PortAddress.Create(NodeId.Parse(node), PortId.Create(port));

    /// <summary>Builds an edge from the four identifier texts of its endpoints.</summary>
    /// <param name="fromNode">The origin node identifier path.</param>
    /// <param name="fromPort">The origin port identifier segment.</param>
    /// <param name="toNode">The target node identifier path.</param>
    /// <param name="toPort">The target port identifier segment.</param>
    /// <returns>The edge.</returns>
    internal static GraphEdge Edge(string fromNode, string fromPort, string toNode, string toPort) =>
        GraphEdge.Create(Port(fromNode, fromPort), Port(toNode, toPort));

    /// <summary>Builds a result slot definition from its identifier texts.</summary>
    /// <param name="id">The slot identifier segment.</param>
    /// <param name="producerNode">The producing node identifier path.</param>
    /// <param name="producerPort">The producing port identifier segment.</param>
    /// <returns>The slot definition.</returns>
    internal static ResultSlotDefinition Slot(string id, string producerNode, string producerPort) =>
        ResultSlotDefinition.Create(ResultSlotId.Create(id), ResultContract, Port(producerNode, producerPort));

    /// <summary>Builds the port identifiers of a list of port names.</summary>
    /// <param name="names">The port identifier segments, in order.</param>
    /// <returns>The identifiers, in the same order.</returns>
    internal static PortId[] Ports(params string[] names) => [.. names.Select(PortId.Create)];

    /// <summary>Builds a one-node fragment with one open output named <c>out</c>.</summary>
    /// <param name="id">The node identifier path.</param>
    /// <returns>The fragment.</returns>
    internal static GraphFragment Source(string id) => GraphFragment.OfStage(Node(id), [], Ports("out"));

    /// <summary>Builds a one-node fragment with one open input named <c>in</c> and one open output named <c>out</c>.</summary>
    /// <param name="id">The node identifier path.</param>
    /// <returns>The fragment.</returns>
    internal static GraphFragment Flow(string id) => GraphFragment.OfStage(Node(id), Ports("in"), Ports("out"));

    /// <summary>Builds a one-node fragment with one open input named <c>in</c>.</summary>
    /// <param name="id">The node identifier path.</param>
    /// <returns>The fragment.</returns>
    internal static GraphFragment Sink(string id) => GraphFragment.OfStage(Node(id), Ports("in"), []);

    /// <summary>Renders the node identifier texts of a fragment's nodes, in their stored order.</summary>
    /// <param name="fragment">The fragment to read.</param>
    /// <returns>The identifier texts.</returns>
    internal static string[] NodeIds(GraphFragment fragment) => [.. fragment.Nodes.Select(node => node.Id.Value)];

    /// <summary>Renders the edges of a fragment as text, in their stored order.</summary>
    /// <param name="fragment">The fragment to read.</param>
    /// <returns>The edge texts.</returns>
    internal static string[] EdgeTexts(GraphFragment fragment) => [.. fragment.Edges.Select(edge => edge.ToString())];
}
