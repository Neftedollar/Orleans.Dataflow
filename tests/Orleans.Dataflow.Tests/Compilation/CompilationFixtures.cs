using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Tests.Compilation;

/// <summary>
/// The catalog and the document parts the graph compiler tests are written against.
/// </summary>
/// <remarks>
/// <para>
/// Every stage here declares the smallest shape that can trigger exactly one rule, so a test can assert
/// the whole diagnostic list rather than merely that the list contains something. A stage with no ports
/// triggers no connectivity rule, a stage with one required input triggers exactly one, and so on; that is
/// what makes an exact-sequence assertion possible and therefore what makes an unexpected extra
/// diagnostic a failure instead of a silent pass.
/// </para>
/// <para>
/// The parameter payload is the same for every node, because no rule in this milestone depends on the
/// payload except through a stage's own validator, which the tests supply explicitly.
/// </para>
/// </remarks>
internal static class CompilationFixtures
{
    /// <summary>The provider every fixture stage belongs to.</summary>
    private const string Provider = "orleans-core";

    /// <summary>The parameter payload every fixture node carries.</summary>
    private static readonly CanonicalJsonValue Parameters = CanonicalJsonValue.Parse("""{"value":1}""");

    /// <summary>
    /// Builds the fixture catalog with a validator on the <c>strict</c> stage.
    /// </summary>
    /// <param name="strictValidator">The validator to register for the <c>strict</c> stage.</param>
    /// <returns>The catalog.</returns>
    internal static StageCatalog Catalog(IStageParameterValidator strictValidator) =>
        StageCatalog.Create(
            [
                // Declares nothing: a node on this stage can break a node rule and nothing else.
                Specification("probe", []),

                // One output that must be consumed.
                Specification("source", [], [Output("out", "order")], []),

                // One required input and one result port.
                Specification("sink", [Input("in", "order")], [], [Result("result", "counter-result")]),

                // Same shape as 'sink' but a different element contract, for contract-mismatch tests.
                Specification("typed-sink", [Input("in", "order-summary", 3)], [], []),

                // An input a graph may leave unconnected.
                Specification("optional-sink", [Input("in", "order", 1, isOptional: true)], [], []),

                // An output a graph may leave unconsumed.
                Specification("ignorable-source", [], [Output("out", "order", 1, isIgnorable: true)], []),

                // Two required inputs and two consumed outputs, declared in an order the canonical one
                // reverses, so that a connectivity report over this stage pins the documented port order
                // rather than the order the ports were registered in.
                Specification(
                    "hub",
                    [Input("in-b", "order"), Input("in-a", "order")],
                    [Output("out-b", "order"), Output("out-a", "order")],
                    []),

                // Declares nothing but carries a parameter validator.
                StageSpecification.Create(
                    Stage("strict"),
                    [],
                    [],
                    [],
                    ParameterContractOf("strict"),
                    [],
                    strictValidator),

                // Capability requirements, one token each, named so that the order the nodes contribute
                // them in is the opposite of ordinal order.
                Specification("capable", [], [], [], ["nondeployable"]),
                Specification("needs-zulu", [], [], [], ["zulu"]),
                Specification("needs-alpha", [], [], [], ["alpha"]),
            ]);

    /// <summary>
    /// Builds the fixture catalog with a validator that accepts every payload.
    /// </summary>
    /// <returns>The catalog.</returns>
    internal static StageCatalog Catalog() => Catalog(new RecordingValidator());

    /// <summary>
    /// Builds a document from its parts, with the fixture graph identity and revision.
    /// </summary>
    /// <param name="nodes">The nodes.</param>
    /// <param name="edges">The edges.</param>
    /// <param name="resultSlots">The result slots.</param>
    /// <param name="capabilities">The declared capability tokens.</param>
    /// <returns>The structurally valid document.</returns>
    internal static GraphDocument Graph(
        IEnumerable<StageNode>? nodes = null,
        IEnumerable<GraphEdge>? edges = null,
        IEnumerable<ResultSlotDefinition>? resultSlots = null,
        IEnumerable<CapabilityToken>? capabilities = null) =>
        GraphDocument.Create(
            GraphId.Create("compilation-fixture"),
            GraphRevision.Create(1),
            capabilities ?? [],
            nodes ?? [],
            edges ?? [],
            resultSlots ?? []);

    /// <summary>
    /// Builds a node whose declared parameter contract is the one its stage declares.
    /// </summary>
    /// <param name="id">The node identifier text.</param>
    /// <param name="stage">The stage identifier text.</param>
    /// <returns>The node.</returns>
    internal static StageNode Node(string id, string stage) =>
        StageNode.Create(NodeId.Parse(id), Stage(stage), ParameterContractOf(stage), Parameters);

    /// <summary>
    /// Builds a node with an explicitly chosen parameter contract.
    /// </summary>
    /// <param name="id">The node identifier text.</param>
    /// <param name="stage">The stage identifier text.</param>
    /// <param name="parameterContract">The contract identifier text the node declares.</param>
    /// <returns>The node.</returns>
    internal static StageNode NodeWithContract(string id, string stage, string parameterContract) =>
        StageNode.Create(
            NodeId.Parse(id),
            Stage(stage),
            ContractReference.Create(ContractId.Create(parameterContract), 1),
            Parameters);

    /// <summary>
    /// Builds a node that declares an explicit execution policy.
    /// </summary>
    /// <param name="id">The node identifier text.</param>
    /// <param name="stage">The stage identifier text.</param>
    /// <returns>The node.</returns>
    /// <remarks>
    /// No stage specification declares which policy contracts it accepts in this milestone, so the policy
    /// contract named here is deliberately one no fixture stage knows about.
    /// </remarks>
    internal static StageNode NodeWithExecutionPolicy(string id, string stage) =>
        StageNode.Create(
            NodeId.Parse(id),
            Stage(stage),
            ParameterContractOf(stage),
            Parameters,
            ContractReference.Create(ContractId.Create("retry-policy"), 1),
            CanonicalJsonValue.Parse("""{"maxAttempts":5}"""));

    /// <summary>Builds an edge from its two addresses.</summary>
    /// <param name="fromNode">The origin node identifier text.</param>
    /// <param name="fromPort">The origin port name.</param>
    /// <param name="toNode">The target node identifier text.</param>
    /// <param name="toPort">The target port name.</param>
    /// <returns>The edge.</returns>
    internal static GraphEdge Edge(string fromNode, string fromPort, string toNode, string toPort) =>
        GraphEdge.Create(Port(fromNode, fromPort), Port(toNode, toPort));

    /// <summary>Builds a result slot definition.</summary>
    /// <param name="id">The slot identifier text.</param>
    /// <param name="contract">The result contract identifier text.</param>
    /// <param name="node">The producing node identifier text.</param>
    /// <param name="port">The producing port name.</param>
    /// <returns>The slot definition.</returns>
    internal static ResultSlotDefinition Slot(string id, string contract, string node, string port) =>
        ResultSlotDefinition.Create(
            ResultSlotId.Create(id),
            ContractReference.Create(ContractId.Create(contract), 1),
            Port(node, port));

    /// <summary>Builds a port address from its two identifier texts.</summary>
    /// <param name="node">The node identifier path.</param>
    /// <param name="port">The port identifier segment.</param>
    /// <returns>The port address.</returns>
    internal static PortAddress Port(string node, string port) =>
        PortAddress.Create(NodeId.Parse(node), PortId.Create(port));

    /// <summary>Builds a capability token from its text.</summary>
    /// <param name="value">The token text.</param>
    /// <returns>The token.</returns>
    internal static CapabilityToken Capability(string value) => CapabilityToken.Create(value);

    /// <summary>Builds a fixture stage reference.</summary>
    /// <param name="stage">The stage identifier text.</param>
    /// <returns>The stage reference, at major version 1.</returns>
    internal static StageRef Stage(string stage) =>
        StageRef.Create(ProviderId.Create(Provider), StageId.Create(stage), 1);

    /// <summary>
    /// Returns the parameter contract a fixture stage declares.
    /// </summary>
    /// <param name="stage">The stage identifier text.</param>
    /// <returns>The contract reference the specification declares, at major version 1.</returns>
    /// <remarks>
    /// Several stages share one parameter contract on purpose, so that a test can move a node from one
    /// stage to another without also having to change what it declares.
    /// </remarks>
    internal static ContractReference ParameterContractOf(string stage) =>
        ContractReference.Create(
            ContractId.Create(stage switch
            {
                "source" or "ignorable-source" => "source-parameters",
                "sink" or "typed-sink" or "optional-sink" => "sink-parameters",
                "strict" => "strict-parameters",
                "capable" => "capable-parameters",
                _ => "probe-parameters",
            }),
            1);

    /// <summary>Builds a fixture specification.</summary>
    /// <param name="stage">The stage identifier text.</param>
    /// <param name="inputPorts">The input ports.</param>
    /// <param name="outputPorts">The output ports.</param>
    /// <param name="resultPorts">The result ports.</param>
    /// <param name="requiredCapabilities">The required capability token texts.</param>
    /// <returns>The specification.</returns>
    private static StageSpecification Specification(
        string stage,
        IEnumerable<InputPortSpecification> inputPorts,
        IEnumerable<OutputPortSpecification>? outputPorts = null,
        IEnumerable<ResultPortSpecification>? resultPorts = null,
        IEnumerable<string>? requiredCapabilities = null)
    {
        List<CapabilityToken> tokens = [];

        foreach (string token in requiredCapabilities ?? [])
        {
            tokens.Add(CapabilityToken.Create(token));
        }

        return StageSpecification.Create(
            Stage(stage),
            inputPorts,
            outputPorts ?? [],
            resultPorts ?? [],
            ParameterContractOf(stage),
            tokens);
    }

    /// <summary>Builds an input port specification.</summary>
    /// <param name="port">The port name.</param>
    /// <param name="contract">The element contract identifier text.</param>
    /// <param name="majorVersion">The element contract major version.</param>
    /// <param name="isOptional">Whether the port may be left unconnected.</param>
    /// <returns>The port specification.</returns>
    private static InputPortSpecification Input(
        string port,
        string contract,
        int majorVersion = 1,
        bool isOptional = false) =>
        InputPortSpecification.Create(
            PortId.Create(port),
            ContractReference.Create(ContractId.Create(contract), majorVersion),
            isOptional);

    /// <summary>Builds an output port specification.</summary>
    /// <param name="port">The port name.</param>
    /// <param name="contract">The element contract identifier text.</param>
    /// <param name="majorVersion">The element contract major version.</param>
    /// <param name="isIgnorable">Whether the port may be left unconnected.</param>
    /// <returns>The port specification.</returns>
    private static OutputPortSpecification Output(
        string port,
        string contract,
        int majorVersion = 1,
        bool isIgnorable = false) =>
        OutputPortSpecification.Create(
            PortId.Create(port),
            ContractReference.Create(ContractId.Create(contract), majorVersion),
            isIgnorable);

    /// <summary>Builds a result port specification.</summary>
    /// <param name="port">The port name.</param>
    /// <param name="contract">The result contract identifier text.</param>
    /// <returns>The port specification.</returns>
    private static ResultPortSpecification Result(string port, string contract) =>
        ResultPortSpecification.Create(
            PortId.Create(port),
            ContractReference.Create(ContractId.Create(contract), 1));
}
