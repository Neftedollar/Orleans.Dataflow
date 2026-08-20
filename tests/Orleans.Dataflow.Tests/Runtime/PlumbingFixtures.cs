using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The documents and the registered vocabulary the deployable-plumbing tests are written against.
/// </summary>
/// <remarks>
/// <para>
/// Everything here is built through the definition plane directly — nodes, edges, slots, payloads — because
/// there is no other way to write a deployable document holding a named local occurrence: the authoring
/// surface has no spelling for naming one, so a graph it closes declares <c>ephemeral-identity</c> and is
/// not a pipeline. What the fluent API would produce is the same bytes with machine-made identifiers, which
/// <see cref="Api.RegisteredMixingTests"/> already pins.
/// </para>
/// <para>
/// <b>The registered stages declare the opaque element contract, and that is a workaround rather than a
/// design.</b> Every local port declares <c>local-opaque@v1</c>, a registered port declares whatever its
/// provider registered, and the graph compiler's element rule compares the two for equality — so a buffer
/// between two stages carrying a real contract produces two <c>element-contract-mismatch</c> diagnostics and
/// the document does not validate anywhere. <see cref="DeployablePlumbingTests"/> measures exactly that.
/// A provider whose ports declare the opaque contract is saying "my elements are typed by the CLR and not by
/// the document", which is true of this fixture and of every local graph, and it is the only shape in which
/// registered stages and plumbing compose today.
/// </para>
/// </remarks>
internal static class PlumbingFixtures
{
    /// <summary>The provider the fixture stages belong to.</summary>
    internal const string Provider = "plumbing-test";

    /// <summary>The payload every node with nothing to declare carries.</summary>
    internal static readonly CanonicalJsonValue Empty = CanonicalJsonValue.Parse("{}");

    /// <summary>Gets the element contract every fixture port declares.</summary>
    /// <value>The opaque local contract, for the reason this type's remarks give.</value>
    internal static ContractReference Opaque { get; } =
        ContractReference.Create(ContractId.Create("local-opaque"), 1);

    /// <summary>Gets the element contract of a provider that types its elements in the document.</summary>
    /// <value>An ordinary contract, used only to measure the seam that refuses.</value>
    internal static ContractReference Number { get; } =
        ContractReference.Create(ContractId.Create("plumbing-number"), 1);

    /// <summary>Gets the parameter contract every fixture node declares.</summary>
    internal static ContractReference NoParameters { get; } =
        ContractReference.Create(ContractId.Create("plumbing-no-parameters"), 1);

    /// <summary>Gets the result contract the summing sink resolves under.</summary>
    internal static ContractReference TotalContract { get; } =
        ContractReference.Create(ContractId.Create("plumbing-total"), 1);

    /// <summary>Gets the reference of the source that counts up from one.</summary>
    internal static StageRef Numbers { get; } = Stage("numbers");

    /// <summary>Gets the reference of the flow that holds an element until a test releases it.</summary>
    internal static StageRef Hold { get; } = Stage("hold");

    /// <summary>Gets the reference of the sink that sums what reaches it.</summary>
    internal static StageRef Sum { get; } = Stage("sum");

    /// <summary>Gets the catalog a deployment registers for this vocabulary alone.</summary>
    /// <value>
    /// The three fixture stages and nothing else, which is what a deployment writes; the plumbing beside
    /// them is published by the host rather than registered by the deployment, and a host that merged both
    /// halves from a registration would refuse the duplicate.
    /// </value>
    internal static StageCatalog RegisteredCatalog { get; } = StageCatalog.Create(Registered(Opaque));

    /// <summary>Gets the catalog a deployment publishes for this vocabulary and the plumbing beside it.</summary>
    /// <value>
    /// The fixture stages, the plumbing a silo publishes, and nothing else — which is exactly what
    /// <c>AddOrleansDataflow</c> assembles for a deployment that registers this provider.
    /// </value>
    internal static StageCatalog Catalog { get; } = StageCatalog.Create(
    [
        .. Registered(Opaque),
        .. LocalPlumbing.Catalog.Specifications,
    ]);

    /// <summary>Gets the same catalog with the fixture's own element contract on its ports.</summary>
    /// <value>What a provider that types its elements in the document publishes.</value>
    internal static StageCatalog TypedCatalog { get; } = StageCatalog.Create(
    [
        .. Registered(Number),
        .. LocalPlumbing.Catalog.Specifications,
    ]);

    /// <summary>Builds a stage reference of this fixture's provider at major version 1.</summary>
    /// <param name="stage">The stage identifier text.</param>
    /// <returns>The reference.</returns>
    internal static StageRef Stage(string stage) =>
        StageRef.Create(ProviderId.Create(Provider), StageId.Create(stage), 1);

    /// <summary>Builds a local stage reference at major version 1.</summary>
    /// <param name="stage">The stage identifier text, such as <c>buffer</c>.</param>
    /// <returns>The reference.</returns>
    internal static StageRef Local(string stage) =>
        StageRef.Create(ProviderId.Create("local"), StageId.Create(stage), 1);

    /// <summary>Builds one port address.</summary>
    /// <param name="node">The node identifier text.</param>
    /// <param name="port">The port identifier text.</param>
    /// <returns>The address.</returns>
    internal static PortAddress At(string node, string port) =>
        PortAddress.Create(NodeId.Create(node), PortId.Create(port));

    /// <summary>Builds one edge from two port addresses written as text.</summary>
    /// <param name="fromNode">The producing node.</param>
    /// <param name="fromPort">The producing port.</param>
    /// <param name="toNode">The consuming node.</param>
    /// <param name="toPort">The consuming port.</param>
    /// <returns>The edge.</returns>
    internal static GraphEdge Edge(string fromNode, string fromPort, string toNode, string toPort) =>
        GraphEdge.Create(At(fromNode, fromPort), At(toNode, toPort));

    /// <summary>Builds the node of one local plumbing occurrence.</summary>
    /// <param name="id">The name the author gave the occurrence.</param>
    /// <param name="stage">The local stage identifier text.</param>
    /// <param name="parameterContract">The parameter contract identifier text the stage declares.</param>
    /// <param name="parameters">The payload.</param>
    /// <returns>The node.</returns>
    internal static StageNode Plumbing(
        string id,
        string stage,
        string parameterContract,
        CanonicalJsonValue parameters) =>
        StageNode.Create(
            NodeId.Create(id),
            Local(stage),
            ContractReference.Create(ContractId.Create(parameterContract), 1),
            parameters);

    /// <summary>Builds the node of one buffer, from the very writer the authoring surface uses.</summary>
    /// <param name="id">The name the author gave the occurrence.</param>
    /// <param name="options">The capacity and overflow policy.</param>
    /// <returns>The node.</returns>
    /// <remarks>
    /// <see cref="LocalBufferParameters"/> rather than hand-written JSON, so that the payload a deployable
    /// document carries is byte-identical to the one a locally authored graph carries. A test that spelled
    /// the object itself would still pass if the two spellings drifted, which is the one thing it must not
    /// do.
    /// </remarks>
    internal static StageNode Buffer(string id, BufferOptions options) =>
        Plumbing(id, "buffer", "local-buffer-parameters", LocalBufferParameters.Write(options));

    /// <summary>Builds the node of one counted take.</summary>
    /// <param name="id">The name the author gave the occurrence.</param>
    /// <param name="count">How many elements it passes.</param>
    /// <returns>The node.</returns>
    internal static StageNode Take(string id, int count) =>
        Plumbing(id, "take", "local-count-parameters", LocalCountParameters.Write(count));

    /// <summary>Builds the node of one occurrence of a fixture stage.</summary>
    /// <param name="id">The name the author gave the occurrence.</param>
    /// <param name="stage">The stage reference.</param>
    /// <returns>The node.</returns>
    internal static StageNode Registered(string id, StageRef stage) =>
        StageNode.Create(NodeId.Create(id), stage, NoParameters, Empty);

    /// <summary>Builds a document from its parts under a real identity.</summary>
    /// <param name="id">The graph identity.</param>
    /// <param name="nodes">The nodes.</param>
    /// <param name="edges">The edges.</param>
    /// <param name="slots">The result slots.</param>
    /// <returns>The document.</returns>
    /// <remarks>
    /// No capability tokens at all, which is the claim under test: a document of registered stages and
    /// plumbing declares nothing, because nothing in it is bound to the process that wrote it.
    /// </remarks>
    internal static GraphDocument Document(
        string id,
        IEnumerable<StageNode> nodes,
        IEnumerable<GraphEdge> edges,
        IEnumerable<ResultSlotDefinition>? slots = null) =>
        GraphDocument.Create(
            GraphId.Create(id),
            GraphRevision.Create(1),
            [],
            nodes,
            edges,
            slots ?? []);

    /// <summary>Builds the slot declaration of the summing sink's total.</summary>
    /// <param name="node">The node identifier text of the sink.</param>
    /// <returns>The declaration.</returns>
    internal static ResultSlotDefinition Total(string node) =>
        ResultSlotDefinition.Create(ResultSlotId.Create("total"), TotalContract, At(node, "total"));

    /// <summary>Builds the fixture's specifications over one element contract.</summary>
    /// <param name="element">The contract every element port declares.</param>
    /// <returns>The specifications.</returns>
    private static StageSpecification[] Registered(ContractReference element) =>
    [
        StageSpecification.Create(
            Numbers,
            NoParameters,
            outputPorts: [OutputPortSpecification.Create(PortId.Create("out"), element)]),
        StageSpecification.Create(
            Hold,
            NoParameters,
            inputPorts: [InputPortSpecification.Create(PortId.Create("in"), element)],
            outputPorts: [OutputPortSpecification.Create(PortId.Create("out"), element)]),
        StageSpecification.Create(
            Sum,
            NoParameters,
            inputPorts: [InputPortSpecification.Create(PortId.Create("in"), element)],
            resultPorts: [ResultPortSpecification.Create(PortId.Create("total"), TotalContract)]),
    ];
}

/// <summary>
/// The runtime factory of <see cref="PlumbingFixtures"/>, written against the public provider seam.
/// </summary>
/// <param name="count">How many numbers the source emits, counting up from one.</param>
/// <param name="release">
/// What the holding flow waits on before it lets an element past, or <see langword="null"/> for a flow that
/// passes everything straight through.
/// </param>
/// <remarks>
/// The holding flow is what makes a buffer's capacity observable rather than merely declared. It takes one
/// element and never returns it, so the channel in front of it fills to exactly its declared capacity and
/// the next offer meets a full one — which under <see cref="OverflowPolicy.Fail"/> is a failure the run
/// reports and not a race a test hopes for.
/// </remarks>
internal sealed class PlumbingStageFactory(int count, TaskCompletionSource? release) : IDataflowStageFactory
{
    /// <inheritdoc/>
    public DataflowStageRuntime Create(DataflowStageRequest request)
    {
        StageRef stage = request.Node.Stage;

        if (stage == PlumbingFixtures.Numbers)
        {
            return DataflowStageRuntime.Source(_ => Numbers());
        }

        if (stage == PlumbingFixtures.Hold)
        {
            return DataflowStageRuntime.ElementAsync(
                async (element, cancellationToken) =>
                {
                    if (release is not null)
                    {
                        await release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                    }

                    return element;
                },
                maxConcurrency: 1,
                ordered: true);
        }

        if (stage == PlumbingFixtures.Sum)
        {
            return DataflowStageRuntime.Terminal(
                static () => 0L,
                static (state, element) => (long)state! + (long)element!,
                finish: null,
                producesResult: true);
        }

        throw new NotSupportedException($"The plumbing fixture provider does not implement '{stage}'.");
    }

    /// <summary>Emits the numbers one through <c>count</c>.</summary>
    /// <returns>The sequence.</returns>
    private async IAsyncEnumerable<object?> Numbers()
    {
        for (int index = 1; index <= count; index++)
        {
            yield return (long)index;

            await Task.Yield();
        }
    }
}
