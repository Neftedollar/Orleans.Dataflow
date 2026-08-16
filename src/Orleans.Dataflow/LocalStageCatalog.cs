using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow;

/// <summary>
/// The stage catalog that describes the local, lambda-implemented vocabulary the C# authoring API builds
/// graphs from.
/// </summary>
/// <remarks>
/// <para>
/// This is the catalog every graph this API closes validates against:
/// <c>GraphCompiler.Validate(graph.Document, LocalStageCatalog.Instance)</c> is valid for every graph the
/// authoring types can express, which is what connects the C# frontend to the definition plane rather than
/// leaving it a parallel world.
/// </para>
/// <para>
/// Five specifications, one per stage of the vocabulary, all under the provider <c>local</c> at major
/// version 1. Every element port declares the same opaque element contract, because a local graph's element
/// types live in the C# type system and never in the document; every stage declares the same empty
/// parameter contract, because its behavior is a delegate and a delegate is never durable topology; and
/// every stage requires the <c>nondeployable</c> capability, which is how a document that contains one is
/// stopped before it can be persisted, resumed, or placed remotely.
/// </para>
/// <para>
/// The catalog is therefore not a registration mechanism and is not extensible. Registered stages, with
/// real contracts and real parameters, are the deployable path and arrive with their own registration
/// surface.
/// </para>
/// </remarks>
public static class LocalStageCatalog
{
    /// <summary>Gets the catalog of the local stage vocabulary.</summary>
    /// <value>
    /// A catalog holding one specification for each of <c>from-enumerable</c>, <c>select</c>, <c>where</c>,
    /// <c>fold</c>, and <c>ignore</c>.
    /// </value>
    /// <remarks>
    /// The catalog is immutable and stateless, so one instance serves every caller; a
    /// <see cref="CatalogFingerprint"/> taken over it is stable for the lifetime of the assembly version.
    /// </remarks>
    public static IStageCatalog Instance { get; } = Build();

    /// <summary>Builds the five specifications of the local vocabulary.</summary>
    /// <returns>The catalog.</returns>
    /// <remarks>
    /// The port lists spell out the shape each authoring type relies on: a source produces and does not
    /// consume, a flow does both, a fold consumes and declares a result, and a discarding sink only
    /// consumes. No port is optional or ignorable, so the graph compiler's connectivity rule requires every
    /// port of every occurrence to be wired — which is exactly the linear chain the authoring types can
    /// build, and nothing looser.
    /// </remarks>
    private static StageCatalog Build()
    {
        InputPortSpecification input =
            InputPortSpecification.Create(LocalVocabulary.InputPort, LocalVocabulary.ElementContract);
        OutputPortSpecification output =
            OutputPortSpecification.Create(LocalVocabulary.OutputPort, LocalVocabulary.ElementContract);
        ResultPortSpecification result =
            ResultPortSpecification.Create(LocalVocabulary.ResultPort, LocalVocabulary.FoldResultContract);

        return StageCatalog.Create(
        [
            Specification(LocalVocabulary.FromEnumerable, [], [output], []),
            Specification(LocalVocabulary.Select, [input], [output], []),
            Specification(LocalVocabulary.Where, [input], [output], []),
            Specification(LocalVocabulary.Fold, [input], [], [result]),
            Specification(LocalVocabulary.Ignore, [input], [], []),
        ]);
    }

    /// <summary>Builds one specification of the local vocabulary.</summary>
    /// <param name="stage">The stage reference.</param>
    /// <param name="inputPorts">The input ports.</param>
    /// <param name="outputPorts">The output ports.</param>
    /// <param name="resultPorts">The result ports.</param>
    /// <returns>The specification.</returns>
    /// <remarks>
    /// The parameter contract and the required capability are the same for every local stage, so they are
    /// supplied here rather than repeated at each call site where a difference would look intentional.
    /// </remarks>
    private static StageSpecification Specification(
        StageRef stage,
        IReadOnlyList<InputPortSpecification> inputPorts,
        IReadOnlyList<OutputPortSpecification> outputPorts,
        IReadOnlyList<ResultPortSpecification> resultPorts) =>
        StageSpecification.Create(
            stage,
            inputPorts,
            outputPorts,
            resultPorts,
            LocalVocabulary.ParameterContract,
            [CapabilityToken.Nondeployable]);
}
