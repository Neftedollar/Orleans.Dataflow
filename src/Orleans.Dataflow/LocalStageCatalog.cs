using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;

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
/// One specification per stage of the vocabulary, all under the provider <c>local</c> at major version 1,
/// and every one of them derived from the same answers the authoring occurrence derives its node from: the
/// stage reference, where the shape stands in a chain, the parameter contract, its check, and the result
/// contract. Deriving rather than listing is what makes a specification and the occurrence validated
/// against it agree by construction; a stage added to the vocabulary appears here with the ports its place
/// implies, or fails to classify at all.
/// </para>
/// <para>
/// Every element port declares the same opaque element contract, because a local graph's element types live
/// in the C# type system and never in the document; and every stage requires the <c>nondeployable</c>
/// capability, which is how a document that contains one is stopped before it can be persisted, resumed, or
/// placed remotely.
/// </para>
/// <para>
/// Parameters split the vocabulary in two. Most shapes have nothing to declare — their behavior is a
/// delegate, and a delegate is never durable topology — so they carry the empty payload under
/// <c>local-parameters</c> and need no check. The shapes that are configured by numbers carry real payloads
/// under contracts of their own and each brings the very reader the runtime uses. The validator is what
/// makes a hand-written document's capacity of zero a diagnostic rather than a run that hangs.
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
    /// A catalog holding one specification for each local stage: the sources <c>from-enumerable</c>,
    /// <c>empty</c>, <c>single</c>, <c>repeat</c>, <c>range</c>, <c>from-task</c>, <c>failed</c>,
    /// <c>unfold</c>, <c>from-async-enumerable</c>, <c>from-factory</c>, <c>from-async-factory</c>,
    /// <c>never</c>, <c>cycle</c>, <c>unfold-async</c>, <c>queue</c>, and <c>from-channel</c>; the
    /// operators <c>select</c>, <c>where</c>, <c>scan</c>, <c>take</c>, <c>skip</c>, <c>take-while</c>,
    /// <c>take-through</c>, <c>skip-while</c>, <c>distinct</c>, <c>buffer</c>, <c>select-async</c>, and
    /// <c>select-async-unordered</c>; and the sinks <c>fold</c>, <c>ignore</c>, <c>for-each</c>,
    /// <c>for-each-async</c>, <c>first</c>, <c>first-or-default</c>, <c>count</c>, <c>last</c>,
    /// <c>last-or-default</c>, <c>collect</c>, and <c>to-channel</c>.
    /// </value>
    /// <remarks>
    /// The catalog is immutable and stateless, so one instance serves every caller; a
    /// <see cref="CatalogFingerprint"/> taken over it is stable for the lifetime of the assembly version.
    /// </remarks>
    public static IStageCatalog Instance { get; } = Build();

    /// <summary>Builds one specification for every shape the local vocabulary declares.</summary>
    /// <returns>The catalog.</returns>
    /// <remarks>
    /// The port lists follow from where a shape stands: a source produces and does not consume, an operator
    /// does both, a junction consumes one stream and produces several, and a sink consumes and produces
    /// nothing, with a result port when it exposes a value. A buffer and an asynchronous mapping are
    /// operator-shaped, because from the document's point of view they are: one element in, one element
    /// out, whatever they do about queueing and concurrency in between. No port of a chain shape is
    /// optional or ignorable, so the graph compiler's connectivity rule requires every one of them to be
    /// wired — which is exactly the linear chain the authoring types can build, and nothing looser. The
    /// legs of a junction beyond its first two are the one exception and are ignorable, because the edges
    /// of a document are what state how many legs a given junction has.
    /// </remarks>
    private static StageCatalog Build()
    {
        LocalStageKind[] kinds = Enum.GetValues<LocalStageKind>();
        StageSpecification[] specifications = new StageSpecification[kinds.Length];

        for (int index = 0; index < kinds.Length; index++)
        {
            specifications[index] = Specification(kinds[index]);
        }

        return StageCatalog.Create(specifications);

        StageSpecification Specification(LocalStageKind kind)
        {
            IReadOnlyList<InputPortSpecification> inputs = LocalVocabulary.InputPortsOf(kind);
            IReadOnlyList<OutputPortSpecification> outputs = LocalVocabulary.OutputPortsOf(kind);
            ResultPortSpecification[] results = LocalVocabulary.ResultPortOf(kind) is { } result
                ? [result]
                : [];

            return LocalVocabulary.ParameterValidatorOf(kind) is { } validator
                ? StageSpecification.Create(
                    LocalVocabulary.StageOf(kind),
                    inputs,
                    outputs,
                    results,
                    LocalVocabulary.ParameterContractOf(kind),
                    LocalVocabulary.RequiredCapabilities,
                    validator)
                : StageSpecification.Create(
                    LocalVocabulary.StageOf(kind),
                    inputs,
                    outputs,
                    results,
                    LocalVocabulary.ParameterContractOf(kind),
                    LocalVocabulary.RequiredCapabilities);
        }
    }
}
