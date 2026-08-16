using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// Graph closure: the one place where a chain of stage occurrences becomes node identifiers, fragments, a
/// graph document, and a fingerprint.
/// </summary>
/// <remarks>
/// <para>
/// Everything that needs a position happens here and nowhere else. Identifiers are allocated in authoring
/// order (<c>stage-1</c>, <c>stage-2</c>, per ADR 0004 section 6), each occurrence becomes a one-node
/// fragment through <see cref="GraphFragment.OfStage"/>, and the fragments are joined with
/// <see cref="GraphFragmentComposer.Append"/> and closed with
/// <see cref="GraphFragmentComposer.Close"/>. The algebra is the substrate; this type never builds a
/// document any other way.
/// </para>
/// <para>
/// <see cref="GraphFragmentComposer.Import"/> is deliberately unused. Import exists to make two copies of
/// one reusable fragment disjoint, and no fragment exists before closure here: identifiers are allocated
/// once, over the whole chain, so every one-node fragment is already disjoint from every other. A reused
/// <see cref="Orleans.Dataflow.Flow{TIn, TOut}"/> contributes its occurrences twice and they are numbered
/// twice, which is the flat numbering ADR 0004 asks for rather than a nested scope.
/// </para>
/// </remarks>
internal static class LocalGraphBuilder
{
    /// <summary>
    /// Closes a chain of local stage occurrences into a runnable graph.
    /// </summary>
    /// <param name="stages">
    /// The occurrences in authoring order. The chain is linear and complete: the first occurrence declares
    /// no input port, the last declares no output port, and every adjacent pair connects.
    /// </param>
    /// <param name="slotId">
    /// The name to expose the last occurrence's result port under, or <see langword="null"/> when the graph
    /// declares no result.
    /// </param>
    /// <returns>The closed, fingerprinted graph with its authoring-side binding table.</returns>
    /// <exception cref="ArgumentException">
    /// The document the chain describes is not structurally valid. That is unreachable for a chain built
    /// through the authoring types, whose shapes are enforced by the C# type system; the exception is the
    /// algebra's and it is deliberately not translated, because a defect here is a defect in this type.
    /// </exception>
    /// <remarks>
    /// <para>
    /// Every closed local graph declares both <see cref="CapabilityToken.Nondeployable"/> and
    /// <see cref="LocalVocabulary.EphemeralIdentity"/>. The first is required by every local stage
    /// specification: a delegate is not durable topology. The second follows from the numbering above,
    /// because this slice of the API has no way to name an occurrence explicitly at all, so every
    /// occurrence of every graph it builds is positional. Explicit stage naming is the registered-stage
    /// surface's concern, and when it arrives this is the line that stops being unconditional.
    /// </para>
    /// <para>
    /// The result slot's producer is read from the allocated identifier of the last occurrence rather than
    /// from the closed document's last node. The document orders nodes ordinally by identifier text, and
    /// <c>stage-10</c> sorts before <c>stage-2</c>, so the last node of a document is not the last stage of
    /// a chain once a chain passes nine occurrences.
    /// </para>
    /// </remarks>
    internal static RunnableGraph Close(IReadOnlyList<LocalStageDescriptor> stages, ResultSlotId? slotId)
    {
        NodeId[] ids = new NodeId[stages.Count];
        Dictionary<NodeId, LocalStageDescriptor> bindings = new(stages.Count);

        for (int index = 0; index < stages.Count; index++)
        {
            ids[index] = LocalVocabulary.AutoName(index + 1);
            bindings.Add(ids[index], stages[index]);
        }

        GraphFragment composed = FragmentOf(stages[0], ids[0]);

        for (int index = 1; index < stages.Count; index++)
        {
            composed = GraphFragmentComposer.Append(composed, FragmentOf(stages[index], ids[index]));
        }

        ResultSlotDefinition[] slots = slotId is { } declared
            ?
            [
                ResultSlotDefinition.Create(
                    declared,
                    LocalVocabulary.FoldResultContract,
                    PortAddress.Create(ids[^1], LocalVocabulary.ResultPort)),
            ]
            : [];

        GraphDocument document = GraphFragmentComposer.Close(
            composed,
            LocalVocabulary.AnonymousGraph,
            LocalVocabulary.FirstRevision,
            [CapabilityToken.Nondeployable, LocalVocabulary.EphemeralIdentity],
            slots);

        return new RunnableGraph(document, GraphDocumentSerializer.Fingerprint(document), bindings);
    }

    /// <summary>
    /// Builds the one-node fragment of a single occurrence, leaving exactly the ports its shape declares
    /// open.
    /// </summary>
    /// <param name="stage">The occurrence.</param>
    /// <param name="id">The identifier allocated to it.</param>
    /// <returns>The fragment.</returns>
    /// <remarks>
    /// A result port is never an open port. Open ports are what a later connection consumes, and a result
    /// is exposed by declaring a slot against the closed graph, not by wiring an edge to it.
    /// </remarks>
    private static GraphFragment FragmentOf(LocalStageDescriptor stage, NodeId id)
    {
        StageNode node = StageNode.Create(
            id,
            stage.Stage,
            LocalVocabulary.ParameterContract,
            LocalVocabulary.EmptyParameters);

        PortId[] openInputs = stage.HasInput ? [LocalVocabulary.InputPort] : [];
        PortId[] openOutputs = stage.HasOutput ? [LocalVocabulary.OutputPort] : [];

        return GraphFragment.OfStage(node, openInputs, openOutputs);
    }
}
