using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The half of the local vocabulary a document states completely: which stages those are, what a host
/// publishes for them, and how a node of one becomes the occurrence a run plans.
/// </summary>
/// <remarks>
/// <para>
/// ADR 0009. A pipeline of registered stages is deployable and a buffer between two of them used to make it
/// not, although a buffer carries no delegate at all: its whole configuration is a number, the document
/// states that number, and the planner has always read it from there rather than from a CLR field. This is
/// the seam that lets the deployable path rebuild such an occurrence from the document alone.
/// </para>
/// <para>
/// <b>There is no <c>local</c> runtime factory and there is deliberately not going to be one.</b> A buffer
/// is not a source, an element, or a terminal; it is a queueing boundary the engine implements structurally,
/// which is exactly why a cycle is relieved by one and not by a delay. Publishing it through the provider
/// seam would mean adding engine primitives to an interface whose own documentation says a stage wanting a
/// seventh shape is asking for a new engine primitive. Rehydration asks for nothing: the descriptor this
/// builds is the descriptor the authoring surface builds, so fusion, the buffer boundary rule, cycle relief
/// and every payload reader are one implementation reading one payload rather than two kept in step.
/// </para>
/// <para>
/// <b>What is refused is refused by name.</b> A silo that accepted a document it would then fail to build
/// would turn a reconcilable mistake into a run that dies at materialization, so
/// <see cref="Refusal(GraphDocument)"/> answers the question before anything is started and says which node,
/// which stage, and which of the three reasons applies.
/// </para>
/// </remarks>
internal static class LocalPlumbing
{
    /// <summary>Gets the specifications a host publishes so that plumbing resolves in its catalog.</summary>
    /// <value>
    /// One specification per shape <see cref="LocalVocabulary.RunsFromTheDocumentAlone"/> admits, and
    /// nothing else. Which shapes those are is not written down here: the enumeration is walked and the
    /// predicate asked, so a shape that becomes rehydratable is published the moment it does and a shape
    /// that grows a delegate stops being published in the same edit.
    /// </value>
    /// <remarks>
    /// A separate catalog rather than <see cref="LocalStageCatalog.Instance"/>, because the two answer
    /// different questions. The full catalog is what a locally authored graph validates against — every
    /// shape, including the ones whose behavior is a lambda — and a host that published it would be
    /// promising to run a <c>local/select@v1</c> it cannot build. This one is exactly the promise a host can
    /// keep.
    /// </remarks>
    internal static StageCatalog Catalog { get; } = Build();

    /// <summary>Reports whether a stage reference names plumbing this runtime rebuilds from a document.</summary>
    /// <param name="stage">The reference as a node declares it.</param>
    /// <returns><see langword="true"/> when a node of it needs no binding and no runtime factory.</returns>
    /// <remarks>
    /// Read by a host deciding whether a node needs a factory registered for its provider. A <c>local</c>
    /// node answers <see langword="true"/> here exactly when nothing has to be registered to run it, which
    /// is what makes "this silo can execute every node of this document" a question a registry can answer.
    /// </remarks>
    internal static bool Rehydrates(StageRef stage) =>
        stage.Provider == LocalVocabulary.Provider &&
        LocalVocabulary.TryReadStage(stage.ToString(), out LocalStageKind kind) &&
        LocalVocabulary.RunsFromTheDocumentAlone(kind);

    /// <summary>Names every <c>local</c> node of a document that no deployable run could build.</summary>
    /// <param name="document">The document a host is deciding whether to accept.</param>
    /// <returns>The refusal, or <see langword="null"/> when every <c>local</c> node rehydrates.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="document"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// Every offending node is named rather than the first, because a caller reconciling a document with the
    /// deployable plane is fixing a graph and not a line. The reason is per node and is the sharp one: a
    /// delegate, an untellable default, a control nobody could reach, or a stage this build does not declare
    /// at all.
    /// </para>
    /// <para>
    /// It is asked <b>before</b> a document is validated against a host's catalog, which is not where a
    /// refusal would naturally go. The reason is that the sentence is better: whether some deployment
    /// happens to publish <c>local/select@v1</c> is beside the point, because the thing that cannot be
    /// deployed is a property of the stage rather than of the catalog it was looked up in, and "this stage's
    /// behavior is a delegate" is what an author can act on where "no such stage here" is not.
    /// </para>
    /// </remarks>
    internal static string? Refusal(GraphDocument document)
    {
        ArgumentNullException.ThrowIfNull(document);

        List<string> refused = [];

        foreach (StageNode node in document.Nodes)
        {
            if (node.Stage.Provider != LocalVocabulary.Provider)
            {
                continue;
            }

            if (!LocalVocabulary.TryReadStage(node.Stage.ToString(), out LocalStageKind kind))
            {
                refused.Add($"'{node.Id}' is an occurrence of '{node.Stage}', which this build of the library declares no local stage for");

                continue;
            }

            if (!LocalVocabulary.RunsFromTheDocumentAlone(kind))
            {
                refused.Add($"'{node.Id}' is an occurrence of '{node.Stage}', and {Why(kind)}");
            }
        }

        return refused.Count == 0
            ? null
            : $"The document names {refused.Count} local stage{(refused.Count == 1 ? string.Empty : "s")} that no deployment could build, because a local stage is executable only where the graph that declared it was authored: {string.Join("; ", refused)}. The local stages that do deploy are the ones a document states completely — a buffer, a take, a delay, a junction — and they are the only ones this runtime rebuilds from a document.";
    }

    /// <summary>Rebuilds the occurrence of every <c>local</c> node a document declares.</summary>
    /// <param name="document">The document being materialized.</param>
    /// <returns>
    /// The binding table the planner reads first, holding one entry per <c>local</c> node and nothing else;
    /// empty for a document made entirely of registered stages.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="document"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// Some <c>local</c> node of the document is not one a deployable run can build.
    /// </exception>
    /// <remarks>
    /// The table is what makes the deployable path and the local one the same path. A locally authored graph
    /// carries this table from its authoring surface; a deployable document has it built here, from the same
    /// vocabulary, holding descriptors of the same type — and the planner asks the table before it asks the
    /// binder either way, so nothing downstream of this method knows which plane it is compiling.
    /// </remarks>
    internal static IReadOnlyDictionary<NodeId, LocalStageDescriptor> Bindings(GraphDocument document)
    {
        ArgumentNullException.ThrowIfNull(document);

        Dictionary<NodeId, LocalStageDescriptor> bindings = [];

        foreach (StageNode node in document.Nodes)
        {
            if (node.Stage.Provider != LocalVocabulary.Provider)
            {
                continue;
            }

            if (LocalVocabulary.TryReadStage(node.Stage.ToString(), out LocalStageKind kind) &&
                LocalVocabulary.RunsFromTheDocumentAlone(kind))
            {
                bindings.Add(node.Id, LocalStageDescriptor.Rehydrated(kind, node.Parameters));

                continue;
            }

            // The whole document is described rather than this one node, because a caller reaching here has
            // usually not asked Refusal first and is owed the same sentence it would have given. Reading the
            // document twice is what a refusal costs and nothing an accepted one pays.
            throw new InvalidOperationException(
                Refusal(document) ?? $"The node '{node.Id}' is an occurrence of '{node.Stage}', which no deployment could build.");
        }

        return bindings;
    }

    /// <summary>Builds the specification of every shape a document states completely.</summary>
    /// <returns>The catalog.</returns>
    private static StageCatalog Build()
    {
        List<StageSpecification> specifications = [];

        foreach (LocalStageKind kind in Enum.GetValues<LocalStageKind>())
        {
            if (LocalVocabulary.RunsFromTheDocumentAlone(kind))
            {
                specifications.Add(LocalVocabulary.SpecificationOf(kind));
            }
        }

        return StageCatalog.Create(specifications);
    }

    /// <summary>Says why one behavior-bearing or unrecoverable shape cannot be rebuilt from a document.</summary>
    /// <param name="kind">The shape, which is known not to be one a document states completely.</param>
    /// <returns>The clause, read after "and".</returns>
    /// <remarks>
    /// Three reasons and not one, because they are three different mistakes and an author fixes them
    /// differently: a delegate is published as a registered stage, an untellable default is written as a
    /// stage of one's own that names the value, and a control is not something a run on a silo has anybody
    /// to hand back to at all.
    /// </remarks>
    private static string Why(LocalStageKind kind) => kind switch
    {
        LocalStageKind.FirstOrDefault or LocalStageKind.LastOrDefault =>
            "the value it resolves when it saw no element is the default of an element type, which a document names nowhere; a rehydrated occurrence would resolve nothing where the authored one resolved that value",
        _ when !LocalVocabulary.CarriesBehavior(kind) =>
            "it produces a runtime control, which is an object an author reaches by name inside the process that built the graph; a run in a deployment has no such author, so the control would be built and never reached",
        _ =>
            "its behavior is a delegate, a sequence, or a value the authoring process closed over, and no document carries one",
    };
}
