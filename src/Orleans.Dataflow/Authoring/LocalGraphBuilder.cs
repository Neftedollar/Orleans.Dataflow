using System.Globalization;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// Graph closure: the one place where a shape of stage occurrences becomes node identifiers, fragments, a
/// graph document, and a fingerprint.
/// </summary>
/// <remarks>
/// <para>
/// Everything that needs a position happens here and nowhere else. An occurrence the author named keeps
/// its name; an unnamed one is numbered by its position in authoring order (<c>stage-0001</c>,
/// <c>stage-0002</c>, per ADR 0004 section 6). Each occurrence becomes a one-node fragment through
/// <see cref="GraphFragment.OfStage"/>, the shape's links are laid down with
/// <see cref="GraphFragmentComposer.Connect"/> and <see cref="GraphFragmentComposer.Wire"/>, and the result
/// is closed with <see cref="GraphFragmentComposer.Close"/>. The algebra is the substrate; this type never
/// builds a document any other way.
/// </para>
/// <para>
/// The zero padding buys one invariant, and it is worth stating as an invariant rather than as a detail
/// of the spelling: for every graph whose occurrences are all automatically named, the document's
/// canonical node order — ordinal over identifier text — is the authoring order of the occurrences it was
/// built from. An explicit name sorts wherever its text sorts, which is the price of an identity that
/// survives an edit and is exactly why the two kinds of name are two kinds.
/// </para>
/// <para>
/// A registered occurrence numbers nothing, but it does occupy a position: the automatic numbers are the
/// positions in the whole shape rather than a separate count of the unnamed ones. That keeps a
/// lambda-only graph numbered exactly as it was before registered stages existed, and it keeps
/// <c>stage-0003</c> meaning "the third occurrence" in a mixed graph instead of "the third lambda".
/// </para>
/// <para>
/// Which composition operator lays down a link is decided by the link, not by the kind of graph: a link
/// between two occurrences that are still in separate fragments is a <see cref="GraphFragmentComposer.Connect"/>,
/// which is also what reports two occurrences sharing a name, and a link whose two ends are already in one
/// fragment is a <see cref="GraphFragmentComposer.Wire"/>. A chain only ever produces the first kind, so its
/// diagnostics are exactly what they always were; a diamond produces one of the second kind, which is the
/// re-convergence a tree cannot express and the reason that operator exists.
/// </para>
/// <para>
/// <see cref="GraphFragmentComposer.Import"/> is deliberately unused. Import exists to make two copies of
/// one reusable fragment disjoint, and no fragment exists before closure here: identifiers are allocated
/// once, over the whole shape, so every one-node fragment is already disjoint from every other — unless
/// the author gave two occurrences one name, which the composer reports naming the collision. A reused
/// <see cref="Orleans.Dataflow.Flow{TIn, TOut}"/> of lambdas contributes its occurrences twice and they
/// are numbered twice, which is the flat numbering ADR 0004 asks for rather than a nested scope.
/// </para>
/// </remarks>
internal static class LocalGraphBuilder
{
    /// <summary>Gets the request list of a graph that declares no result of its own.</summary>
    /// <remarks>
    /// Runtime controls are declared from the occurrences that produce them and are not requests, so a
    /// graph closed with a resultless sink asks for nothing here and can still declare a queue's control.
    /// </remarks>
    internal static IReadOnlyList<LocalSlotRequest> NoSlots { get; } =
        Array.AsReadOnly<LocalSlotRequest>([]);

    /// <summary>
    /// Closes a shape of stage occurrences into a runnable graph.
    /// </summary>
    /// <param name="shape">
    /// The shape, which must be complete: every port that a link names is connected by that link, and
    /// nothing is left open.
    /// </param>
    /// <param name="slots">
    /// The results to expose, in the order the author wrote them; empty when the graph declares none.
    /// </param>
    /// <returns>The closed, fingerprinted graph with its authoring-side binding table.</returns>
    /// <exception cref="ArgumentException">
    /// Two occurrences carry the same explicit name, which the fragment algebra reports naming every
    /// collision; or the document the shape describes is not structurally valid, which is unreachable for
    /// a shape built through the authoring types, whose shapes are enforced by the C# type system. Both
    /// exceptions are the algebra's and are deliberately not translated.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// An automatically numbered occurrence stands past <see cref="LocalVocabulary.MaxAutoNamedPosition"/>,
    /// which is further than automatic numbering can name while keeping document order equal to authoring
    /// order.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The capability tokens are derived from the occurrences rather than fixed here.
    /// <see cref="CapabilityToken.Nondeployable"/> appears exactly when the shape holds a local stage,
    /// because every local stage specification requires it and no registered one does;
    /// <see cref="CapabilityToken.EphemeralIdentity"/> appears exactly when some occurrence had no name to
    /// keep. A fully registered, fully named graph therefore declares neither and is a pipeline candidate,
    /// and a graph that mixes the two declares what it actually contains. Every junction stage is a local
    /// stage, so a graph with a junction in it declares <c>nondeployable</c> whatever its other stages are,
    /// until a provider registers junctions of its own.
    /// </para>
    /// <para>
    /// The binding table is built after composition rather than during identifier allocation, so that two
    /// occurrences sharing a name are reported by the composer — which names the collision — instead of by
    /// a dictionary complaining about a duplicate key.
    /// </para>
    /// <para>
    /// Each result slot's producer is read from the allocated identifier of the occurrence the request
    /// names rather than from the closed document's last node. The producer of a slot is the occurrence
    /// that terminates the chain or the branch it belongs to, and reading it from the shape says so without
    /// depending on how the document happens to sort — which, once explicit names or several terminals
    /// exist, is no longer the authoring order at all.
    /// </para>
    /// <para>
    /// A slot a branch declared is bound to the graph last, after the document exists, because that is the
    /// first moment there is an identity to bind it to. A shape that fails to close therefore leaves every
    /// branch slot unbound, and the branch can be handed to another junction call instead.
    /// </para>
    /// </remarks>
    internal static RunnableGraph Close(LocalGraphShape shape, IReadOnlyList<LocalSlotRequest> slots)
    {
        IReadOnlyList<StageOccurrence> stages = shape.Stages;
        IReadOnlyList<LocalSlotRequest> declared = shape.Slots.Count == 0 ? slots : [.. shape.Slots, .. slots];
        NodeId[] ids = Allocate(stages);
        GraphFragment composed = Compose(shape, ids);

        Dictionary<NodeId, LocalStageDescriptor> bindings = new(stages.Count);

        for (int index = 0; index < stages.Count; index++)
        {
            if (stages[index] is LocalStageDescriptor descriptor)
            {
                bindings.Add(ids[index], descriptor);
            }
        }

        GraphDocument document = GraphFragmentComposer.Close(
            composed,
            LocalVocabulary.AnonymousGraph,
            LocalVocabulary.FirstRevision,
            Capabilities(stages),
            Slots(stages, ids, declared));

        RunnableGraph graph = new(
            document,
            GraphDocumentSerializer.Fingerprint(document),
            bindings,
            Controls(stages));

        for (int index = 0; index < declared.Count; index++)
        {
            declared[index].Binding?.Bind(graph.Fingerprint, graph.AuthoringNonce);
        }

        return graph;
    }

    /// <summary>Lays a shape's links down through the fragment algebra.</summary>
    /// <param name="shape">The shape to compose.</param>
    /// <param name="ids">The identifiers allocated to its occurrences, in the same positions.</param>
    /// <returns>The one composed fragment, with no port left open.</returns>
    /// <exception cref="InvalidOperationException">
    /// The shape leaves a port open or falls into more than one piece, neither of which any authoring value
    /// can express: both are defects in this assembly rather than mistakes an author could make.
    /// </exception>
    /// <remarks>
    /// Each occurrence starts as a one-node fragment whose open ports are exactly the ports its links use,
    /// so a junction opens the legs this graph wires and not the eight its specification declares. The links
    /// are then laid down in authoring order, which decides nothing about the document — a fragment sorts its
    /// nodes and edges canonically — and only decides which defect is reported first.
    /// </remarks>
    private static GraphFragment Compose(LocalGraphShape shape, NodeId[] ids)
    {
        IReadOnlyList<StageOccurrence> stages = shape.Stages;

        if (shape.OpenOutputs.Count > 0)
        {
            throw new InvalidOperationException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"A graph is closed only when nothing is left to connect, and this shape still has {shape.OpenOutputs.Count} open outputs."));
        }

        List<PortId>[] openInputs = new List<PortId>[stages.Count];
        List<PortId>[] openOutputs = new List<PortId>[stages.Count];

        for (int index = 0; index < stages.Count; index++)
        {
            openInputs[index] = [];
            openOutputs[index] = [];
        }

        for (int index = 0; index < shape.Links.Count; index++)
        {
            LocalStageLink link = shape.Links[index];

            openOutputs[link.From].Add(link.FromPort);
            openInputs[link.To].Add(link.ToPort);
        }

        GraphFragment?[] fragments = new GraphFragment?[stages.Count];
        int[] owner = new int[stages.Count];

        for (int index = 0; index < stages.Count; index++)
        {
            fragments[index] = GraphFragment.OfStage(
                StageNode.Create(ids[index], stages[index].Stage, stages[index].ParameterContract, stages[index].Parameters),
                openInputs[index],
                openOutputs[index]);
            owner[index] = index;
        }

        for (int index = 0; index < shape.Links.Count; index++)
        {
            LocalStageLink link = shape.Links[index];
            PortAddress from = PortAddress.Create(ids[link.From], link.FromPort);
            PortAddress to = PortAddress.Create(ids[link.To], link.ToPort);
            int producing = owner[link.From];
            int consuming = owner[link.To];

            if (producing == consuming)
            {
                fragments[producing] = GraphFragmentComposer.Wire(fragments[producing]!, from, to);

                continue;
            }

            fragments[producing] = GraphFragmentComposer.Connect(
                fragments[producing]!,
                from,
                fragments[consuming]!,
                to);
            fragments[consuming] = null;

            for (int stage = 0; stage < owner.Length; stage++)
            {
                if (owner[stage] == consuming)
                {
                    owner[stage] = producing;
                }
            }
        }

        GraphFragment? composed = null;

        for (int index = 0; index < fragments.Length; index++)
        {
            if (fragments[index] is not { } piece)
            {
                continue;
            }

            if (composed is not null)
            {
                throw new InvalidOperationException(
                    "This shape falls into more than one piece, and a graph is one connected document. Every authoring value connects what it adds, so a shape that does not is a defect in this assembly.");
            }

            composed = piece;
        }

        return composed ??
            throw new InvalidOperationException(
                "This shape declares no occurrence at all, and a graph always describes at least one stage.");
    }

    /// <summary>Collects the type of every runtime control the shape declares, by name.</summary>
    /// <param name="stages">The occurrences in authoring order.</param>
    /// <returns>The control types, keyed by the name each is declared under.</returns>
    /// <exception cref="ArgumentException">
    /// Two occurrences declare a control under one name. Reported here rather than by the document, whose
    /// own uniqueness rule would report it as a repeated slot without saying that both were controls.
    /// </exception>
    /// <remarks>
    /// The registry never enters the document, exactly as the binding table never does: a CLR type is not
    /// durable topology. What the document says about a control is its name, its port, and its contract;
    /// what this says is which type an author may ask for it as, so that asking for the wrong one is a
    /// diagnostic naming both types instead of a cast that fails inside a run.
    /// </remarks>
    private static Dictionary<ResultSlotId, Type> Controls(IReadOnlyList<StageOccurrence> stages)
    {
        Dictionary<ResultSlotId, Type> controls = [];

        for (int index = 0; index < stages.Count; index++)
        {
            if (stages[index] is { ControlSlot: { } slot, ControlType: { } type } &&
                !controls.TryAdd(slot, type))
            {
                throw new ArgumentException(
                    $"Two stages of this graph declare a runtime control named '{slot}', and a name resolves one control. Give each control a name of its own; the names are what a run handle resolves them by.");
            }
        }

        return controls;
    }

    /// <summary>Allocates the node identifier of every occurrence of a shape.</summary>
    /// <param name="stages">The occurrences in authoring order.</param>
    /// <returns>The identifiers, in the same positions.</returns>
    /// <exception cref="InvalidOperationException">
    /// An occurrence with no name of its own stands past <see cref="LocalVocabulary.MaxAutoNamedPosition"/>.
    /// </exception>
    /// <remarks>
    /// The bound is checked per occurrence rather than over the whole shape, because it is a statement
    /// about automatic numbering and not about size: a graph of ten thousand named occurrences numbers
    /// nothing and breaks nothing.
    /// </remarks>
    private static NodeId[] Allocate(IReadOnlyList<StageOccurrence> stages)
    {
        NodeId[] ids = new NodeId[stages.Count];

        for (int index = 0; index < stages.Count; index++)
        {
            if (stages[index].Name is { } declared)
            {
                ids[index] = declared;

                continue;
            }

            int position = index + 1;

            if (position > LocalVocabulary.MaxAutoNamedPosition)
            {
                throw new InvalidOperationException(
                    string.Create(
                        CultureInfo.InvariantCulture,
                        $"An occurrence with no name of its own stands at position {position} of {stages.Count} in this graph, and automatic node identifiers reach position {LocalVocabulary.MaxAutoNamedPosition} at most. They are numbered '{LocalVocabulary.AutoNamePrefix}0001' upwards and zero-padded to four digits so that a document's node order is its authoring order; a longer graph cannot be numbered that way. Name the occurrences past that position explicitly, which the registered-stage authoring surface does."));
            }

            ids[index] = LocalVocabulary.AutoName(position);
        }

        return ids;
    }

    /// <summary>Collects the capability tokens the closed document declares.</summary>
    /// <param name="stages">The occurrences in authoring order.</param>
    /// <returns>The distinct tokens, in no particular order.</returns>
    /// <remarks>
    /// Two sources, and only two. A stage requires what its specification says it requires, which the
    /// graph compiler's <c>undeclared-capability</c> rule makes mandatory rather than optional: a document
    /// that declared less than its stages require would not validate against the catalog those stages came
    /// from. An unnamed occurrence adds <see cref="CapabilityToken.EphemeralIdentity"/>, which is a fact
    /// about the identifiers this closure just allocated and about nothing else. The order is the
    /// document's to fix, so none is imposed here.
    /// </remarks>
    private static CapabilityToken[] Capabilities(IReadOnlyList<StageOccurrence> stages)
    {
        HashSet<CapabilityToken> declared = [];

        for (int index = 0; index < stages.Count; index++)
        {
            StageOccurrence stage = stages[index];

            for (int token = 0; token < stage.RequiredCapabilities.Count; token++)
            {
                declared.Add(stage.RequiredCapabilities[token]);
            }

            if (stage.Name is null)
            {
                declared.Add(LocalVocabulary.EphemeralIdentity);
            }
        }

        return [.. declared];
    }

    /// <summary>Builds every result slot a closed graph declares.</summary>
    /// <param name="stages">The occurrences in authoring order.</param>
    /// <param name="ids">The identifiers allocated to them, in the same positions.</param>
    /// <param name="requests">The results the closing call asked for, in the author's order.</param>
    /// <returns>The slots: one per runtime control, and one more per request.</returns>
    /// <exception cref="InvalidOperationException">
    /// A slot name was requested of an occurrence that declares no result port. That is unreachable through
    /// the authoring types, whose result-bearing overloads accept only a result-bearing sink, and it is a
    /// defect in this assembly rather than a mistake the author could make.
    /// </exception>
    /// <remarks>
    /// <para>
    /// A control is a result slot and is declared here beside the requested ones, which is what ADR 0002
    /// meant by listing a queue control next to a fold result. The only difference is where the name came
    /// from: a terminal's is an argument of <c>To</c>, and a control's was written on the stage that
    /// produces it, because there is no closing call in the middle of a chain to hand one back from.
    /// </para>
    /// <para>
    /// The port and the contract come from each occurrence's own declaration rather than from a constant,
    /// because a registered stage names its result port and its result contract whatever it likes, and a
    /// slot whose contract did not match the port's would be a <c>result-contract-mismatch</c> the moment
    /// the document met the catalog.
    /// </para>
    /// </remarks>
    private static ResultSlotDefinition[] Slots(
        IReadOnlyList<StageOccurrence> stages,
        NodeId[] ids,
        IReadOnlyList<LocalSlotRequest> requests)
    {
        List<ResultSlotDefinition> slots = [];

        for (int index = 0; index < stages.Count; index++)
        {
            if (stages[index] is { ControlSlot: { } control, ResultPort: { } port })
            {
                slots.Add(
                    ResultSlotDefinition.Create(
                        control,
                        port.ResultContract,
                        PortAddress.Create(ids[index], port.Id)));
            }
        }

        for (int index = 0; index < requests.Count; index++)
        {
            LocalSlotRequest request = requests[index];

            if (stages[request.Stage].ResultPort is not { } result)
            {
                throw new InvalidOperationException(
                    $"The graph was closed under the result name '{request.Id}' by an occurrence of '{stages[request.Stage].Stage}', which declares no result port to expose.");
            }

            slots.Add(
                ResultSlotDefinition.Create(
                    request.Id,
                    result.ResultContract,
                    PortAddress.Create(ids[request.Stage], result.Id)));
        }

        return [.. slots];
    }
}
