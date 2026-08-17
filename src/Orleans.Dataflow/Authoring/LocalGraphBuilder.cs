using System.Globalization;
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
/// Everything that needs a position happens here and nowhere else. An occurrence the author named keeps
/// its name; an unnamed one is numbered by its position in authoring order (<c>stage-0001</c>,
/// <c>stage-0002</c>, per ADR 0004 section 6). Each occurrence becomes a one-node fragment through
/// <see cref="GraphFragment.OfStage"/>, and the fragments are joined with
/// <see cref="GraphFragmentComposer.Append"/> and closed with
/// <see cref="GraphFragmentComposer.Close"/>. The algebra is the substrate; this type never builds a
/// document any other way.
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
/// positions in the whole chain rather than a separate count of the unnamed ones. That keeps a
/// lambda-only graph numbered exactly as it was before registered stages existed, and it keeps
/// <c>stage-0003</c> meaning "the third occurrence" in a mixed chain instead of "the third lambda".
/// </para>
/// <para>
/// <see cref="GraphFragmentComposer.Import"/> is deliberately unused. Import exists to make two copies of
/// one reusable fragment disjoint, and no fragment exists before closure here: identifiers are allocated
/// once, over the whole chain, so every one-node fragment is already disjoint from every other — unless
/// the author gave two occurrences one name, which the composer reports naming the collision. A reused
/// <see cref="Orleans.Dataflow.Flow{TIn, TOut}"/> of lambdas contributes its occurrences twice and they
/// are numbered twice, which is the flat numbering ADR 0004 asks for rather than a nested scope.
/// </para>
/// </remarks>
internal static class LocalGraphBuilder
{
    /// <summary>
    /// Closes a chain of stage occurrences into a runnable graph.
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
    /// Two occurrences carry the same explicit name, which the fragment algebra reports naming every
    /// collision; or the document the chain describes is not structurally valid, which is unreachable for
    /// a chain built through the authoring types, whose shapes are enforced by the C# type system. Both
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
    /// <see cref="CapabilityToken.Nondeployable"/> appears exactly when the chain holds a local stage,
    /// because every local stage specification requires it and no registered one does;
    /// <see cref="CapabilityToken.EphemeralIdentity"/> appears exactly when some occurrence had no name to
    /// keep. A fully registered, fully named chain therefore declares neither and is a pipeline candidate,
    /// and a chain that mixes the two declares what it actually contains.
    /// </para>
    /// <para>
    /// The binding table is built after composition rather than during identifier allocation, so that two
    /// occurrences sharing a name are reported by the composer — which names the collision — instead of by
    /// a dictionary complaining about a duplicate key.
    /// </para>
    /// <para>
    /// The result slot's producer is read from the allocated identifier of the last occurrence rather than
    /// from the closed document's last node. The producer of a slot is the occurrence the author closed
    /// the graph with, and reading it from the chain says so without depending on how the document happens
    /// to sort — which, once explicit names exist, is no longer the authoring order at all.
    /// </para>
    /// </remarks>
    internal static RunnableGraph Close(IReadOnlyList<StageOccurrence> stages, ResultSlotId? slotId)
    {
        NodeId[] ids = Allocate(stages);
        GraphFragment composed = FragmentOf(stages[0], ids[0]);

        for (int index = 1; index < stages.Count; index++)
        {
            composed = GraphFragmentComposer.Append(composed, FragmentOf(stages[index], ids[index]));
        }

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
            Slots(stages, ids, slotId));

        return new RunnableGraph(
            document,
            GraphDocumentSerializer.Fingerprint(document),
            bindings,
            Controls(stages));
    }

    /// <summary>Collects the type of every runtime control the chain declares, by name.</summary>
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
                    $"Two stages of this chain declare a runtime control named '{slot}', and a name resolves one control. Give each control a name of its own; the names are what a run handle resolves them by.");
            }
        }

        return controls;
    }

    /// <summary>Allocates the node identifier of every occurrence of a chain.</summary>
    /// <param name="stages">The occurrences in authoring order.</param>
    /// <returns>The identifiers, in the same positions.</returns>
    /// <exception cref="InvalidOperationException">
    /// An occurrence with no name of its own stands past <see cref="LocalVocabulary.MaxAutoNamedPosition"/>.
    /// </exception>
    /// <remarks>
    /// The bound is checked per occurrence rather than over the whole chain, because it is a statement
    /// about automatic numbering and not about length: a chain of ten thousand named occurrences numbers
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
                        $"An occurrence with no name of its own stands at position {position} of {stages.Count} in this chain, and automatic node identifiers reach position {LocalVocabulary.MaxAutoNamedPosition} at most. They are numbered '{LocalVocabulary.AutoNamePrefix}0001' upwards and zero-padded to four digits so that a document's node order is its authoring order; a longer chain cannot be numbered that way. Name the occurrences past that position explicitly, which the registered-stage authoring surface does."));
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
    /// <param name="slotId">The slot name, or <see langword="null"/> when the graph declares no result.</param>
    /// <returns>The slots: one per runtime control, and one more for the terminal's result when it has one.</returns>
    /// <exception cref="InvalidOperationException">
    /// A slot name was supplied for a terminal that declares no result port. That is unreachable through
    /// the authoring types, whose result-bearing overloads accept only a result-bearing sink, and it is a
    /// defect in this assembly rather than a mistake the author could make.
    /// </exception>
    /// <remarks>
    /// <para>
    /// A control is a result slot and is declared here beside the terminal's, which is what ADR 0002 meant
    /// by listing a queue control next to a fold result. The only difference is where the name came from:
    /// a terminal's is an argument of <c>To</c>, and a control's was written on the stage that produces it,
    /// because there is no closing call in the middle of a chain to hand one back from.
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
        ResultSlotId? slotId)
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

        if (slotId is not { } declared)
        {
            return [.. slots];
        }

        if (stages[^1].ResultPort is not { } result)
        {
            throw new InvalidOperationException(
                $"The graph was closed under the result name '{declared}' by an occurrence of '{stages[^1].Stage}', which declares no result port to expose.");
        }

        slots.Add(ResultSlotDefinition.Create(declared, result.ResultContract, PortAddress.Create(ids[^1], result.Id)));

        return [.. slots];
    }

    /// <summary>
    /// Builds the one-node fragment of a single occurrence, leaving exactly the ports its shape declares
    /// open.
    /// </summary>
    /// <param name="stage">The occurrence.</param>
    /// <param name="id">The identifier allocated to it.</param>
    /// <returns>The fragment.</returns>
    /// <remarks>
    /// <para>
    /// A result port is never an open port. Open ports are what a later connection consumes, and a result
    /// is exposed by declaring a slot against the closed graph, not by wiring an edge to it.
    /// </para>
    /// <para>
    /// The port names, the parameter contract, and the payload all come from the occurrence rather than
    /// from constants here, because none of them is the same for every shape: a local buffer and a local
    /// asynchronous stage carry the options the author chose under contracts of their own, and a
    /// registered stage carries whatever its specification declares, under whatever names it declares them.
    /// </para>
    /// </remarks>
    private static GraphFragment FragmentOf(StageOccurrence stage, NodeId id)
    {
        StageNode node = StageNode.Create(id, stage.Stage, stage.ParameterContract, stage.Parameters);

        PortId[] openInputs = stage.InputPort is { } input ? [input] : [];
        PortId[] openOutputs = stage.OutputPort is { } output ? [output] : [];

        return GraphFragment.OfStage(node, openInputs, openOutputs);
    }
}
