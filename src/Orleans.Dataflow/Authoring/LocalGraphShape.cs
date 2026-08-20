using System.Collections.ObjectModel;
using System.Globalization;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The partial local graph an authoring value carries: the occurrences in authoring order, the wiring
/// between them, and the output ports still open for what comes next.
/// </summary>
/// <remarks>
/// <para>
/// A linear chain is the special case with one open output and one link per adjacent pair, and it is how
/// every <see cref="Orleans.Dataflow.Source{T}"/> starts. The shape exists because a junction is not a
/// chain: a fan-out has one input and several legs, a fan-in has several inputs and one output, and a
/// diamond re-converges. All four facts are edges, so an authoring value that can hold a junction has to
/// hold edges rather than an ordered list alone.
/// </para>
/// <para>
/// A shape is immutable and every operation allocates a new one, exactly as
/// <see cref="LocalStageChain"/> does and for the same reason: a value composed into two graphs is
/// byte-for-byte the value it was before either, so <c>a.Merge(b)</c> cannot disturb <c>a</c> or <c>b</c>.
/// </para>
/// <para>
/// Positions are the only identity a shape has. Node identifiers are allocated once, over the whole shape,
/// when <see cref="LocalGraphBuilder"/> closes it, and the allocation walks <see cref="Stages"/> in order —
/// so the order occurrences are added in is the order they are numbered in, and reordering the arguments of
/// a junction call reorders identities exactly as reordering a chain does.
/// </para>
/// <para>
/// <see cref="Orleans.Dataflow.Flow{TIn, TOut}"/> and <see cref="Orleans.Dataflow.Sink{T}"/> deliberately do
/// not carry a shape. Both are linear by construction — a flow has one input and one output and a sink has
/// one input — so a chain says everything about them, and a value that cannot branch should not be able to
/// hold a branch.
/// </para>
/// </remarks>
internal sealed class LocalGraphShape
{
    /// <summary>Initializes a new instance of the <see cref="LocalGraphShape"/> class.</summary>
    /// <param name="stages">The occurrences in authoring order.</param>
    /// <param name="links">The wiring between them.</param>
    /// <param name="openOutputs">The output ports still open, in the order they were declared.</param>
    /// <param name="slots">The results already asked for, in the order they were asked for.</param>
    private LocalGraphShape(
        IReadOnlyList<StageOccurrence> stages,
        IReadOnlyList<LocalStageLink> links,
        IReadOnlyList<LocalOpenOutput> openOutputs,
        IReadOnlyList<LocalSlotRequest> slots)
    {
        Stages = stages;
        Links = links;
        OpenOutputs = openOutputs;
        Slots = slots;
    }

    /// <summary>Gets the occurrences of this shape, in authoring order.</summary>
    internal IReadOnlyList<StageOccurrence> Stages { get; }

    /// <summary>Gets the wiring internal to this shape, in the order it was declared.</summary>
    /// <value>
    /// One link per connection. The order is the order the connections were authored in and is not
    /// canonical: a closed document sorts its edges, so this order reaches the document nowhere. It decides
    /// only which composition step reports a defect first.
    /// </value>
    internal IReadOnlyList<LocalStageLink> Links { get; }

    /// <summary>Gets the output ports this shape leaves open, in declaration order.</summary>
    /// <value>
    /// One for a source, two for a fork, and none for a shape that is ready to be closed into a graph.
    /// </value>
    internal IReadOnlyList<LocalOpenOutput> OpenOutputs { get; }

    /// <summary>Gets the results this shape has already been asked to expose, in the author's order.</summary>
    /// <value>
    /// One request per result-bearing branch a junction call has consumed so far. Empty for every chain,
    /// because a chain's one result is named by the very call that closes it and never has to be carried.
    /// </value>
    /// <remarks>
    /// A tap is why these are carried rather than passed to the closing call: <c>AlsoTo</c> consumes a branch
    /// that may declare a result and returns a source, so the name and the producing occurrence are known
    /// long before anything closes the graph. Carrying them on the shape keeps the closing call ignorant of
    /// how many results the graph already has.
    /// </remarks>
    internal IReadOnlyList<LocalSlotRequest> Slots { get; }

    /// <summary>Gets the request list of a shape that has been asked for no result yet.</summary>
    private static IReadOnlyList<LocalSlotRequest> NoSlots { get; } = Array.AsReadOnly<LocalSlotRequest>([]);

    /// <summary>Records one more result this shape's closure is to expose.</summary>
    /// <param name="request">The name, the producing occurrence, and the branch binding to fill.</param>
    /// <returns>A new shape; this one is unchanged.</returns>
    /// <remarks>
    /// Asked for by a junction call that consumed a result-bearing branch and did not close the graph — a
    /// tap. The request travels with the shape until something closes it, which is where every slot a
    /// document declares is finally written.
    /// </remarks>
    internal LocalGraphShape Declaring(LocalSlotRequest request) =>
        new(Stages, Links, OpenOutputs, Array.AsReadOnly<LocalSlotRequest>([.. Slots, request]));

    /// <summary>Creates the shape of a linear chain of occurrences.</summary>
    /// <param name="stages">The occurrences in authoring order; at least one.</param>
    /// <returns>The shape, with the last occurrence's output port open when it has one.</returns>
    /// <remarks>
    /// The bridge from the chain-shaped values — a sink, a flow, and the occurrence lists the testing
    /// package builds — into the shape a source carries. A chain is a shape with nothing branching in it,
    /// which is why this is a conversion rather than a second representation.
    /// </remarks>
    internal static LocalGraphShape OfChain(IReadOnlyList<StageOccurrence> stages)
    {
        LocalStageLink[] links = new LocalStageLink[stages.Count - 1];

        for (int index = 1; index < stages.Count; index++)
        {
            links[index - 1] = new LocalStageLink(
                index - 1,
                Producing(stages[index - 1], index - 1),
                index,
                Consuming(stages[index], index));
        }

        return new LocalGraphShape(
            stages,
            Array.AsReadOnly<LocalStageLink>(links),
            OpenEndOf(stages, stages.Count - 1),
            NoSlots);
    }

    /// <summary>Extends this shape with one occurrence at its only open output.</summary>
    /// <param name="stage">The occurrence to attach.</param>
    /// <returns>A new shape; this one is unchanged.</returns>
    internal LocalGraphShape Append(StageOccurrence stage) => Concat(LocalStageChain.Of(stage));

    /// <summary>Names the occurrence this shape's only open output belongs to.</summary>
    /// <param name="name">The validated name.</param>
    /// <returns>A new shape; this one is unchanged.</returns>
    /// <exception cref="InvalidOperationException">
    /// The shape does not have exactly one open output, or the occurrence at that output is already named.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The occurrence at the open output rather than the last one added, and the difference is what makes
    /// this rule work on a shape that is not a chain. After an operator the two are the same occurrence.
    /// After a fan-in — a merge, a concat, an interleave, a zip, a fork's rejoin — the open output belongs to
    /// the junction, which is the occurrence that call contributed. After a tap the last occurrence added is
    /// the branch's terminal, which the branch already named where it was written, and the open output
    /// belongs to the tapping junction — which is the one occurrence such a call contributes that has no
    /// other spelling. So "the occurrence this value ends at" names exactly the stage the next operator would
    /// attach to, in every shape a source can hold.
    /// </para>
    /// <para>
    /// A shape with two open ends is a fork and has no answer here, which is why <see cref="Single"/> is what
    /// asks. No authoring value reaches this method in that state: a fork is a type of its own and carries no
    /// naming call, so the refusal is a defect check rather than a diagnostic an author can provoke.
    /// </para>
    /// </remarks>
    internal LocalGraphShape Naming(NodeId name)
    {
        LocalOpenOutput only = Single();
        StageOccurrence[] named = [.. Stages];

        named[only.Stage] = LocalOccurrenceName.Rename(named[only.Stage], name);

        return new LocalGraphShape(Array.AsReadOnly(named), Links, OpenOutputs, Slots);
    }

    /// <summary>Extends this shape with a linear chain of occurrences at its only open output.</summary>
    /// <param name="stages">The occurrences to attach, in authoring order; possibly none.</param>
    /// <returns>A new shape; this one is unchanged.</returns>
    /// <remarks>
    /// An empty chain attaches nothing and changes nothing, which is how the identity flow
    /// <see cref="Orleans.Dataflow.Flow.For{T}"/> composes: it contributes no occurrence, so a source that
    /// goes through it is the source it was.
    /// </remarks>
    internal LocalGraphShape Concat(IReadOnlyList<StageOccurrence> stages)
    {
        LocalOpenOutput only = Single();

        List<StageOccurrence> grown = [.. Stages];
        List<LocalStageLink> links = [.. Links];
        LocalOpenOutput? cursor = Attach(grown, links, only, stages);

        return new LocalGraphShape(
            Array.AsReadOnly<StageOccurrence>([.. grown]),
            Array.AsReadOnly<LocalStageLink>([.. links]),
            cursor is { } open ? Array.AsReadOnly<LocalOpenOutput>([open]) : Array.AsReadOnly<LocalOpenOutput>([]),
            Slots);
    }

    /// <summary>Splits this shape at its only open output into one junction and its legs.</summary>
    /// <param name="junction">The splitting junction occurrence.</param>
    /// <param name="legs">The junction's output ports, one per branch, in argument order.</param>
    /// <param name="branches">
    /// The occurrences of each branch in argument order; a branch that ends in a terminal closes its leg,
    /// and an empty branch leaves the junction's own leg port open.
    /// </param>
    /// <returns>A new shape; this one is unchanged.</returns>
    /// <remarks>
    /// The legs are attached in argument order and the occurrences are appended in that same order, which is
    /// what makes branch order identity-bearing: the first branch's occurrences are numbered before the
    /// second's, so swapping two arguments produces a different document (ADR 0006).
    /// </remarks>
    internal LocalGraphShape Split(
        StageOccurrence junction,
        IReadOnlyList<PortId> legs,
        IReadOnlyList<IReadOnlyList<StageOccurrence>> branches)
    {
        LocalOpenOutput only = Single();

        List<StageOccurrence> grown = [.. Stages];
        List<LocalStageLink> links = [.. Links];
        int junctionIndex = grown.Count;

        grown.Add(junction);
        links.Add(new LocalStageLink(only.Stage, only.Port, junctionIndex, Consuming(junction, junctionIndex)));

        List<LocalOpenOutput> open = [];

        for (int leg = 0; leg < branches.Count; leg++)
        {
            LocalOpenOutput? cursor = Attach(
                grown,
                links,
                new LocalOpenOutput(junctionIndex, legs[leg]),
                branches[leg]);

            if (cursor is { } tail)
            {
                open.Add(tail);
            }
        }

        return new LocalGraphShape(
            Array.AsReadOnly<StageOccurrence>([.. grown]),
            Array.AsReadOnly<LocalStageLink>([.. links]),
            Array.AsReadOnly<LocalOpenOutput>([.. open]),
            Slots);
    }

    /// <summary>Places another shape beside this one, sharing nothing.</summary>
    /// <param name="other">The shape to place beside this one, which is not modified.</param>
    /// <returns>A new shape whose open outputs are this one's followed by the other's.</returns>
    /// <remarks>
    /// The two halves of a fan-in before the junction that joins them. Nothing is connected here: a union of
    /// two shapes is two disconnected streams, and it is <see cref="Combine"/> that makes it a graph. The
    /// other shape's positions are rebased by this shape's length, which is what keeps the numbering the
    /// argument order of the call.
    /// </remarks>
    internal LocalGraphShape Union(LocalGraphShape other)
    {
        int offset = Stages.Count;

        List<StageOccurrence> stages = [.. Stages, .. other.Stages];
        List<LocalStageLink> links = [.. Links];
        List<LocalOpenOutput> open = [.. OpenOutputs];
        List<LocalSlotRequest> slots = [.. Slots];

        for (int index = 0; index < other.Slots.Count; index++)
        {
            LocalSlotRequest request = other.Slots[index];

            slots.Add(request with { Stage = request.Stage + offset });
        }

        for (int index = 0; index < other.Links.Count; index++)
        {
            LocalStageLink link = other.Links[index];

            links.Add(new LocalStageLink(link.From + offset, link.FromPort, link.To + offset, link.ToPort));
        }

        for (int index = 0; index < other.OpenOutputs.Count; index++)
        {
            LocalOpenOutput output = other.OpenOutputs[index];

            open.Add(new LocalOpenOutput(output.Stage + offset, output.Port));
        }

        return new LocalGraphShape(
            Array.AsReadOnly<StageOccurrence>([.. stages]),
            Array.AsReadOnly<LocalStageLink>([.. links]),
            Array.AsReadOnly<LocalOpenOutput>([.. open]),
            Array.AsReadOnly<LocalSlotRequest>([.. slots]));
    }

    /// <summary>Joins every open output of this shape into one junction.</summary>
    /// <param name="junction">The joining junction occurrence.</param>
    /// <param name="inputs">The junction's input ports, one per open output, in the same order.</param>
    /// <returns>A new shape with the junction's single output open; this one is unchanged.</returns>
    /// <remarks>
    /// Every open output is consumed, so a fan-in is written where the streams to join are the whole of what
    /// is open — which is what a union of two sources is, and what a fork is. The order is positional: the
    /// first open output reaches <c>in-0</c>, and that is the order a concat consumes in, an interleave
    /// rotates in, and a zip builds its rows in.
    /// </remarks>
    internal LocalGraphShape Combine(StageOccurrence junction, IReadOnlyList<PortId> inputs)
    {
        List<StageOccurrence> stages = [.. Stages];
        List<LocalStageLink> links = [.. Links];
        int junctionIndex = stages.Count;

        stages.Add(junction);

        for (int index = 0; index < OpenOutputs.Count; index++)
        {
            LocalOpenOutput open = OpenOutputs[index];

            links.Add(new LocalStageLink(open.Stage, open.Port, junctionIndex, inputs[index]));
        }

        return new LocalGraphShape(
            Array.AsReadOnly<StageOccurrence>([.. stages]),
            Array.AsReadOnly<LocalStageLink>([.. links]),
            OpenEndOf(stages, junctionIndex),
            Slots);
    }

    /// <summary>Attaches a chain of occurrences after one open output.</summary>
    /// <param name="stages">The growing occurrence list, which this method appends to.</param>
    /// <param name="links">The growing link list, which this method appends to.</param>
    /// <param name="from">The open output the chain attaches to.</param>
    /// <param name="chain">The occurrences to attach, in authoring order.</param>
    /// <returns>The open output left at the end of the chain, or <see langword="null"/> when it terminates.</returns>
    /// <exception cref="InvalidOperationException">
    /// An occurrence stands after one that produces nothing, which no authoring value can express and is a
    /// defect in this assembly rather than a mistake an author could make.
    /// </exception>
    private static LocalOpenOutput? Attach(
        List<StageOccurrence> stages,
        List<LocalStageLink> links,
        LocalOpenOutput from,
        IReadOnlyList<StageOccurrence> chain)
    {
        LocalOpenOutput? cursor = from;

        for (int index = 0; index < chain.Count; index++)
        {
            StageOccurrence stage = chain[index];
            int position = stages.Count;

            if (cursor is not { } open)
            {
                throw new InvalidOperationException(
                    string.Create(
                        CultureInfo.InvariantCulture,
                        $"An occurrence of '{stage.Stage}' stands after one that produces no elements, so there is nothing to connect it to. A chain ends at its terminal, and this one does not."));
            }

            stages.Add(stage);
            links.Add(new LocalStageLink(open.Stage, open.Port, position, Consuming(stage, position)));

            cursor = stage.OutputPort is { } produced ? new LocalOpenOutput(position, produced) : null;
        }

        return cursor;
    }

    /// <summary>Reads the output port one occurrence produces through.</summary>
    /// <param name="stage">The occurrence.</param>
    /// <param name="position">Its position, for the diagnostic.</param>
    /// <returns>The port.</returns>
    /// <exception cref="InvalidOperationException">The occurrence produces no single stream.</exception>
    private static PortId Producing(StageOccurrence stage, int position) =>
        stage.OutputPort ??
        throw new InvalidOperationException(
            string.Create(
                CultureInfo.InvariantCulture,
                $"The occurrence of '{stage.Stage}' at position {position + 1} produces no single stream, so a chain cannot continue through it. A junction is composed through the shape's own junction operations, never as a link in a chain."));

    /// <summary>Reads the input port one occurrence consumes through.</summary>
    /// <param name="stage">The occurrence.</param>
    /// <param name="position">Its position, for the diagnostic.</param>
    /// <returns>The port.</returns>
    /// <exception cref="InvalidOperationException">The occurrence consumes no single stream.</exception>
    private static PortId Consuming(StageOccurrence stage, int position) =>
        stage.InputPort ??
        throw new InvalidOperationException(
            string.Create(
                CultureInfo.InvariantCulture,
                $"The occurrence of '{stage.Stage}' at position {position + 1} consumes no single stream, so nothing can be attached to it as a chain. A source consumes nothing, and a fan-in junction is joined through the shape's own junction operations."));

    /// <summary>Builds the open-output list of a shape that ends at one occurrence.</summary>
    /// <param name="stages">The occurrences.</param>
    /// <param name="last">The position of the last one.</param>
    /// <returns>Its output port when it has one, and nothing when it is a terminal.</returns>
    private static ReadOnlyCollection<LocalOpenOutput> OpenEndOf(IReadOnlyList<StageOccurrence> stages, int last) =>
        stages[last].OutputPort is { } port
            ? Array.AsReadOnly<LocalOpenOutput>([new LocalOpenOutput(last, port)])
            : Array.AsReadOnly<LocalOpenOutput>([]);

    /// <summary>Reads the one open output a linear value's shape always has.</summary>
    /// <returns>The open output.</returns>
    /// <exception cref="InvalidOperationException">The shape does not have exactly one open output.</exception>
    private LocalOpenOutput Single() =>
        OpenOutputs.Count == 1
            ? OpenOutputs[0]
            : throw new InvalidOperationException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"A chain continues from exactly one open output, and this shape has {OpenOutputs.Count}. A shape with several is a fork, which is rejoined rather than continued."));
}
