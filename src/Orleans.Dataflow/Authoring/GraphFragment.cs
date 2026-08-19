using System.Globalization;
using System.Text;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// An immutable partial graph: stage occurrences, the wiring between them, and the ports that are
/// deliberately still unconnected.
/// </summary>
/// <remarks>
/// <para>
/// The fragment is the language-neutral value both authoring frontends compile into. A C# fluent chain and
/// an F# pipeline are facades over the same algebra, so every rule that decides whether a partial graph is
/// well formed lives here and not in either frontend.
/// </para>
/// <para>
/// A fragment is valid by construction. <see cref="Create"/> enforces the structural rules of the
/// definition model over <see cref="Nodes"/> and <see cref="Edges"/>, plus the rules that only a partial
/// graph has: an open port names a declared node, an open port is not also an edge endpoint, and the open
/// lists are duplicate-free. All violations are reported at once, not merely the first.
/// </para>
/// <para>
/// A fragment carries no result slots. A <see cref="ResultSlotId"/> is a single segment and cannot be
/// path-rebased the way a <see cref="NodeId"/> can, and a slot binds to an occurrence in a graph
/// rather than to a reusable value; slots are therefore declared only when a fragment is closed, against
/// the producer's (possibly scoped) address.
/// </para>
/// <para>
/// <see cref="Nodes"/> and <see cref="Edges"/> are stored in the canonical order a document fixes, so two
/// fragments built from the same elements in different orders are indistinguishable afterwards.
/// <see cref="OpenInputs"/> and <see cref="OpenOutputs"/> keep the caller's order instead, because they
/// are positional API surface: composition names a boundary port by position or by address, so sorting
/// them would silently renumber the boundary the caller just declared.
/// </para>
/// <para>
/// Equality is structural and element-wise over all four collections, with the open lists compared in
/// their stored order. Two fragments whose boundaries differ only in order are therefore different
/// fragments, which is the same statement as the ordering rule above.
/// </para>
/// </remarks>
public sealed record class GraphFragment
{
    /// <summary>
    /// Initializes a new instance of the <see cref="GraphFragment"/> class.
    /// </summary>
    /// <param name="nodes">The validated, canonically ordered, read-only nodes.</param>
    /// <param name="edges">The validated, canonically ordered, read-only edges.</param>
    /// <param name="openInputs">The validated, caller-ordered, read-only open input addresses.</param>
    /// <param name="openOutputs">The validated, caller-ordered, read-only open output addresses.</param>
    /// <remarks>
    /// The constructor is private and every member is get-only, so a fragment cannot be built or amended
    /// around <see cref="Create"/>: a <c>with</c> expression has no member it is allowed to change.
    /// </remarks>
    private GraphFragment(
        IReadOnlyList<StageNode> nodes,
        IReadOnlyList<GraphEdge> edges,
        IReadOnlyList<PortAddress> openInputs,
        IReadOnlyList<PortAddress> openOutputs)
    {
        Nodes = nodes;
        Edges = edges;
        OpenInputs = openInputs;
        OpenOutputs = openOutputs;
    }

    /// <summary>
    /// Gets the stage occurrences of this fragment.
    /// </summary>
    /// <value>
    /// A nonempty read-only list of nodes in ordinal order of their node identifier text.
    /// </value>
    public IReadOnlyList<StageNode> Nodes { get; }

    /// <summary>
    /// Gets the wiring internal to this fragment.
    /// </summary>
    /// <value>
    /// A read-only list of edges in ordinal order of origin node, origin port, target node, and target
    /// port; empty for a single-node fragment.
    /// </value>
    public IReadOnlyList<GraphEdge> Edges { get; }

    /// <summary>
    /// Gets the input ports this fragment leaves open for a later connection.
    /// </summary>
    /// <value>A read-only list of distinct addresses in the order the caller supplied them.</value>
    /// <remarks>
    /// The order is the caller's, not a canonical one, because the boundary of a fragment is API surface:
    /// a linear composition consumes "the" open input and a junction shape will name one by position.
    /// </remarks>
    public IReadOnlyList<PortAddress> OpenInputs { get; }

    /// <summary>
    /// Gets the output ports this fragment leaves open for a later connection.
    /// </summary>
    /// <value>A read-only list of distinct addresses in the order the caller supplied them.</value>
    /// <remarks>
    /// The order is the caller's, not a canonical one, for the same reason as <see cref="OpenInputs"/>.
    /// </remarks>
    public IReadOnlyList<PortAddress> OpenOutputs { get; }

    /// <summary>
    /// Creates a structurally valid <see cref="GraphFragment"/>.
    /// </summary>
    /// <param name="nodes">The stage occurrences, in any order; at least one.</param>
    /// <param name="edges">The wiring internal to the fragment, in any order.</param>
    /// <param name="openInputs">The unconnected input addresses, in the caller's own order.</param>
    /// <param name="openOutputs">The unconnected output addresses, in the caller's own order.</param>
    /// <returns>
    /// The validated fragment, with the nodes and edges in canonical order and the open lists in the
    /// caller's order.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="nodes"/>, <paramref name="edges"/>, <paramref name="openInputs"/>, or
    /// <paramref name="openOutputs"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// The inputs break at least one structural invariant. The message is a numbered list of every
    /// violation found, so one call reports every problem rather than one problem per call.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The structural invariants are: at least one node; no null node and no default element anywhere;
    /// unique node identifiers; every edge endpoint naming a declared node; at most one
    /// edge originating at any output port; at most one edge terminating at any input port; every open
    /// port naming a declared node; no open port that is also an edge endpoint on the matching side; no
    /// repeated address within an open list; and no address that appears in both open lists.
    /// </para>
    /// <para>
    /// One address in both open lists is rejected rather than accepted as an input-output pair. Port
    /// direction is declared by a stage specification, and a fragment deliberately cannot see the catalog,
    /// so it cannot know that the two entries describe different directions of one port; a caller that
    /// lists an address twice has a bug worth seeing.
    /// </para>
    /// <para>
    /// Each sequence is enumerated exactly once and copied, so a caller may pass a lazy sequence and may
    /// keep mutating its own collection afterwards without affecting the fragment.
    /// </para>
    /// </remarks>
    public static GraphFragment Create(
        IEnumerable<StageNode> nodes,
        IEnumerable<GraphEdge> edges,
        IEnumerable<PortAddress> openInputs,
        IEnumerable<PortAddress> openOutputs)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        ArgumentNullException.ThrowIfNull(edges);
        ArgumentNullException.ThrowIfNull(openInputs);
        ArgumentNullException.ThrowIfNull(openOutputs);

        StageNode[] nodeArray = [.. nodes];
        GraphEdge[] edgeArray = [.. edges];
        PortAddress[] openInputArray = [.. openInputs];
        PortAddress[] openOutputArray = [.. openOutputs];

        List<string> violations = Validate(nodeArray, edgeArray, openInputArray, openOutputArray);

        if (violations.Count > 0)
        {
            throw new ArgumentException(FormatViolations(violations));
        }

        // The same sort keys as GraphDocument, so that closing a fragment is a rewrapping rather than a
        // reordering. Neither site restates the order any more: both read it off the identity types
        // themselves, so there is nothing left that could drift apart. Both keys are unique on validated
        // input: node identifiers are unique by rule, and two edges cannot share all four keys without
        // also breaking both edge multiplicity rules. The order is therefore total and an unstable sort
        // is still deterministic.
        Array.Sort(nodeArray, static (left, right) => left.Id.CompareTo(right.Id));
        Array.Sort(edgeArray);

        return new GraphFragment(
            Array.AsReadOnly(nodeArray),
            Array.AsReadOnly(edgeArray),
            Array.AsReadOnly(openInputArray),
            Array.AsReadOnly(openOutputArray));
    }

    /// <summary>
    /// Creates a single-node fragment whose open ports are the named ports of that node.
    /// </summary>
    /// <param name="node">The only stage occurrence of the fragment.</param>
    /// <param name="openInputs">The input port names to leave open, in the caller's own order.</param>
    /// <param name="openOutputs">The output port names to leave open, in the caller's own order.</param>
    /// <returns>The validated fragment, with no edges.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="node"/>, <paramref name="openInputs"/>, or <paramref name="openOutputs"/> is
    /// <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// A port identifier is the default value, or the resulting fragment breaks a structural invariant,
    /// which for this shape means a repeated port name within one list or the same port name in both.
    /// </exception>
    /// <remarks>
    /// This is the entry point every authoring frontend needs: a source is one node with one open output,
    /// a flow is one node with one of each, and a sink is one node with one open input. The port names are
    /// the caller's claim about the stage's specification; whether the stage really declares them, and in
    /// which direction, is a catalog rule the graph compiler checks after the graph is closed.
    /// </remarks>
    public static GraphFragment OfStage(
        StageNode node,
        IEnumerable<PortId> openInputs,
        IEnumerable<PortId> openOutputs)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(openInputs);
        ArgumentNullException.ThrowIfNull(openOutputs);

        return Create(
            [node],
            [],
            Addresses(node.Id, openInputs, nameof(openInputs)),
            Addresses(node.Id, openOutputs, nameof(openOutputs)));
    }

    /// <summary>
    /// Determines whether this fragment describes the same partial graph as <paramref name="other"/>.
    /// </summary>
    /// <param name="other">The fragment to compare with, which may be <see langword="null"/>.</param>
    /// <returns>
    /// <see langword="true"/> when both fragments have element-wise equal collections; otherwise
    /// <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// The synthesized record equality would compare the collection properties by reference, which would
    /// make two independently built copies of one fragment unequal. Comparison is therefore element-wise.
    /// Nodes and edges are in canonical order, so comparing them position by position is insensitive to
    /// the caller's input order; the open lists are in the caller's order, so two fragments with the same
    /// boundary in a different order are deliberately not equal.
    /// </remarks>
    public bool Equals(GraphFragment? other)
    {
        if (ReferenceEquals(this, other))
        {
            return true;
        }

        return other is not null &&
            SequenceEquals(Nodes, other.Nodes) &&
            SequenceEquals(Edges, other.Edges) &&
            SequenceEquals(OpenInputs, other.OpenInputs) &&
            SequenceEquals(OpenOutputs, other.OpenOutputs);
    }

    /// <summary>
    /// Returns a hash code over every element of every collection.
    /// </summary>
    /// <returns>A hash code consistent with <see cref="Equals(GraphFragment)"/>.</returns>
    /// <remarks>
    /// This is a hash-table hash, not a durable identity: <see cref="HashCode"/> is seeded per process. A
    /// fragment has no durable identity at all, because only a closed document is ever persisted.
    /// </remarks>
    public override int GetHashCode()
    {
        HashCode hash = default;

        AddSequence(ref hash, Nodes);
        AddSequence(ref hash, Edges);
        AddSequence(ref hash, OpenInputs);
        AddSequence(ref hash, OpenOutputs);

        return hash.ToHashCode();
    }

    /// <summary>
    /// Returns a one-line diagnostic summary of this fragment.
    /// </summary>
    /// <returns>Text of the form <c>fragment (2 nodes, 1 edge, 1 open input, 1 open output)</c>.</returns>
    /// <remarks>
    /// The counts are formatted with the invariant culture so that the text is identical under every
    /// ambient culture, and each noun agrees with its own count. The summary is for logs and debugger
    /// display, and it never throws.
    /// </remarks>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"fragment ({Counted(Nodes.Count, "node")}, {Counted(Edges.Count, "edge")}, {Counted(OpenInputs.Count, "open input")}, {Counted(OpenOutputs.Count, "open output")})");

    /// <summary>
    /// Renders one count of one kind of element, with the noun agreeing with the count.
    /// </summary>
    /// <param name="count">The number of elements.</param>
    /// <param name="noun">The singular noun, which is pluralized by a trailing <c>s</c>.</param>
    /// <returns>Text of the form <c>2 nodes</c>, or <c>1 node</c> for exactly one.</returns>
    private static string Counted(int count, string noun) =>
        string.Create(CultureInfo.InvariantCulture, $"{count} {noun}{(count == 1 ? string.Empty : "s")}");

    /// <summary>
    /// Collects every structural invariant the candidate fragment breaks.
    /// </summary>
    /// <param name="nodes">The candidate nodes.</param>
    /// <param name="edges">The candidate edges.</param>
    /// <param name="openInputs">The candidate open input addresses.</param>
    /// <param name="openOutputs">The candidate open output addresses.</param>
    /// <returns>
    /// One lower-case sentence fragment per violation, in a deterministic order, or an empty list when the
    /// candidate is structurally valid.
    /// </returns>
    /// <remarks>
    /// A rule is evaluated only when its own inputs are well formed: a malformed element is reported once
    /// and then left out of the relations it would otherwise participate in, and the reference rules are
    /// skipped entirely while the set of declared nodes is unknown. That keeps the report free of
    /// follow-on violations that would disappear on their own once the reported ones are fixed.
    /// </remarks>
    private static List<string> Validate(
        StageNode[] nodes,
        GraphEdge[] edges,
        PortAddress[] openInputs,
        PortAddress[] openOutputs)
    {
        List<string> violations = [];

        if (nodes.Length == 0)
        {
            violations.Add("the fragment declares no nodes, and a fragment always describes at least one stage occurrence");
        }

        HashSet<NodeId> declaredNodes = [];
        bool declaredNodesAreKnown = true;

        for (int index = 0; index < nodes.Length; index++)
        {
            StageNode node = nodes[index];

            if (node is null)
            {
                violations.Add($"nodes[{index}] is null");
                declaredNodesAreKnown = false;
            }
            else if (!declaredNodes.Add(node.Id))
            {
                violations.Add($"nodes[{index}] repeats the node id '{node.Id}', and node ids are unique within a fragment");
            }
        }

        Dictionary<PortAddress, int> origins = [];
        Dictionary<PortAddress, int> targets = [];

        for (int index = 0; index < edges.Length; index++)
        {
            GraphEdge edge = edges[index];

            if (edge.IsDefault)
            {
                violations.Add($"edges[{index}] is the default {nameof(GraphEdge)}, which connects nothing");
                continue;
            }

            if (origins.TryGetValue(edge.From, out int firstOrigin))
            {
                violations.Add(
                    $"edges[{index}] originates at the output port '{edge.From}', which edges[{firstOrigin}] already originates at, and fan-out is a junction stage rather than edge multiplicity");
            }
            else
            {
                origins.Add(edge.From, index);
            }

            if (targets.TryGetValue(edge.To, out int firstTarget))
            {
                violations.Add(
                    $"edges[{index}] terminates at the input port '{edge.To}', which edges[{firstTarget}] already terminates at, and fan-in is a junction stage rather than edge multiplicity");
            }
            else
            {
                targets.Add(edge.To, index);
            }

            if (!declaredNodesAreKnown)
            {
                continue;
            }

            if (!declaredNodes.Contains(edge.From.Node))
            {
                violations.Add(
                    $"edges[{index}] originates at '{edge.From}', whose node '{edge.From.Node}' is not declared in the fragment");
            }

            if (!declaredNodes.Contains(edge.To.Node))
            {
                violations.Add(
                    $"edges[{index}] terminates at '{edge.To}', whose node '{edge.To.Node}' is not declared in the fragment");
            }
        }

        Dictionary<PortAddress, int> declaredOpenInputs = [];

        for (int index = 0; index < openInputs.Length; index++)
        {
            PortAddress address = openInputs[index];

            if (address.IsDefault)
            {
                violations.Add($"openInputs[{index}] is the default {nameof(PortAddress)}, which names no port");
                continue;
            }

            // A repeated address is reported once and then skipped, because every further rule would
            // report the same address a second time and all of those reports disappear together.
            if (declaredOpenInputs.TryGetValue(address, out int firstInput))
            {
                violations.Add(
                    $"openInputs[{index}] repeats the open input '{address}', which openInputs[{firstInput}] already names, and an open port list names each address at most once");
                continue;
            }

            declaredOpenInputs.Add(address, index);

            if (declaredNodesAreKnown && !declaredNodes.Contains(address.Node))
            {
                violations.Add(
                    $"openInputs[{index}] '{address}' names node '{address.Node}', which is not declared in the fragment");
            }

            if (targets.TryGetValue(address, out int edgeIndex))
            {
                violations.Add(
                    $"openInputs[{index}] '{address}' is where edges[{edgeIndex}] terminates, and an open port is by definition unconnected");
            }
        }

        Dictionary<PortAddress, int> declaredOpenOutputs = [];

        for (int index = 0; index < openOutputs.Length; index++)
        {
            PortAddress address = openOutputs[index];

            if (address.IsDefault)
            {
                violations.Add($"openOutputs[{index}] is the default {nameof(PortAddress)}, which names no port");
                continue;
            }

            if (declaredOpenOutputs.TryGetValue(address, out int firstOutput))
            {
                violations.Add(
                    $"openOutputs[{index}] repeats the open output '{address}', which openOutputs[{firstOutput}] already names, and an open port list names each address at most once");
                continue;
            }

            declaredOpenOutputs.Add(address, index);

            if (declaredNodesAreKnown && !declaredNodes.Contains(address.Node))
            {
                violations.Add(
                    $"openOutputs[{index}] '{address}' names node '{address.Node}', which is not declared in the fragment");
            }

            if (origins.TryGetValue(address, out int edgeIndex))
            {
                violations.Add(
                    $"openOutputs[{index}] '{address}' is where edges[{edgeIndex}] originates, and an open port is by definition unconnected");
            }

            if (declaredOpenInputs.TryGetValue(address, out int inputIndex))
            {
                violations.Add(
                    $"openOutputs[{index}] '{address}' is also openInputs[{inputIndex}], and one address is open on at most one side: port direction is declared by a stage specification, which a fragment cannot see");
            }
        }

        return violations;
    }

    /// <summary>
    /// Renders the collected violations as one numbered list.
    /// </summary>
    /// <param name="violations">The violations, in the order <see cref="Validate"/> found them.</param>
    /// <returns>A message whose first line states the count and whose remaining lines are numbered.</returns>
    /// <remarks>
    /// The exception carries no parameter name because the invariants are relations between the arguments:
    /// an open port naming a node that no element of <c>nodes</c> declares is not the fault of either
    /// argument alone. The numbered list is the diagnostic, and it names every offending identity.
    /// </remarks>
    private static string FormatViolations(List<string> violations)
    {
        StringBuilder message = new();

        message.Append(CultureInfo.InvariantCulture, $"The graph fragment breaks {violations.Count} structural ");
        message.Append(violations.Count == 1 ? "invariant:" : "invariants:");

        for (int index = 0; index < violations.Count; index++)
        {
            message.Append(Environment.NewLine)
                .Append(CultureInfo.InvariantCulture, $"{index + 1}. {violations[index]}.");
        }

        return message.ToString();
    }

    /// <summary>
    /// Builds the port addresses of one node from a sequence of port identifiers.
    /// </summary>
    /// <param name="node">The node that owns every port.</param>
    /// <param name="ports">The port identifiers, in the caller's order.</param>
    /// <param name="parameterName">The name of the argument the identifiers came from.</param>
    /// <returns>One address per identifier, in the same order.</returns>
    /// <exception cref="ArgumentException">A port identifier is the default value.</exception>
    /// <remarks>
    /// The default identifier is rejected here rather than by <see cref="PortAddress.Create"/> so that the
    /// exception names the argument of the public factory instead of an internal parameter.
    /// </remarks>
    private static List<PortAddress> Addresses(NodeId node, IEnumerable<PortId> ports, string parameterName)
    {
        List<PortAddress> addresses = [];

        foreach (PortId port in ports)
        {
            if (port.IsDefault)
            {
                throw new ArgumentException(
                    $"A {nameof(GraphFragment)} open port requires a created {nameof(PortId)}; the default {nameof(PortId)} names no port.",
                    parameterName);
            }

            addresses.Add(PortAddress.Create(node, port));
        }

        return addresses;
    }

    /// <summary>Determines whether two lists hold equal elements in the same positions.</summary>
    /// <typeparam name="TElement">The element type.</typeparam>
    /// <param name="left">The left list.</param>
    /// <param name="right">The right list.</param>
    /// <returns><see langword="true"/> when the lists have equal length and equal elements.</returns>
    private static bool SequenceEquals<TElement>(IReadOnlyList<TElement> left, IReadOnlyList<TElement> right)
    {
        if (left.Count != right.Count)
        {
            return false;
        }

        EqualityComparer<TElement> comparer = EqualityComparer<TElement>.Default;

        for (int index = 0; index < left.Count; index++)
        {
            if (!comparer.Equals(left[index], right[index]))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Adds every element of a list to a hash code, in order.</summary>
    /// <typeparam name="TElement">The element type.</typeparam>
    /// <param name="hash">The hash code under construction.</param>
    /// <param name="elements">The elements to add.</param>
    private static void AddSequence<TElement>(ref HashCode hash, IReadOnlyList<TElement> elements)
    {
        hash.Add(elements.Count);

        for (int index = 0; index < elements.Count; index++)
        {
            hash.Add(elements[index]);
        }
    }
}
