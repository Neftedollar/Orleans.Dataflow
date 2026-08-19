using System.Globalization;
using System.Text;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Definition;

/// <summary>
/// The immutable, nongeneric, behavior-free description of one revision of one graph.
/// </summary>
/// <remarks>
/// <para>
/// The graph document is the only durable representation of a graph. It contains identifiers,
/// canonical payloads, and wiring, and nothing that could load code: everything it names resolves through
/// a stage catalog that deployment code registers.
/// </para>
/// <para>
/// A document is canonical by construction. <see cref="Create"/> sorts every collection into the single
/// order the canonical byte form fixes, so two documents built from the same elements in different orders
/// are indistinguishable afterwards, element for element, and serialize to identical bytes. Callers never
/// choose the order and never choose <see cref="FormatVersion"/>.
/// </para>
/// <para>
/// A document is also structurally valid by construction: <see cref="Create"/> enforces every structural
/// invariant of the definition model and reports all violations it finds at once, not merely the first.
/// Structural validity is catalog-free, so a structurally valid document can still be semantically invalid
/// against a catalog; those rules belong to the graph compiler.
/// </para>
/// <para>
/// Equality is structural: two documents are equal when their format version, identity, revision, and
/// canonically ordered collections are element-wise equal. Reference equality of the collections is
/// deliberately not the rule, because documents built independently from permuted inputs describe the same
/// graph and must compare equal.
/// </para>
/// </remarks>
public sealed record class GraphDocument
{
    /// <summary>
    /// The document format version this library writes.
    /// </summary>
    /// <remarks>
    /// Every document built through <see cref="Create"/> carries this version. Reading a document written
    /// under a different version is the reader's problem, and an unknown version fails before any other
    /// rule runs rather than being parsed on a best-effort basis.
    /// </remarks>
    public const int CurrentFormatVersion = 1;

    /// <summary>
    /// Initializes a new instance of the <see cref="GraphDocument"/> class.
    /// </summary>
    /// <param name="id">The validated graph identity.</param>
    /// <param name="revision">The validated revision.</param>
    /// <param name="capabilities">The validated, canonically ordered, read-only capability tokens.</param>
    /// <param name="nodes">The validated, canonically ordered, read-only nodes.</param>
    /// <param name="edges">The validated, canonically ordered, read-only edges.</param>
    /// <param name="resultSlots">The validated, canonically ordered, read-only result slots.</param>
    /// <remarks>
    /// The constructor is private and every member is get-only, so a document cannot be built or amended
    /// around <see cref="Create"/>: a <c>with</c> expression has no member it is allowed to change.
    /// </remarks>
    private GraphDocument(
        GraphId id,
        GraphRevision revision,
        IReadOnlyList<CapabilityToken> capabilities,
        IReadOnlyList<StageNode> nodes,
        IReadOnlyList<GraphEdge> edges,
        IReadOnlyList<ResultSlotDefinition> resultSlots)
    {
        FormatVersion = CurrentFormatVersion;
        Id = id;
        Revision = revision;
        Capabilities = capabilities;
        Nodes = nodes;
        Edges = edges;
        ResultSlots = resultSlots;
    }

    /// <summary>
    /// Gets the format version of this document.
    /// </summary>
    /// <value>Always <see cref="CurrentFormatVersion"/> for a document built by this library.</value>
    public int FormatVersion { get; }

    /// <summary>
    /// Gets the identity of the graph lineage this document belongs to.
    /// </summary>
    /// <value>A created <see cref="GraphId"/>.</value>
    public GraphId Id { get; }

    /// <summary>
    /// Gets the revision this document is.
    /// </summary>
    /// <value>A created <see cref="GraphRevision"/>.</value>
    public GraphRevision Revision { get; }

    /// <summary>
    /// Gets the capability tokens this document declares.
    /// </summary>
    /// <value>
    /// A read-only list of distinct tokens in ordinal order of their text; empty when the document
    /// declares nothing.
    /// </value>
    public IReadOnlyList<CapabilityToken> Capabilities { get; }

    /// <summary>
    /// Gets the stage occurrences of this graph.
    /// </summary>
    /// <value>A read-only list of nodes in ordinal order of their node identifier text.</value>
    public IReadOnlyList<StageNode> Nodes { get; }

    /// <summary>
    /// Gets the wiring between the nodes of this graph.
    /// </summary>
    /// <value>
    /// A read-only list of edges in ordinal order of origin node, origin port, target node, and target
    /// port.
    /// </value>
    public IReadOnlyList<GraphEdge> Edges { get; }

    /// <summary>
    /// Gets the result slots this graph exposes.
    /// </summary>
    /// <value>A read-only list of slot definitions in ordinal order of their slot identifier text.</value>
    public IReadOnlyList<ResultSlotDefinition> ResultSlots { get; }

    /// <summary>
    /// Creates a canonical, structurally valid <see cref="GraphDocument"/>.
    /// </summary>
    /// <param name="id">The graph identity; must not be the default value.</param>
    /// <param name="revision">The revision; must not be the default value.</param>
    /// <param name="capabilities">The declared capability tokens, in any order, without duplicates.</param>
    /// <param name="nodes">The stage occurrences, in any order.</param>
    /// <param name="edges">The wiring, in any order.</param>
    /// <param name="resultSlots">The exposed result slots, in any order.</param>
    /// <returns>The validated document, with every collection in canonical order.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="capabilities"/>, <paramref name="nodes"/>, <paramref name="edges"/>, or
    /// <paramref name="resultSlots"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// The inputs break at least one structural invariant. The message is a numbered list of every
    /// violation found, so one call reports every problem rather than one problem per call.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The structural invariants are: no null element and no default struct in any collection; unique node
    /// identifiers; unique result slot identifiers; every edge endpoint and every result slot producer
    /// naming a declared node; at most one edge originating at any output port; at most one edge
    /// terminating at any input port; distinct capability tokens; and a created identity and revision. Two
    /// result slots may share one producer, which is two names for one produced value, so producers are
    /// deliberately not required to be distinct.
    /// </para>
    /// <para>
    /// A cycle is deliberately not among them, and a self-loop is a cycle of one node rather than a shape
    /// of its own. Whether a loop can run is a statement about the boundaries it passes — it is legal
    /// exactly when it passes one that can hold an element and answer without waiting for its own
    /// downstream — so it belongs to the runtime that has to execute it rather than to the shape of a
    /// document, and a self-loop is tested by that rule like any other loop.
    /// </para>
    /// <para>
    /// Duplicate capability tokens are rejected rather than folded away. A caller that declares one token
    /// twice has a bug worth seeing, and silently accepting the input would make the document's own history
    /// unreadable.
    /// </para>
    /// <para>
    /// Each sequence is enumerated exactly once and copied, so a caller may pass a lazy sequence and may
    /// keep mutating its own collection afterwards without affecting the document.
    /// </para>
    /// </remarks>
    public static GraphDocument Create(
        GraphId id,
        GraphRevision revision,
        IEnumerable<CapabilityToken> capabilities,
        IEnumerable<StageNode> nodes,
        IEnumerable<GraphEdge> edges,
        IEnumerable<ResultSlotDefinition> resultSlots)
    {
        ArgumentNullException.ThrowIfNull(capabilities);
        ArgumentNullException.ThrowIfNull(nodes);
        ArgumentNullException.ThrowIfNull(edges);
        ArgumentNullException.ThrowIfNull(resultSlots);

        CapabilityToken[] capabilityArray = [.. capabilities];
        StageNode[] nodeArray = [.. nodes];
        GraphEdge[] edgeArray = [.. edges];
        ResultSlotDefinition[] slotArray = [.. resultSlots];

        List<string> violations = Validate(id, revision, capabilityArray, nodeArray, edgeArray, slotArray);

        if (violations.Count > 0)
        {
            throw new ArgumentException(FormatViolations(violations));
        }

        // Canonical order is the identity types' own order, never a comparator restated here: tokens,
        // node identifiers, and slot identifiers sort ordinally over their text, and an edge sorts by
        // origin node, origin port, target node, and target port. Every sort key is unique on validated
        // input: node identifiers, slot identifiers, and capability tokens are unique by rule, and two
        // edges cannot share both endpoints without also sharing an output port and an input port. The
        // order is therefore total, and an unstable sort still yields one deterministic result for every
        // permutation of the same elements.
        Array.Sort(capabilityArray);
        Array.Sort(nodeArray, static (left, right) => left.Id.CompareTo(right.Id));
        Array.Sort(edgeArray);
        Array.Sort(slotArray, static (left, right) => left.Id.CompareTo(right.Id));

        return new GraphDocument(
            id,
            revision,
            Array.AsReadOnly(capabilityArray),
            Array.AsReadOnly(nodeArray),
            Array.AsReadOnly(edgeArray),
            Array.AsReadOnly(slotArray));
    }

    /// <summary>
    /// Determines whether this document describes the same graph revision as <paramref name="other"/>.
    /// </summary>
    /// <param name="other">The document to compare with, which may be <see langword="null"/>.</param>
    /// <returns>
    /// <see langword="true"/> when both documents have the same format version, identity, revision, and
    /// element-wise equal collections; otherwise <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// The synthesized record equality would compare the collection properties by reference, which would
    /// make two independently built copies of one graph unequal. Comparison is therefore element-wise over
    /// the collections. Because construction already put them in canonical order, element-wise comparison
    /// is order-insensitive with respect to the caller's input while staying a cheap linear scan.
    /// </remarks>
    public bool Equals(GraphDocument? other)
    {
        if (ReferenceEquals(this, other))
        {
            return true;
        }

        return other is not null &&
            FormatVersion == other.FormatVersion &&
            Id == other.Id &&
            Revision == other.Revision &&
            SequenceEquals(Capabilities, other.Capabilities) &&
            SequenceEquals(Nodes, other.Nodes) &&
            SequenceEquals(Edges, other.Edges) &&
            SequenceEquals(ResultSlots, other.ResultSlots);
    }

    /// <summary>
    /// Returns a hash code over the format version, identity, revision, and every collection element.
    /// </summary>
    /// <returns>A hash code consistent with <see cref="Equals(GraphDocument)"/>.</returns>
    /// <remarks>
    /// This is a hash-table hash, not a durable identity: <see cref="HashCode"/> is seeded per process, so
    /// the same document hashes differently in a different process. The durable identity of a document is
    /// the SHA-256 of its canonical bytes, never this number.
    /// </remarks>
    public override int GetHashCode()
    {
        HashCode hash = default;

        hash.Add(FormatVersion);
        hash.Add(Id);
        hash.Add(Revision);
        AddSequence(ref hash, Capabilities);
        AddSequence(ref hash, Nodes);
        AddSequence(ref hash, Edges);
        AddSequence(ref hash, ResultSlots);

        return hash.ToHashCode();
    }

    /// <summary>
    /// Returns a one-line diagnostic summary of this document.
    /// </summary>
    /// <returns>Text of the form <c>graph-id@r3 (2 nodes, 1 edge, 1 slot)</c>.</returns>
    /// <remarks>
    /// The counts are formatted with the invariant culture so that the text is identical under every
    /// ambient culture, and each noun agrees with its own count. The summary is for logs and debugger
    /// display, not for serialization, and it never throws.
    /// </remarks>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"{Id}@r{Revision} ({Counted(Nodes.Count, "node")}, {Counted(Edges.Count, "edge")}, {Counted(ResultSlots.Count, "slot")})");

    /// <summary>
    /// Renders one count of one kind of element, with the noun agreeing with the count.
    /// </summary>
    /// <param name="count">The number of elements.</param>
    /// <param name="noun">The singular noun, which is pluralized by a trailing <c>s</c>.</param>
    /// <returns>Text of the form <c>2 nodes</c>, or <c>1 node</c> for exactly one.</returns>
    private static string Counted(int count, string noun) =>
        string.Create(CultureInfo.InvariantCulture, $"{count} {noun}{(count == 1 ? string.Empty : "s")}");

    /// <summary>
    /// Collects every structural invariant the candidate document breaks.
    /// </summary>
    /// <param name="id">The candidate graph identity.</param>
    /// <param name="revision">The candidate revision.</param>
    /// <param name="capabilities">The candidate capability tokens.</param>
    /// <param name="nodes">The candidate nodes.</param>
    /// <param name="edges">The candidate edges.</param>
    /// <param name="resultSlots">The candidate result slots.</param>
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
        GraphId id,
        GraphRevision revision,
        CapabilityToken[] capabilities,
        StageNode[] nodes,
        GraphEdge[] edges,
        ResultSlotDefinition[] resultSlots)
    {
        List<string> violations = [];

        if (id.IsDefault)
        {
            violations.Add($"the graph id is the default {nameof(GraphId)}, which names no graph");
        }

        if (revision.IsDefault)
        {
            violations.Add($"the revision is the default {nameof(GraphRevision)}, which names no revision");
        }

        HashSet<CapabilityToken> declaredCapabilities = [];

        for (int index = 0; index < capabilities.Length; index++)
        {
            CapabilityToken token = capabilities[index];

            if (token.IsDefault)
            {
                violations.Add($"capabilities[{index}] is the default {nameof(CapabilityToken)}, which names no capability");
            }
            else if (!declaredCapabilities.Add(token))
            {
                violations.Add($"capabilities[{index}] repeats the capability token '{token}', and a document declares each token at most once");
            }
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
                violations.Add($"nodes[{index}] repeats the node id '{node.Id}', and node ids are unique within a document");
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
                    $"edges[{index}] originates at '{edge.From}', whose node '{edge.From.Node}' is not declared in the document");
            }

            if (!declaredNodes.Contains(edge.To.Node))
            {
                violations.Add(
                    $"edges[{index}] terminates at '{edge.To}', whose node '{edge.To.Node}' is not declared in the document");
            }
        }

        HashSet<ResultSlotId> declaredSlots = [];

        for (int index = 0; index < resultSlots.Length; index++)
        {
            ResultSlotDefinition slot = resultSlots[index];

            if (slot is null)
            {
                violations.Add($"resultSlots[{index}] is null");
                continue;
            }

            if (!declaredSlots.Add(slot.Id))
            {
                violations.Add(
                    $"resultSlots[{index}] repeats the result slot id '{slot.Id}', and result slot ids are unique within a document");
            }

            if (declaredNodesAreKnown && !declaredNodes.Contains(slot.Producer.Node))
            {
                violations.Add(
                    $"resultSlots[{index}] '{slot.Id}' is produced by '{slot.Producer}', whose node '{slot.Producer.Node}' is not declared in the document");
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
    /// an edge naming a node that no element of <c>nodes</c> declares is not the fault of either argument
    /// alone. The numbered list is the diagnostic, and it names every offending identity.
    /// </remarks>
    private static string FormatViolations(List<string> violations)
    {
        StringBuilder message = new();

        message.Append(CultureInfo.InvariantCulture, $"The graph document breaks {violations.Count} structural ");
        message.Append(violations.Count == 1 ? "invariant:" : "invariants:");

        for (int index = 0; index < violations.Count; index++)
        {
            message.Append(Environment.NewLine)
                .Append(CultureInfo.InvariantCulture, $"{index + 1}. {violations[index]}.");
        }

        return message.ToString();
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
