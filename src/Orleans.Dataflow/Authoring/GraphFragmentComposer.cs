using System.Globalization;
using System.Text;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The composition operators of the fragment algebra: import a fragment under a scope, connect two
/// fragments, and close a fragment into a graph document.
/// </summary>
/// <remarks>
/// <para>
/// The operators live beside <see cref="GraphFragment"/> rather than on it, for the same reason
/// <see cref="Orleans.Dataflow.Compilation.GraphCompiler"/> lives beside <see cref="GraphDocument"/>: the
/// fragment is a value with construction and inspection on it, and every operator here is a relation
/// between two fragments or a translation into another type, neither of which reads as a property of one
/// receiver. Keeping them separate also gives the F# frontend a module-shaped surface to bind to, so
/// <c>import</c> and <c>connect</c> can be ordinary functions rather than method calls on a value.
/// </para>
/// <para>
/// Every operator is pure and deterministic: it reads its arguments, allocates a new fragment, and
/// mutates nothing. Equal inputs always produce equal outputs, and nothing here executes any stage.
/// </para>
/// <para>
/// Every operator rebuilds its result through <see cref="GraphFragment.Create"/> or
/// <see cref="GraphDocument.Create"/>, so a result is validated even when the operator's own reasoning
/// says it must already be valid.
/// </para>
/// </remarks>
public static class GraphFragmentComposer
{
    /// <summary>
    /// Rebases every node identifier of a fragment below an import scope.
    /// </summary>
    /// <param name="fragment">The fragment to import.</param>
    /// <param name="scopeSegment">
    /// The scope segment to prefix, which must be a valid identifier segment.
    /// </param>
    /// <returns>
    /// A new fragment with the same shape, whose nodes, edge endpoints, and open ports all name scoped
    /// node identifiers.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="fragment"/> or <paramref name="scopeSegment"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="scopeSegment"/> is not a valid identifier segment, or prefixing it would push a
    /// node identifier past <see cref="NodeId.MaxDepth"/> or <see cref="NodeId.MaxPathLength"/>. The
    /// message is the one <see cref="NodeId.InScope(string)"/> produces, because the grammar of a scope
    /// segment is that method's rule and restating it here would let the two drift apart.
    /// </exception>
    /// <remarks>
    /// <para>
    /// All four identifier surfaces are rebased together: the nodes, both endpoints of every edge, and
    /// both open port lists. Rebasing is pure prefixing, so importing one fragment under two different
    /// scopes yields two disjoint node sets, importing under nested scopes composes by nesting prefixes,
    /// and two identical calls produce equal fragments.
    /// </para>
    /// <para>
    /// A fragment always declares at least one node, so an invalid scope segment always throws; there is
    /// no shape for which the argument would go unchecked.
    /// </para>
    /// <para>
    /// The open lists keep their positions, so a caller that knows a fragment's boundary by position still
    /// knows it after the import.
    /// </para>
    /// </remarks>
    public static GraphFragment Import(GraphFragment fragment, string scopeSegment)
    {
        ArgumentNullException.ThrowIfNull(fragment);

        Dictionary<NodeId, NodeId> rebased = new(fragment.Nodes.Count);
        StageNode[] nodes = new StageNode[fragment.Nodes.Count];

        for (int index = 0; index < fragment.Nodes.Count; index++)
        {
            StageNode node = fragment.Nodes[index];
            NodeId scopedId = node.Id.InScope(scopeSegment);

            rebased.Add(node.Id, scopedId);
            nodes[index] = Rebase(node, scopedId);
        }

        GraphEdge[] edges = new GraphEdge[fragment.Edges.Count];

        for (int index = 0; index < fragment.Edges.Count; index++)
        {
            GraphEdge edge = fragment.Edges[index];

            edges[index] = GraphEdge.Create(Rebase(edge.From, rebased), Rebase(edge.To, rebased));
        }

        return GraphFragment.Create(
            nodes,
            edges,
            Rebase(fragment.OpenInputs, rebased),
            Rebase(fragment.OpenOutputs, rebased));
    }

    /// <summary>
    /// Joins one fragment's open output to another fragment's open input with a new edge.
    /// </summary>
    /// <param name="upstream">The fragment that produces.</param>
    /// <param name="upstreamOutput">
    /// The address to consume, which must be an element of <paramref name="upstream"/>'s open outputs.
    /// </param>
    /// <param name="downstream">The fragment that consumes.</param>
    /// <param name="downstreamInput">
    /// The address to consume, which must be an element of <paramref name="downstream"/>'s open inputs.
    /// </param>
    /// <returns>The merged fragment, with the two consumed ports no longer open.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="upstream"/> or <paramref name="downstream"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="upstreamOutput"/> is not an open output of <paramref name="upstream"/>,
    /// <paramref name="downstreamInput"/> is not an open input of <paramref name="downstream"/>, or the
    /// two fragments share at least one node identifier. The collision message lists every shared
    /// identifier at once.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The two fragments must have disjoint node identifiers, which is what <see cref="Import"/> is for.
    /// Connecting a fragment to itself, or to an unscoped copy of itself, is not a special case: it is the
    /// collision case, and it is reported as one.
    /// </para>
    /// <para>
    /// The result's boundary order is fixed and deterministic: the open inputs are
    /// <paramref name="upstream"/>'s in their order, then <paramref name="downstream"/>'s in their order
    /// with the consumed one removed; the open outputs are <paramref name="upstream"/>'s with the consumed
    /// one removed, then <paramref name="downstream"/>'s. Upstream first on both sides, so a chain of
    /// connections keeps the boundary in the order the chain was written.
    /// </para>
    /// </remarks>
    public static GraphFragment Connect(
        GraphFragment upstream,
        PortAddress upstreamOutput,
        GraphFragment downstream,
        PortAddress downstreamInput)
    {
        ArgumentNullException.ThrowIfNull(upstream);
        ArgumentNullException.ThrowIfNull(downstream);

        if (!upstream.OpenOutputs.Contains(upstreamOutput))
        {
            throw new ArgumentException(
                DescribeUnopenPort(upstreamOutput, "open output", "upstream", upstream.OpenOutputs),
                nameof(upstreamOutput));
        }

        if (!downstream.OpenInputs.Contains(downstreamInput))
        {
            throw new ArgumentException(
                DescribeUnopenPort(downstreamInput, "open input", "downstream", downstream.OpenInputs),
                nameof(downstreamInput));
        }

        EnsureDisjointNodes(upstream, downstream);

        // Both endpoints are declared and the two node sets are disjoint, so this edge cannot be a
        // self-loop and cannot collide with an existing edge: the consumed ports were open, and an open
        // port is not an edge endpoint.
        GraphEdge connection = GraphEdge.Create(upstreamOutput, downstreamInput);

        return GraphFragment.Create(
            [.. upstream.Nodes, .. downstream.Nodes],
            [.. upstream.Edges, .. downstream.Edges, connection],
            [.. upstream.OpenInputs, .. Without(downstream.OpenInputs, downstreamInput)],
            [.. Without(upstream.OpenOutputs, upstreamOutput), .. downstream.OpenOutputs]);
    }

    /// <summary>
    /// Joins two linear fragments: the upstream fragment's only open output to the downstream fragment's
    /// only open input.
    /// </summary>
    /// <param name="upstream">The fragment that produces, which must have exactly one open output.</param>
    /// <param name="downstream">The fragment that consumes, which must have exactly one open input.</param>
    /// <returns>The merged fragment, with the two consumed ports no longer open.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="upstream"/> or <paramref name="downstream"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="upstream"/> does not have exactly one open output or <paramref name="downstream"/>
    /// does not have exactly one open input, or the two fragments share a node identifier.
    /// </exception>
    /// <remarks>
    /// This is the convenience the linear shapes need, and nothing more: it is
    /// <see cref="Connect"/> with the two addresses read off the fragments instead of named by the caller.
    /// A fragment with a boundary wider than one port per side has to name its ports, because there is no
    /// defensible default for which one to consume.
    /// </remarks>
    public static GraphFragment Append(GraphFragment upstream, GraphFragment downstream)
    {
        ArgumentNullException.ThrowIfNull(upstream);
        ArgumentNullException.ThrowIfNull(downstream);

        if (upstream.OpenOutputs.Count != 1 || downstream.OpenInputs.Count != 1)
        {
            throw new ArgumentException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"{nameof(Append)} joins one open output to one open input, so the upstream fragment must have exactly 1 open output and the downstream fragment exactly 1 open input; the upstream fragment has {upstream.OpenOutputs.Count} open outputs and the downstream fragment has {downstream.OpenInputs.Count} open inputs. Use {nameof(Connect)} to name the two addresses explicitly."));
        }

        return Connect(upstream, upstream.OpenOutputs[0], downstream, downstream.OpenInputs[0]);
    }

    /// <summary>
    /// Joins one open output of a fragment to one open input of the same fragment.
    /// </summary>
    /// <param name="fragment">The fragment to wire.</param>
    /// <param name="output">
    /// The address to consume, which must be an element of <paramref name="fragment"/>'s open outputs.
    /// </param>
    /// <param name="input">
    /// The address to consume, which must be an element of <paramref name="fragment"/>'s open inputs.
    /// </param>
    /// <returns>The same fragment with one more edge, and the two consumed ports no longer open.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="fragment"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="output"/> is not an open output of the fragment, or <paramref name="input"/> is not
    /// an open input of it.
    /// </exception>
    /// <remarks>
    /// <para>
    /// <see cref="Connect"/> merges two fragments and can therefore never join a fragment to itself; that is
    /// exactly what makes it safe, and exactly what makes it unable to express the two shapes where an edge
    /// has both ends inside one partial graph. The first is re-convergence: a stream split by a junction and
    /// rejoined by another is a diamond, and the edge that closes it runs between two nodes the earlier
    /// connections already brought into one fragment. The second is a cycle, whose relieving edge runs
    /// backwards into a node that is already there. Both are legal documents the engine runs, and neither is
    /// reachable by folding <see cref="Connect"/>, so the algebra has this operator rather than an authoring
    /// frontend having a private path around it.
    /// </para>
    /// <para>
    /// Nothing here judges what the new edge means. A fragment does not know which stage is upstream of
    /// which — that is the document's edge set to state and the planner's to read — so wiring an output back
    /// to an input the stream already passed through builds a cycle, deliberately and without comment, and a
    /// self-loop is simply the one-node case of that: ADR 0005 subsumed its old special refusal into the
    /// cycle rule, so the loop is built here and judged where every loop is judged. What is checked is what
    /// a fragment can check: both ports are open and both are declared.
    /// </para>
    /// <para>
    /// The remaining open ports keep their relative order, so a caller that knows the rest of a boundary by
    /// position still knows it after the wiring.
    /// </para>
    /// </remarks>
    public static GraphFragment Wire(GraphFragment fragment, PortAddress output, PortAddress input)
    {
        ArgumentNullException.ThrowIfNull(fragment);

        if (!fragment.OpenOutputs.Contains(output))
        {
            throw new ArgumentException(
                DescribeUnopenPort(output, "open output", "wired", fragment.OpenOutputs),
                nameof(output));
        }

        if (!fragment.OpenInputs.Contains(input))
        {
            throw new ArgumentException(
                DescribeUnopenPort(input, "open input", "wired", fragment.OpenInputs),
                nameof(input));
        }

        return GraphFragment.Create(
            fragment.Nodes,
            [.. fragment.Edges, GraphEdge.Create(output, input)],
            Without(fragment.OpenInputs, input),
            Without(fragment.OpenOutputs, output));
    }

    /// <summary>
    /// Closes a fragment with no open ports into a graph document.
    /// </summary>
    /// <param name="fragment">The fragment to close, which must have no open ports.</param>
    /// <param name="id">The graph identity.</param>
    /// <param name="revision">The revision.</param>
    /// <param name="capabilities">The declared capability tokens, in any order, without duplicates.</param>
    /// <param name="resultSlots">The exposed result slots, in any order.</param>
    /// <returns>The validated document.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="fragment"/> is <see langword="null"/>, or
    /// <see cref="GraphDocument.Create"/> rejects <paramref name="capabilities"/> or
    /// <paramref name="resultSlots"/> as <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="fragment"/> still has open ports, or <see cref="GraphDocument.Create"/> rejects the
    /// document.
    /// </exception>
    /// <remarks>
    /// <para>
    /// Only the fragment's own precondition is checked here. Everything else is
    /// <see cref="GraphDocument.Create"/>'s rule, and its aggregate exception propagates untranslated: a
    /// result slot naming an undeclared producer is a document violation whatever built the document, and
    /// rewording it would give the same defect two different diagnostics depending on the path it took.
    /// </para>
    /// <para>
    /// The fragment's nodes and edges are already in the document's canonical order, so closing reorders
    /// nothing; the document's constructor copies them regardless, so the two values share no state.
    /// </para>
    /// </remarks>
    public static GraphDocument Close(
        GraphFragment fragment,
        GraphId id,
        GraphRevision revision,
        IEnumerable<CapabilityToken> capabilities,
        IEnumerable<ResultSlotDefinition> resultSlots)
    {
        ArgumentNullException.ThrowIfNull(fragment);

        if (fragment.OpenInputs.Count > 0 || fragment.OpenOutputs.Count > 0)
        {
            throw new ArgumentException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"A fragment is closed into a graph only when nothing is left to connect, and this fragment has {fragment.OpenInputs.Count} open inputs and {fragment.OpenOutputs.Count} open outputs. Connect every open port, or declare the stage that terminates the graph."),
                nameof(fragment));
        }

        return GraphDocument.Create(id, revision, capabilities, fragment.Nodes, fragment.Edges, resultSlots);
    }

    /// <summary>
    /// Rebuilds a node under a new identity, preserving everything else it carries.
    /// </summary>
    /// <param name="node">The node to rebase.</param>
    /// <param name="scopedId">The rebased identity.</param>
    /// <returns>An equal node except for its identity.</returns>
    /// <remarks>
    /// The execution policy travels with the node. Its contract and payload are present together or absent
    /// together, so one test decides which factory overload applies, and an import can neither drop a
    /// declared policy nor invent one.
    /// </remarks>
    private static StageNode Rebase(StageNode node, NodeId scopedId) =>
        node.ExecutionPolicyContract is { } policyContract && node.ExecutionPolicy is { } policy
            ? StageNode.Create(scopedId, node.Stage, node.ParameterContract, node.Parameters, policyContract, policy)
            : StageNode.Create(scopedId, node.Stage, node.ParameterContract, node.Parameters);

    /// <summary>
    /// Rebases one port address through a node identifier map.
    /// </summary>
    /// <param name="address">The address to rebase.</param>
    /// <param name="rebased">The map from every declared node identifier to its scoped form.</param>
    /// <returns>The address on the scoped node, with the same port.</returns>
    /// <remarks>
    /// The lookup is total on a validated fragment: every edge endpoint and every open port names a
    /// declared node, and the map holds every declared node.
    /// </remarks>
    private static PortAddress Rebase(PortAddress address, Dictionary<NodeId, NodeId> rebased) =>
        PortAddress.Create(rebased[address.Node], address.Port);

    /// <summary>
    /// Rebases a list of port addresses through a node identifier map, in order.
    /// </summary>
    /// <param name="addresses">The addresses to rebase.</param>
    /// <param name="rebased">The map from every declared node identifier to its scoped form.</param>
    /// <returns>The rebased addresses in the same positions.</returns>
    private static PortAddress[] Rebase(IReadOnlyList<PortAddress> addresses, Dictionary<NodeId, NodeId> rebased)
    {
        PortAddress[] result = new PortAddress[addresses.Count];

        for (int index = 0; index < addresses.Count; index++)
        {
            result[index] = Rebase(addresses[index], rebased);
        }

        return result;
    }

    /// <summary>
    /// Returns the addresses of a list except one.
    /// </summary>
    /// <param name="addresses">The list to filter.</param>
    /// <param name="consumed">The address to drop, which occurs at most once because the list is distinct.</param>
    /// <returns>The remaining addresses, in their original order.</returns>
    private static IEnumerable<PortAddress> Without(IReadOnlyList<PortAddress> addresses, PortAddress consumed)
    {
        for (int index = 0; index < addresses.Count; index++)
        {
            if (addresses[index] != consumed)
            {
                yield return addresses[index];
            }
        }
    }

    /// <summary>
    /// Rejects two fragments that share a node identifier, naming every shared identifier.
    /// </summary>
    /// <param name="upstream">The upstream fragment.</param>
    /// <param name="downstream">The downstream fragment.</param>
    /// <exception cref="ArgumentException">The two fragments share at least one node identifier.</exception>
    /// <remarks>
    /// The exception carries no parameter name because a collision is a relation between the two
    /// fragments and belongs to neither argument alone.
    /// </remarks>
    private static void EnsureDisjointNodes(GraphFragment upstream, GraphFragment downstream)
    {
        HashSet<NodeId> upstreamIds = [.. upstream.Nodes.Select(node => node.Id)];
        List<NodeId> collisions = [.. downstream.Nodes.Select(node => node.Id).Where(upstreamIds.Contains)];

        if (collisions.Count == 0)
        {
            return;
        }

        StringBuilder message = new();

        message.Append(CultureInfo.InvariantCulture, $"Composing two fragments requires disjoint node ids, and these two share {collisions.Count} node ");
        message.Append(collisions.Count == 1 ? "id:" : "ids:");

        for (int index = 0; index < collisions.Count; index++)
        {
            message.Append(Environment.NewLine)
                .Append(CultureInfo.InvariantCulture, $"{index + 1}. '{collisions[index]}'.");
        }

        message.Append(Environment.NewLine)
            .Append(CultureInfo.InvariantCulture, $"Use {nameof(Import)} to rebase one or both fragments below a scope segment first; that is also what makes two copies of one reusable fragment disjoint.");

        throw new ArgumentException(message.ToString());
    }

    /// <summary>
    /// Builds the message for an address that is not open on the side it was supplied for.
    /// </summary>
    /// <param name="address">The offending address.</param>
    /// <param name="role">The list the address was expected in, in prose.</param>
    /// <param name="side">The fragment the list belongs to, in prose.</param>
    /// <param name="open">The addresses that list actually holds.</param>
    /// <returns>A message naming the address, the list, and the list's contents.</returns>
    /// <remarks>
    /// The message renders the address through <see cref="PortAddress.ToString"/>, which is defined for
    /// the default value too, so reporting a default address never throws while reporting it.
    /// </remarks>
    private static string DescribeUnopenPort(
        PortAddress address,
        string role,
        string side,
        IReadOnlyList<PortAddress> open) =>
        $"'{address}' is not an {role} of the {side} fragment, and only an open port can be connected. The {side} fragment's {role}s are: {DescribeAddresses(open)}.";

    /// <summary>
    /// Renders a list of addresses for a diagnostic message.
    /// </summary>
    /// <param name="addresses">The addresses to render.</param>
    /// <returns>The addresses in order, or a literal naming the empty case.</returns>
    private static string DescribeAddresses(IReadOnlyList<PortAddress> addresses) =>
        addresses.Count == 0 ? "none" : string.Join(", ", addresses.Select(address => $"'{address}'"));
}
