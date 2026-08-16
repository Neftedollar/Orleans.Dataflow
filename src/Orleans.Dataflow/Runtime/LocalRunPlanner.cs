using System.Collections;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// Turns a validated graph into the plan one run executes: the document decides the order and the
/// configuration, the binding table decides the behavior.
/// </summary>
/// <remarks>
/// <para>
/// The two halves of a local graph are read here and nowhere else. The document is the statement of
/// topology, so the chain order comes from its edges; it is also the statement of configuration, so a
/// buffer's capacity and policy and an asynchronous stage's concurrency bound are read from its parameter
/// payloads. The binding table is the statement of behavior, so the delegates come from it. Neither is
/// trusted to imply the other: a node whose binding is missing, whose binding has the wrong shape for the
/// stage the document names, or whose payload this runtime cannot read, is a mismatch this type reports
/// rather than a cast failure or a hang inside a running loop.
/// </para>
/// <para>
/// Reading the options from the document rather than from the authoring-side descriptor is deliberate and
/// is what makes the payload real. A capacity that lived only in the binding table would make two
/// documents that differ observably look identical, and a hand-built document's capacity would be
/// decoration the runtime ignored.
/// </para>
/// <para>
/// The order is derived from edges rather than from node order, because a document orders its nodes
/// ordinally by identifier text and nothing obliges that order to be the flow. The authoring API's
/// zero-padded numbering happens to make the two agree for the graphs it closes, but a document this type
/// is handed by anything else has no such property, and a runtime that read the node list would execute
/// such a document in the wrong order rather than reject it. Following edges is also what makes the
/// linearity check real: every shape that is not one chain from one source to one terminal is rejected
/// here, which is the defense that keeps the run loops free of cases they cannot execute.
/// </para>
/// <para>
/// <b>Fusion.</b> The chain is cut into segments at boundaries and nowhere else, so adjacent synchronous
/// stages end up in one segment and one loop. A <c>buffer</c> declares the channel of the next cut; an
/// asynchronous stage cuts and heads the segment that follows. A buffer standing immediately before an
/// asynchronous stage is that stage's own input channel rather than a second one with an empty relay
/// segment between them — which is what an author who writes <c>Buffer(8).SelectAsync(...)</c> means, and
/// it keeps the count of channels equal to the count of boundaries the author wrote.
/// </para>
/// <para>
/// None of the rejections here is reachable through the authoring API. Its generic signatures make the
/// shapes agree by construction, its operators validate their options before building anything, and a
/// graph it closes is a linear chain by construction. They exist for the documents this type will one day
/// be handed by something other than that API.
/// </para>
/// </remarks>
internal static class LocalRunPlanner
{
    /// <summary>Compiles a graph into the plan for one run.</summary>
    /// <param name="graph">The closed graph, already validated against the local stage catalog.</param>
    /// <returns>The plan.</returns>
    /// <exception cref="InvalidOperationException">
    /// The document is not one linear chain, a node has no binding, a binding does not have the shape the
    /// stage it is bound to requires, or a parameterized stage carries a payload this runtime cannot read.
    /// </exception>
    internal static LocalRunPlan Compile(RunnableGraph graph)
    {
        List<NodeId> order = LinearOrder(graph.Document);
        Dictionary<NodeId, StageNode> declarations = Declarations(graph.Document);
        List<LocalSegment> segments = [];
        List<LocalBoundary> boundaries = [];
        List<LocalElementStage> stages = [];
        IEnumerable? elements = null;
        LocalAsyncStage? head = null;
        LocalBoundary? pending = null;
        Func<object?, object?, object?>? folder = null;
        object? seed = null;

        for (int index = 0; index < order.Count; index++)
        {
            LocalStageDescriptor descriptor = Binding(graph, order[index]);
            StageNode declaration = declarations[order[index]];
            bool first = index == 0;
            bool last = index == order.Count - 1;

            switch (descriptor.Kind)
            {
                case LocalStageKind.FromEnumerable when first && !last:
                    elements = LocalDelegateAdapter.Elements(descriptor.Behavior);
                    break;
                case LocalStageKind.Select when !first && !last:
                    Settle();
                    stages.Add(LocalElementStage.Select(LocalDelegateAdapter.Selector(descriptor.Behavior)));
                    break;
                case LocalStageKind.Where when !first && !last:
                    Settle();
                    stages.Add(LocalElementStage.Where(LocalDelegateAdapter.Predicate(descriptor.Behavior)));
                    break;
                case LocalStageKind.Buffer when !first && !last:
                    Settle();
                    pending = Boundary(declaration);
                    break;
                case LocalStageKind.SelectAsync or LocalStageKind.SelectAsyncUnordered when !first && !last:
                    Cut(pending ?? LocalBoundary.Handoff);
                    head = Asynchronous(declaration, descriptor);
                    break;
                case LocalStageKind.Fold when last:
                    Settle();
                    folder = LocalDelegateAdapter.Folder(descriptor.Behavior);
                    seed = descriptor.Seed;
                    break;
                case LocalStageKind.Ignore when last:
                    Settle();
                    break;
                default:
                    throw Foreign(
                        $"the node '{order[index]}' is a '{descriptor.Kind}' stage at position {index + 1} of {order.Count}, where that shape cannot stand");
            }
        }

        segments.Add(new LocalSegment(elements, head, [.. stages], folder));

        if (segments[0].Elements is null)
        {
            throw Foreign("the chain does not begin with a source");
        }

        return new LocalRunPlan(
            segments,
            boundaries,
            seed,
            Slot(graph.Document, order[^1], folder is not null));

        // Closes the segment under construction at a boundary and starts the next one.
        void Cut(LocalBoundary boundary)
        {
            segments.Add(new LocalSegment(elements, head, [.. stages], folder: null));
            boundaries.Add(boundary);
            pending = null;
            elements = null;
            head = null;
            stages.Clear();
        }

        // Materializes a buffer's boundary once something has to stand on the far side of it. Deferring
        // the cut this way is what lets a buffer in front of an asynchronous stage be that stage's own
        // input channel rather than one more channel with an empty segment between them.
        void Settle()
        {
            if (pending is { } boundary)
            {
                Cut(boundary);
            }
        }
    }

    /// <summary>Reads the behavior bound to one node.</summary>
    /// <param name="graph">The graph being compiled.</param>
    /// <param name="node">The node identifier.</param>
    /// <returns>The occurrence's descriptor.</returns>
    /// <exception cref="InvalidOperationException">The node has no binding.</exception>
    private static LocalStageDescriptor Binding(RunnableGraph graph, NodeId node) =>
        graph.LocalBindings.TryGetValue(node, out LocalStageDescriptor? descriptor)
            ? descriptor
            : throw Foreign($"the node '{node}' has no bound behavior");

    /// <summary>Indexes a document's nodes by identifier.</summary>
    /// <param name="document">The document being compiled.</param>
    /// <returns>The nodes, keyed by identifier.</returns>
    /// <remarks>
    /// A document's node identifiers are unique by construction, so the index is total and building it is
    /// the cheapest way to read a node's payload while walking the chain in edge order.
    /// </remarks>
    private static Dictionary<NodeId, StageNode> Declarations(GraphDocument document)
    {
        Dictionary<NodeId, StageNode> declarations = new(document.Nodes.Count);

        foreach (StageNode node in document.Nodes)
        {
            declarations.Add(node.Id, node);
        }

        return declarations;
    }

    /// <summary>Reads a buffer node's payload as the boundary it declares.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <returns>The boundary.</returns>
    /// <exception cref="InvalidOperationException">The payload is not a buffer payload.</exception>
    /// <remarks>
    /// Unreachable for a document validated against the local catalog, whose buffer specification runs the
    /// very same reader as its parameter check. It is here because this type is also handed documents that
    /// were never validated, and a capacity it could not read would otherwise become a channel of some
    /// silently chosen size.
    /// </remarks>
    private static LocalBoundary Boundary(StageNode node) =>
        LocalBufferParameters.TryRead(node.Parameters, out BufferOptions? options, out IReadOnlyList<string> violations)
            ? new LocalBoundary(options!.Capacity, options.OverflowPolicy)
            : throw Foreign(
                $"the buffer '{node.Id}' carries parameters this runtime cannot read: {string.Join("; ", violations)}");

    /// <summary>Reads an asynchronous node's payload and binding as the stage that heads a segment.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <param name="descriptor">The occurrence, which carries the kind and the bound callback.</param>
    /// <returns>The stage.</returns>
    /// <exception cref="InvalidOperationException">
    /// The payload is not a parallelism payload, or the binding is not an asynchronous callback.
    /// </exception>
    /// <remarks>
    /// Ordering comes from the stage the document names and the concurrency bound from its payload, which
    /// is the split the two planes make everywhere: which operator was written is topology, and how many
    /// of its callbacks run at once is configuration.
    /// </remarks>
    private static LocalAsyncStage Asynchronous(StageNode node, LocalStageDescriptor descriptor)
    {
        if (!LocalParallelismParameters.TryRead(
            node.Parameters,
            out ParallelismOptions? options,
            out IReadOnlyList<string> violations))
        {
            throw Foreign(
                $"the asynchronous stage '{node.Id}' carries parameters this runtime cannot read: {string.Join("; ", violations)}");
        }

        return new LocalAsyncStage(
            LocalDelegateAdapter.AsyncSelector(descriptor.Behavior, descriptor.Kind),
            options!.MaxConcurrency,
            descriptor.Kind is LocalStageKind.SelectAsync);
    }

    /// <summary>Orders a document's nodes by following its edges from the one node nothing feeds.</summary>
    /// <param name="document">The document to walk.</param>
    /// <returns>The node identifiers in flow order.</returns>
    /// <exception cref="InvalidOperationException">The document is not one linear chain.</exception>
    private static List<NodeId> LinearOrder(GraphDocument document)
    {
        Dictionary<NodeId, NodeId> next = new(document.Edges.Count);
        HashSet<NodeId> fed = new(document.Edges.Count);

        foreach (GraphEdge edge in document.Edges)
        {
            if (!next.TryAdd(edge.From.Node, edge.To.Node))
            {
                throw Foreign($"the node '{edge.From.Node}' feeds more than one node");
            }

            if (!fed.Add(edge.To.Node))
            {
                throw Foreign($"the node '{edge.To.Node}' is fed by more than one node");
            }
        }

        NodeId? head = null;

        foreach (StageNode node in document.Nodes)
        {
            if (fed.Contains(node.Id))
            {
                continue;
            }

            if (head is not null)
            {
                throw Foreign($"both '{head}' and '{node.Id}' begin a chain");
            }

            head = node.Id;
        }

        if (head is null)
        {
            throw Foreign("no node begins a chain");
        }

        List<NodeId> order = new(document.Nodes.Count);
        HashSet<NodeId> walked = new(document.Nodes.Count);
        NodeId current = head.Value;

        while (walked.Add(current))
        {
            order.Add(current);

            if (!next.TryGetValue(current, out NodeId following))
            {
                break;
            }

            current = following;
        }

        return order.Count == document.Nodes.Count
            ? order
            : throw Foreign(
                $"its {document.Nodes.Count} nodes do not form one chain, and following the edges from '{head}' reached {order.Count} of them");
    }

    /// <summary>Reads the result slot the terminal produces.</summary>
    /// <param name="document">The document being compiled.</param>
    /// <param name="terminal">The identifier of the last node of the chain.</param>
    /// <param name="folds">Whether the terminal is a fold, which is the only shape with a result port.</param>
    /// <returns>The slot name, or <see langword="null"/> when the document declares no result.</returns>
    /// <exception cref="InvalidOperationException">
    /// The document declares more than one result, or declares one that the terminal does not produce.
    /// </exception>
    private static ResultSlotId? Slot(GraphDocument document, NodeId terminal, bool folds)
    {
        if (document.ResultSlots.Count == 0)
        {
            return null;
        }

        if (document.ResultSlots.Count > 1)
        {
            throw Foreign($"it declares {document.ResultSlots.Count} results, and a linear chain produces at most one");
        }

        ResultSlotDefinition declared = document.ResultSlots[0];

        return folds && declared.Producer.Node == terminal
            ? declared.Id
            : throw Foreign($"the result '{declared.Id}' is produced by '{declared.Producer}', which does not terminate the chain");
    }

    /// <summary>Builds the exception for a document this runtime cannot execute.</summary>
    /// <param name="reason">The clause naming what is wrong, read after "cannot be materialized because".</param>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException Foreign(string reason) =>
        new($"The graph cannot be materialized by the local runtime because {reason}. This runtime executes one linear chain of local stages, from one source to one terminal.");
}
