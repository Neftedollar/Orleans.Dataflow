using System.Collections;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// Turns a validated graph into the plan one run executes: the document decides the order, the binding
/// table decides the behavior.
/// </summary>
/// <remarks>
/// <para>
/// The two halves of a local graph are read here and nowhere else. The document is the statement of
/// topology, so the chain order comes from its edges; the binding table is the statement of behavior, so
/// the delegates come from it. Neither is trusted to imply the other: a node whose binding is missing, or
/// whose binding has the wrong shape for the stage the document names, is a mismatch this type reports
/// rather than a cast failure inside a running loop.
/// </para>
/// <para>
/// The order is derived from edges rather than from node order, because a document orders its nodes
/// ordinally by identifier text and nothing obliges that order to be the flow. The authoring API's
/// zero-padded numbering happens to make the two agree for the graphs it closes, but a document this type
/// is handed by anything else has no such property, and a runtime that read the node list would execute
/// such a document in the wrong order rather than reject it. Following edges is also what makes the
/// linearity check real: every shape that is not one chain from one source to one terminal is rejected
/// here, which is the defense that keeps the run loop free of cases it cannot execute.
/// </para>
/// <para>
/// None of these rejections is reachable through the authoring API. Its generic signatures make the shapes
/// agree by construction, and a graph it closes is a linear chain by construction. They exist for the
/// documents this type will one day be handed by something other than that API.
/// </para>
/// </remarks>
internal static class LocalRunPlanner
{
    /// <summary>Compiles a graph into the plan for one run.</summary>
    /// <param name="graph">The closed graph, already validated against the local stage catalog.</param>
    /// <returns>The plan.</returns>
    /// <exception cref="InvalidOperationException">
    /// The document is not one linear chain, a node has no binding, or a binding does not have the shape
    /// the stage it is bound to requires.
    /// </exception>
    internal static LocalRunPlan Compile(RunnableGraph graph)
    {
        List<NodeId> order = LinearOrder(graph.Document);
        List<LocalElementStage> stages = new(Math.Max(order.Count - 2, 0));
        IEnumerable? elements = null;
        Func<object?, object?, object?>? folder = null;
        object? seed = null;

        for (int index = 0; index < order.Count; index++)
        {
            LocalStageDescriptor descriptor = Binding(graph, order[index]);
            bool first = index == 0;
            bool last = index == order.Count - 1;

            switch (descriptor.Kind)
            {
                case LocalStageKind.FromEnumerable when first && !last:
                    elements = LocalDelegateAdapter.Elements(descriptor.Behavior);
                    break;
                case LocalStageKind.Select when !first && !last:
                    stages.Add(LocalElementStage.Select(LocalDelegateAdapter.Selector(descriptor.Behavior)));
                    break;
                case LocalStageKind.Where when !first && !last:
                    stages.Add(LocalElementStage.Where(LocalDelegateAdapter.Predicate(descriptor.Behavior)));
                    break;
                case LocalStageKind.Fold when last:
                    folder = LocalDelegateAdapter.Folder(descriptor.Behavior);
                    seed = descriptor.Seed;
                    break;
                case LocalStageKind.Ignore when last:
                    break;
                default:
                    throw Foreign(
                        $"the node '{order[index]}' is a '{descriptor.Kind}' stage at position {index + 1} of {order.Count}, where that shape cannot stand");
            }
        }

        if (elements is null)
        {
            throw Foreign("the chain does not begin with a source");
        }

        return new LocalRunPlan(elements, stages, folder, seed, Slot(graph.Document, order[^1], folder is not null));
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
