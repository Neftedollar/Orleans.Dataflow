using System.Collections;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// Turns a validated graph into the plan one run executes: the document decides the shape and the
/// configuration, the binding table decides the behavior.
/// </summary>
/// <remarks>
/// <para>
/// The two halves of a local graph are read here and nowhere else. The document is the statement of
/// topology, so the branch order comes from its edges; it is also the statement of configuration, so a
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
/// decoration the runtime ignored. A junction is the same rule seen from the other side: how many legs one
/// has is stated by the edges alone, so there is no arity payload to disagree with them.
/// </para>
/// <para>
/// The order is derived from edges rather than from node order, because a document orders its nodes
/// ordinally by identifier text and nothing obliges that order to be the flow. The authoring API's
/// zero-padded numbering happens to make the two agree for the graphs it closes, but a document this type
/// is handed by anything else has no such property, and a runtime that read the node list would execute
/// such a document in the wrong order rather than reject it. Following edges is also what makes the shape
/// check real: every shape this checkpoint cannot execute — two sources, a node fed by two others, a node
/// that is not a junction feeding two, a cycle, a component nothing reaches — is rejected here, which is
/// the defense that keeps the run loops free of cases they cannot execute.
/// </para>
/// <para>
/// <b>Branches and fusion.</b> The graph is walked as branches: one from the source, and one more from
/// every leg of every junction. A branch is cut into segments at boundaries and nowhere else, so adjacent
/// synchronous stages end up in one segment and one loop, exactly as a whole linear graph does. A
/// <c>buffer</c> declares the channel of the next cut; an asynchronous stage cuts and heads the segment
/// that follows, whether it maps its elements or is the callback sink that ends the branch; a junction cuts
/// and is its own segment, because its pump shape is what defines it. A buffer standing immediately before
/// an asynchronous stage or a junction is that stage's own input channel, and a buffer standing
/// immediately on a leg is that leg's own channel, rather than a second one with an empty relay segment
/// between them — which is what an author who writes <c>Buffer(8)</c> there means, and it keeps the count
/// of channels equal to the count of boundaries the author wrote.
/// </para>
/// <para>
/// A linear document therefore plans to exactly the segments, channels and fusion it always did: one
/// branch, no junction, one ending. The graph plan is the general case and the chain is its degenerate
/// one, rather than two planners that have to be kept in agreement.
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
    /// <param name="graph">The closed graph, already validated against the host's catalog.</param>
    /// <param name="binder">The resolver of every node no local behavior is bound to.</param>
    /// <param name="runIdentity">What the run this plan is for is called in this process.</param>
    /// <returns>The plan.</returns>
    /// <exception cref="InvalidOperationException">
    /// The document is not a shape this runtime executes, a node has no binding, a binding does not have
    /// the shape the stage it is bound to requires, or a parameterized stage carries a payload this runtime
    /// cannot read.
    /// </exception>
    internal static LocalRunPlan Compile(RunnableGraph graph, StageRuntimeBinder binder, string runIdentity) =>
        Compile(graph.Document, graph.LocalBindings, binder, runIdentity);

    /// <summary>Compiles a document into the plan for one run.</summary>
    /// <param name="document">The document, already validated against the host's catalog.</param>
    /// <param name="bindings">The authoring-side behavior of every locally bound node.</param>
    /// <param name="binder">The resolver of every node no local behavior is bound to.</param>
    /// <param name="runIdentity">What the run this plan is for is called in this deployment.</param>
    /// <returns>The plan.</returns>
    /// <exception cref="InvalidOperationException">
    /// The document is not a shape this runtime executes, a node is neither locally bound nor resolvable
    /// through <paramref name="binder"/>, a binding does not have the shape the stage it is bound to
    /// requires, a resolved stage's shape cannot stand where the node does, or a parameterized stage
    /// carries a payload this runtime cannot read.
    /// </exception>
    /// <remarks>
    /// The two halves of a branch are read the same way whichever plane a node comes from: the document
    /// decides order and configuration, and behavior comes either from the binding table or from a runtime
    /// factory. A mixed document is therefore not a special case here — each node is asked of the table
    /// first and of the binder second, which is exactly the precedence the local vocabulary needs, because
    /// only the local stages are in the table at all.
    /// </remarks>
    internal static LocalRunPlan Compile(
        GraphDocument document,
        IReadOnlyDictionary<NodeId, LocalStageDescriptor> bindings,
        StageRuntimeBinder binder,
        string runIdentity)
    {
        Dictionary<NodeId, StageNode> declarations = Declarations(document);
        Dictionary<PortAddress, NodeId> downstream = new(document.Edges.Count);
        Dictionary<NodeId, List<GraphEdge>> leaving = new(document.Nodes.Count);
        HashSet<NodeId> fed = new(document.Edges.Count);

        foreach (GraphEdge edge in document.Edges)
        {
            if (!downstream.TryAdd(edge.From, edge.To.Node))
            {
                throw Foreign($"the port '{edge.From}' feeds more than one node");
            }

            if (!fed.Add(edge.To.Node))
            {
                throw Foreign($"the node '{edge.To.Node}' is fed by more than one node");
            }

            if (leaving.TryGetValue(edge.From.Node, out List<GraphEdge>? edges))
            {
                edges.Add(edge);
            }
            else
            {
                leaving.Add(edge.From.Node, [edge]);
            }
        }

        NodeId head = Head(document, fed);
        List<LocalSegment> segments = [];
        List<LocalBoundary> boundaries = [];
        List<int> producers = [];
        List<Sink> sinks = [];
        List<int> completesAtStart = [];
        Dictionary<NodeId, (LocalIngressQueue? Queue, object Handle)> controls = [];
        HashSet<NodeId> walked = new(document.Nodes.Count);
        Queue<(NodeId Start, int Input)> branches = new();

        branches.Enqueue((head, -1));

        while (branches.Count > 0)
        {
            (NodeId start, int input) = branches.Dequeue();

            Branch(start, input);
        }

        if (walked.Count != document.Nodes.Count)
        {
            throw Foreign(
                $"its {document.Nodes.Count} nodes do not form one graph, and following the edges from '{head}' reached {walked.Count} of them");
        }

        for (int index = 0; index < segments.Count; index++)
        {
            if (segments[index].Inputs.Count == 0 && segments[index].Elements is null)
            {
                throw Foreign("the graph does not begin with a source");
            }
        }

        (ResultSlotId?[] slots, LocalControl[] declared) = Slots(document, sinks, controls);
        LocalEnding[] endings = new LocalEnding[sinks.Count];

        for (int index = 0; index < endings.Length; index++)
        {
            endings[index] = new LocalEnding(
                sinks[index].Segment,
                sinks[index].Seed,
                sinks[index].SeedFactory,
                slots[index]);
        }

        return new LocalRunPlan(segments, boundaries, producers, endings, declared, completesAtStart);

        // Compiles one maximal junction-free chain, from the node that begins it to the terminal or the
        // junction that ends it. The head branch begins at the source and reads no channel; every other
        // branch begins on a leg and reads the channel that leg is.
        void Branch(NodeId start, int input)
        {
            LocalSource? elements = null;
            LocalAsyncStage? asynchronous = null;
            List<LocalElementStage> stages = [];
            LocalBoundary? pending = null;
            LocalTerminal? terminal = null;
            object? seed = null;
            Func<object?>? seedFactory = null;
            bool produces = false;
            List<int> inputs = input < 0 ? [] : [input];
            NodeId current = start;

            while (true)
            {
                // Refused rather than skipped, because skipping would leave the channel this branch was
                // entered on with nobody reading it, and a channel nobody reads is a run that waits
                // forever. It is unreachable for a document whose nodes each have at most one edge into
                // them, which is checked above; this is what keeps that reasoning from being load-bearing.
                if (!walked.Add(current))
                {
                    throw Foreign($"the node '{current}' is reached from more than one place");
                }

                StageNode declaration = declarations[current];

                // What a shape may be is decided by what the document connects it to and not by a position
                // in a list, because a graph has no single list to count along. For a chain the two agree
                // exactly: the node nothing feeds is the first and the node that feeds nothing is the last.
                bool first = !fed.Contains(current);
                bool last = !leaving.ContainsKey(current);
                int position = walked.Count;

                if (!bindings.TryGetValue(declaration.Id, out LocalStageDescriptor? descriptor))
                {
                    StageRuntime provided = Provided(binder, declaration);

                    switch (provided.Shape)
                    {
                        case StageRuntimeShape.Source when first && !last:
                        {
                            StageSourceOpener open = provided.Opener!;

                            elements = context => LocalSequence.Async(
                                _ => new LocalAsyncCursor<object?>(
                                    open(new StageRunTokens(runIdentity, context.RunToken, context.StopToken))
                                        .GetAsyncEnumerator(context.RunToken)),
                                context);

                            break;
                        }

                        case StageRuntimeShape.Element when !first && !last:
                            Fuse(LocalElementStage.Select(provided.Map!));
                            break;
                        case StageRuntimeShape.ElementAsync when !first && !last:
                            Open(pending ?? LocalBoundary.Handoff);
                            asynchronous = new LocalAsyncStage(
                                provided.AsAsyncCallback(),
                                provided.MaxConcurrency,
                                provided.Ordered);
                            break;
                        case StageRuntimeShape.Terminal when last && !first:
                            Settle();
                            terminal = LocalTerminal.Provided(provided.Fold!, provided.Finish);
                            seedFactory = provided.Seed;
                            produces = provided.ProducesResult;
                            break;
                        default:
                            throw Foreign(
                                $"the node '{declaration.Id}' is an occurrence of the stage '{declaration.Stage}', whose runtime factory built a '{provided.Shape}' shape, and that shape cannot stand at position {position} of {document.Nodes.Count}");
                    }
                }
                else
                {
                    switch (descriptor.Kind)
                    {
                        case LocalStageKind.FromEnumerable when first && !last:
                        {
                            IEnumerable sequence = LocalDelegateAdapter.Elements(descriptor.Behavior, descriptor.Kind);

                            elements = _ => sequence;

                            break;
                        }

                        case LocalStageKind.Empty when first && !last:
                            elements = static _ => LocalSequence.Empty();
                            break;
                        case LocalStageKind.Single when first && !last:
                        {
                            object? value = descriptor.Behavior;

                            elements = _ => LocalSequence.Single(value);

                            break;
                        }

                        case LocalStageKind.Repeat when first && !last:
                        {
                            object? value = descriptor.Behavior;
                            int count = Count(declaration);

                            elements = _ => LocalSequence.Repeat(value, count);

                            break;
                        }

                        case LocalStageKind.Range when first && !last:
                        {
                            (int from, int count) = Range(declaration);

                            elements = _ => LocalSequence.Range(from, count);

                            break;
                        }

                        case LocalStageKind.FromTask when first && !last:
                        {
                            Func<object?> value = LocalDelegateAdapter.TaskValue(descriptor.Behavior);

                            elements = _ => LocalSequence.Deferred(value);

                            break;
                        }

                        case LocalStageKind.Failed when first && !last:
                        {
                            Exception failure = LocalDelegateAdapter.Failure(descriptor.Behavior);

                            elements = _ => LocalSequence.Failed(failure);

                            break;
                        }

                        case LocalStageKind.Unfold when first && !last:
                        {
                            object? state = descriptor.Seed;
                            LocalGenerator generator = LocalDelegateAdapter.Generator(descriptor.Behavior);

                            elements = _ => LocalSequence.Unfold(state, generator);

                            break;
                        }

                        case LocalStageKind.FromAsyncEnumerable when first && !last:
                        {
                            LocalAsyncCursorFactory open = LocalDelegateAdapter.AsyncCursors(descriptor.Behavior);

                            elements = context => LocalSequence.Async(open, context);

                            break;
                        }

                        case LocalStageKind.FromFactory when first && !last:
                        {
                            Func<object?> factory = LocalDelegateAdapter.Factory(descriptor.Behavior);

                            elements = _ => LocalSequence.Deferred(factory);

                            break;
                        }

                        case LocalStageKind.FromAsyncFactory when first && !last:
                        {
                            Func<CancellationToken, object?> factory =
                                LocalDelegateAdapter.AsyncFactory(descriptor.Behavior);

                            elements = context => LocalSequence.Deferred(() => factory(context.RunToken));

                            break;
                        }

                        case LocalStageKind.Never when first && !last:
                            elements = static context => LocalSequence.Never(context);
                            break;
                        case LocalStageKind.Cycle when first && !last:
                        {
                            IEnumerable cycled = LocalDelegateAdapter.Elements(descriptor.Behavior, descriptor.Kind);

                            elements = _ => LocalSequence.Cycle(cycled);

                            break;
                        }

                        case LocalStageKind.UnfoldAsync when first && !last:
                        {
                            object? state = descriptor.Seed;
                            LocalAsyncGenerator generator = LocalDelegateAdapter.AsyncGenerator(descriptor.Behavior);

                            elements = context => LocalSequence.UnfoldAsync(state, generator, context);

                            break;
                        }

                        case LocalStageKind.Queue when first && !last:
                        {
                            LocalIngressQueue queue = Ingress(declaration);
                            object handle = LocalDelegateAdapter.QueueFacade(descriptor.Behavior)(queue);

                            controls.Add(current, (queue, handle));
                            elements = queue.Elements;

                            break;
                        }

                        case LocalStageKind.FromChannel when first && !last:
                        {
                            LocalChannelSource reader = LocalDelegateAdapter.ChannelSource(descriptor.Behavior);

                            elements = context => LocalSequence.Channel(reader, context);

                            break;
                        }

                        case LocalStageKind.Select when !first && !last:
                            Fuse(LocalElementStage.Select(LocalDelegateAdapter.Selector(descriptor.Behavior)));
                            break;
                        case LocalStageKind.Where when !first && !last:
                            Fuse(LocalElementStage.Where(Predicate(descriptor)));
                            break;
                        case LocalStageKind.Scan when !first && !last:
                            Fuse(LocalElementStage.Scan(
                                descriptor.Seed,
                                LocalDelegateAdapter.Folder(descriptor.Behavior, descriptor.Kind)));
                            break;
                        case LocalStageKind.Take when !first && !last:
                            Fuse(LocalElementStage.Take(Count(declaration)));
                            break;
                        case LocalStageKind.Skip when !first && !last:
                            Fuse(LocalElementStage.Skip(Count(declaration)));
                            break;
                        case LocalStageKind.TakeWhile when !first && !last:
                            Fuse(LocalElementStage.TakeWhile(Predicate(descriptor), inclusive: false));
                            break;
                        case LocalStageKind.TakeThrough when !first && !last:
                            Fuse(LocalElementStage.TakeWhile(Predicate(descriptor), inclusive: true));
                            break;
                        case LocalStageKind.SkipWhile when !first && !last:
                            Fuse(LocalElementStage.SkipWhile(Predicate(descriptor)));
                            break;
                        case LocalStageKind.Distinct when !first && !last:
                            Fuse(LocalElementStage.Distinct(
                                Distinct(declaration),
                                LocalDelegateAdapter.Comparer(descriptor.Behavior)));
                            break;
                        case LocalStageKind.Buffer when !first && !last:
                            Settle();
                            pending = Boundary(declaration);
                            break;
                        case LocalStageKind.SelectAsync or
                            LocalStageKind.SelectAsyncUnordered or
                            LocalStageKind.SelectValueTaskAsync or
                            LocalStageKind.SelectValueTaskAsyncUnordered when !first && !last:
                            Open(pending ?? LocalBoundary.Handoff);
                            asynchronous = Asynchronous(declaration, descriptor);
                            break;
                        case LocalStageKind.Broadcast when !first && !last:
                            Split(declaration, descriptor.Kind, LocalFanOut.Broadcast());
                            return;
                        case LocalStageKind.Balance when !first && !last:
                            Split(declaration, descriptor.Kind, LocalFanOut.Balance());
                            return;
                        case LocalStageKind.Unzip when !first && !last:
                            Split(
                                declaration,
                                descriptor.Kind,
                                LocalFanOut.Unzip(LocalDelegateAdapter.Halves(descriptor.Behavior)));
                            return;
                        case LocalStageKind.Fold when last:
                            Settle();
                            terminal = LocalTerminal.Folding(
                                LocalDelegateAdapter.Folder(descriptor.Behavior, descriptor.Kind));
                            seed = descriptor.Seed;
                            produces = true;
                            break;
                        case LocalStageKind.Ignore when last:
                            Settle();
                            break;
                        case LocalStageKind.ForEach when last:
                            Settle();
                            terminal = LocalTerminal.Calling(LocalDelegateAdapter.Action(descriptor.Behavior));
                            break;
                        case LocalStageKind.ForEachAsync when last && !first:
                            Open(pending ?? LocalBoundary.Handoff);
                            asynchronous = Asynchronous(declaration, descriptor);
                            break;
                        case LocalStageKind.First or LocalStageKind.FirstOrDefault when last:
                            Settle();
                            terminal = LocalTerminal.FirstElement(descriptor.Kind is LocalStageKind.First);
                            seed = descriptor.Seed;
                            produces = true;
                            break;
                        case LocalStageKind.Count when last:
                            Settle();
                            terminal = LocalTerminal.Counting();
                            seed = descriptor.Seed;
                            produces = true;
                            break;
                        case LocalStageKind.Last or LocalStageKind.LastOrDefault when last:
                            Settle();
                            terminal = LocalTerminal.LastElement(descriptor.Kind is LocalStageKind.Last);
                            seed = descriptor.Seed;
                            produces = true;
                            break;
                        case LocalStageKind.Collect when last:
                            Settle();
                            terminal = LocalTerminal.Collecting(
                                Collected(declaration),
                                LocalDelegateAdapter.Freeze(descriptor.Behavior));
                            seedFactory = static () => new List<object?>();
                            produces = true;
                            break;
                        case LocalStageKind.ToChannel when last:
                            Settle();
                            terminal = LocalTerminal.Channel(LocalDelegateAdapter.ChannelSink(descriptor.Behavior));
                            break;
                        case LocalStageKind.SinkProbe when last && !first:
                        {
                            Settle();

                            LocalSinkProbe probe = new();

                            controls.Add(current, (Queue: null, LocalDelegateAdapter.ProbeFacade(descriptor.Behavior)(probe)));
                            terminal = LocalTerminal.Probing(probe);

                            break;
                        }

                        default:
                            throw Foreign(
                                $"the node '{current}' is a '{descriptor.Kind}' stage at position {position} of {document.Nodes.Count}, where that shape cannot stand");
                    }
                }

                if (last)
                {
                    segments.Add(new LocalSegment(
                        elements,
                        asynchronous,
                        fanOut: null,
                        [.. stages],
                        terminal,
                        inputs,
                        [],
                        sinks.Count));
                    sinks.Add(new Sink(segments.Count - 1, seed, seedFactory, current, produces));

                    return;
                }

                List<GraphEdge> onwards = leaving[current];

                if (onwards.Count != 1)
                {
                    throw Foreign($"the node '{current}' feeds more than one node");
                }

                current = onwards[0].To.Node;
            }

            // Adds one synchronous stage to the segment under construction, opening the segment a pending
            // buffer declared if this is the first thing to stand on its far side, and remembering a stage
            // whose stream is over before the run begins.
            void Fuse(LocalElementStage stage)
            {
                Settle();
                stages.Add(stage);

                if (stage.CompletesBeforeAnyElement)
                {
                    completesAtStart.Add(segments.Count);
                }
            }

            // Closes the segment under construction at a boundary and starts the next one of this branch.
            void Cut(LocalBoundary boundary)
            {
                int channel = boundaries.Count;

                boundaries.Add(boundary);
                producers.Add(segments.Count);
                segments.Add(new LocalSegment(
                    elements,
                    asynchronous,
                    fanOut: null,
                    [.. stages],
                    terminal: null,
                    inputs,
                    [channel],
                    -1));

                pending = null;
                elements = null;
                asynchronous = null;
                stages.Clear();
                inputs = [channel];
            }

            // Closes the segment under construction at a boundary, unless there is nothing to close. A
            // branch that begins at an asynchronous stage or at a junction was entered on a channel of its
            // own, and cutting an empty segment there would put a relay holding nothing between that
            // channel and the stage that reads it — one more thread, one more channel, and one more
            // element of slack than the author asked for.
            void Open(LocalBoundary boundary)
            {
                if (elements is not null || asynchronous is not null || stages.Count > 0 || pending is not null)
                {
                    Cut(boundary);
                }
            }

            // Materializes a buffer's boundary once something has to stand on the far side of it. Deferring
            // the cut this way is what lets a buffer in front of an asynchronous stage or a junction be
            // that stage's own input channel rather than one more channel with an empty segment between
            // them.
            void Settle()
            {
                if (pending is { } boundary)
                {
                    Cut(boundary);
                }
            }

            // Ends this branch at a junction. The junction is a segment of its own, because its pump shape
            // is what it is and nothing fuses with it: whatever was under construction is closed at a
            // boundary first, the legs are allocated in the specification's port order — which is rotation
            // order — and one branch is queued behind each of them.
            void Split(StageNode node, LocalStageKind kind, LocalFanOut junction)
            {
                Open(pending ?? LocalBoundary.Handoff);

                int segment = segments.Count;
                IReadOnlyList<OutputPortSpecification> ports = LocalVocabulary.OutputPortsOf(kind);
                List<int> legs = [];
                List<(NodeId Start, int Input)> queued = [];

                for (int port = 0; port < ports.Count; port++)
                {
                    if (!downstream.TryGetValue(PortAddress.Create(node.Id, ports[port].Id), out NodeId target))
                    {
                        continue;
                    }

                    (LocalBoundary boundary, NodeId begins) = Leg(target);
                    int channel = boundaries.Count;

                    boundaries.Add(boundary);
                    producers.Add(segment);
                    legs.Add(channel);
                    queued.Add((begins, channel));
                }

                if (legs.Count != leaving[node.Id].Count)
                {
                    throw Foreign(
                        $"the junction '{node.Id}' is wired at a port the stage '{node.Stage}' does not declare as an output");
                }

                if (legs.Count < LocalVocabulary.MinFanOut)
                {
                    throw Foreign(
                        $"the junction '{node.Id}' connects {legs.Count} of its outputs, and a junction routes to at least {LocalVocabulary.MinFanOut}");
                }

                if (junction.Halves is { } halves && halves.Count != legs.Count)
                {
                    throw Foreign(
                        $"the junction '{node.Id}' splits a row into {halves.Count} parts and connects {legs.Count} outputs");
                }

                segments.Add(new LocalSegment(
                    elements: null,
                    async: null,
                    junction,
                    [],
                    terminal: null,
                    inputs,
                    legs,
                    -1));

                for (int leg = 0; leg < queued.Count; leg++)
                {
                    branches.Enqueue(queued[leg]);
                }
            }
        }

        // Reads the boundary one leg of a junction gets and where the branch behind it begins. A buffer
        // standing immediately on a leg is that leg's own channel rather than a second one behind an
        // implicit handoff, which is the rule a buffer in front of an asynchronous stage already follows
        // and what keeps "total memory is the sum of the declared capacities" true across a junction.
        (LocalBoundary Boundary, NodeId Start) Leg(NodeId target)
        {
            if (!bindings.TryGetValue(target, out LocalStageDescriptor? buffer) ||
                buffer.Kind is not LocalStageKind.Buffer ||
                !leaving.TryGetValue(target, out List<GraphEdge>? edges) ||
                edges.Count != 1)
            {
                return (LocalBoundary.Handoff, target);
            }

            _ = walked.Add(target);

            return (Boundary(declarations[target]), edges[0].To.Node);
        }
    }

    /// <summary>Finds the one node a document's edges begin at.</summary>
    /// <param name="document">The document being compiled.</param>
    /// <param name="fed">The nodes some edge terminates at.</param>
    /// <returns>The identifier of the node nothing feeds.</returns>
    /// <exception cref="InvalidOperationException">There is not exactly one such node.</exception>
    /// <remarks>
    /// One source and no more, in this checkpoint. Two sources are not a fan-in — they are two runs written
    /// in one document, whose elements never meet — and executing them as one would give a single outcome
    /// to two independent streams. The junctions that really do join several inputs arrive with the fan-in
    /// pump, and the refusal is what keeps this checkpoint's promise honest until they do.
    /// </remarks>
    private static NodeId Head(GraphDocument document, HashSet<NodeId> fed)
    {
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

        return head ?? throw Foreign("no node begins a chain");
    }

    /// <summary>Resolves a node no local behavior is bound to through the runtime-factory seam.</summary>
    /// <param name="binder">The resolver this compilation was given.</param>
    /// <param name="node">The node as the document declares it.</param>
    /// <returns>The executable form of the node.</returns>
    /// <exception cref="InvalidOperationException">
    /// The node's stage does not resolve in the host's catalog, its provider has no registered factory, or
    /// the factory refused to build it.
    /// </exception>
    /// <remarks>
    /// The message names the stage rather than only the node, because there are two ways to reach it and
    /// the stage is what tells them apart: a document from somewhere else naming stages this process never
    /// bound anything to, and a registered occurrence whose behavior is deliberately not in the binding
    /// table at all. The second is what the seam exists for, and a host without the matching factory says
    /// exactly that rather than half-executing the graph.
    /// </remarks>
    private static StageRuntime Provided(StageRuntimeBinder binder, StageNode node) =>
        binder.TryCreate(node, out StageRuntime? runtime, out string? refusal)
            ? runtime
            : throw Foreign(
                $"the node '{node.Id}' is an occurrence of the stage '{node.Stage}', and no local behavior is bound to it; {refusal}");

    /// <summary>Indexes a document's nodes by identifier.</summary>
    /// <param name="document">The document being compiled.</param>
    /// <returns>The nodes, keyed by identifier.</returns>
    /// <remarks>
    /// A document's node identifiers are unique by construction, so the index is total and building it is
    /// the cheapest way to read a node's payload while walking the graph in edge order.
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

    /// <summary>Reads a counted node's payload as the number of elements it declares.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <returns>The count.</returns>
    /// <exception cref="InvalidOperationException">The payload is not a count payload.</exception>
    /// <remarks>
    /// Unreachable for a document validated against the local catalog, whose counted stages run the very
    /// same reader as their parameter check. It is here because this type is also handed documents that
    /// were never validated, and a count it could not read would otherwise become a bound of some silently
    /// chosen size.
    /// </remarks>
    private static int Count(StageNode node) =>
        LocalCountParameters.TryRead(node.Parameters, out int count, out IReadOnlyList<string> violations)
            ? count
            : throw Foreign(
                $"the node '{node.Id}' carries parameters this runtime cannot read: {string.Join("; ", violations)}");

    /// <summary>Reads a range node's payload as the bounds it declares.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <returns>The first integer and how many follow it.</returns>
    /// <exception cref="InvalidOperationException">The payload is not a range payload.</exception>
    private static (int Start, int Count) Range(StageNode node) =>
        LocalRangeParameters.TryRead(node.Parameters, out int start, out int count, out IReadOnlyList<string> violations)
            ? (start, count)
            : throw Foreign(
                $"the range '{node.Id}' carries parameters this runtime cannot read: {string.Join("; ", violations)}");

    /// <summary>Reads a collecting node's payload as the element bound it declares.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <returns>The greatest number of elements the sink collects.</returns>
    /// <exception cref="InvalidOperationException">The payload is not a collect payload.</exception>
    private static int Collected(StageNode node) =>
        LocalCollectParameters.TryRead(node.Parameters, out CollectOptions? options, out IReadOnlyList<string> violations)
            ? options!.MaxElements
            : throw Foreign(
                $"the collecting sink '{node.Id}' carries parameters this runtime cannot read: {string.Join("; ", violations)}");

    /// <summary>Builds the ingress queue one run offers into, from the node's declared bounds.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <returns>The queue, which belongs to this materialization and to no other.</returns>
    /// <exception cref="InvalidOperationException">The payload is not a buffer payload.</exception>
    /// <remarks>
    /// A queue carries a buffer's payload under a buffer's contract, because its capacity and its overflow
    /// policy are exactly a buffer's; the stage reference is what says that this one stands at the head of
    /// a chain rather than in the middle of it.
    /// </remarks>
    private static LocalIngressQueue Ingress(StageNode node) =>
        LocalBufferParameters.TryRead(node.Parameters, out BufferOptions? options, out IReadOnlyList<string> violations)
            ? new LocalIngressQueue(options!.Capacity, options.OverflowPolicy)
            : throw Foreign(
                $"the queue '{node.Id}' carries parameters this runtime cannot read: {string.Join("; ", violations)}");

    /// <summary>Reads a distinct node's payload as the key bound it declares.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <returns>The greatest number of keys the stage may remember.</returns>
    /// <exception cref="InvalidOperationException">The payload is not a distinct payload.</exception>
    private static int Distinct(StageNode node) =>
        LocalDistinctParameters.TryRead(node.Parameters, out DistinctOptions? options, out IReadOnlyList<string> violations)
            ? options!.MaxTrackedKeys
            : throw Foreign(
                $"the distinct stage '{node.Id}' carries parameters this runtime cannot read: {string.Join("; ", violations)}");

    /// <summary>Reads a node's binding as the predicate its shape requires.</summary>
    /// <param name="descriptor">The occurrence, which carries the kind and the bound delegate.</param>
    /// <returns>The wrapped predicate.</returns>
    /// <exception cref="InvalidOperationException">The binding is not a predicate.</exception>
    /// <remarks>
    /// Four shapes test elements with a predicate and are told apart by their stage reference alone, so the
    /// kind travels into the diagnostic and the wrapping is one call.
    /// </remarks>
    private static Func<object?, bool> Predicate(LocalStageDescriptor descriptor) =>
        LocalDelegateAdapter.Predicate(descriptor.Behavior, descriptor.Kind);

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
    /// of its callbacks run at once is configuration. So does the shape of the callback, and it is resolved
    /// here and nowhere else: the two value-task spellings are converted into the one callback shape the
    /// asynchronous driver knows, so that everything that driver promises is promised once.
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
            descriptor.Kind switch
            {
                LocalStageKind.ForEachAsync => LocalDelegateAdapter.AsyncCallback(descriptor.Behavior),
                LocalStageKind.SelectValueTaskAsync or LocalStageKind.SelectValueTaskAsyncUnordered =>
                    LocalDelegateAdapter.ValueTaskSelector(descriptor.Behavior, descriptor.Kind),
                _ => LocalDelegateAdapter.AsyncSelector(descriptor.Behavior, descriptor.Kind),
            },
            options!.MaxConcurrency,
            descriptor.Kind is LocalStageKind.SelectAsync or LocalStageKind.SelectValueTaskAsync);
    }

    /// <summary>Sorts the document's declared slots into the endings' results and the run's controls.</summary>
    /// <param name="document">The document being compiled.</param>
    /// <param name="sinks">The endings this compilation found, in the order it found them.</param>
    /// <param name="controls">The per-run controls this plan built, keyed by node identifier.</param>
    /// <returns>One slot name per ending, if any, and one <see cref="LocalControl"/> per control.</returns>
    /// <exception cref="InvalidOperationException">
    /// The document declares a slot no node of this graph produces, declares two on one producer, or leaves
    /// a control-bearing stage without one.
    /// </exception>
    /// <remarks>
    /// <para>
    /// Two kinds of slot and one mechanism. A control slot is produced by a <c>control</c> port — a queue's
    /// at the head of a branch, a probe sink's at the end of one — and its value exists from the start of
    /// the run; an ending's slot is produced by its terminal's <c>result</c> port and its value exists at
    /// the end. Everything else about them is the same, which is why they travel together in one document
    /// and resolve through one handle.
    /// </para>
    /// <para>
    /// A graph with several sinks declares several results, and each of them is matched to the ending that
    /// produces it: the slots are per terminal and never per run, so two sinks resolve two values and
    /// neither can be read from the other's fold.
    /// </para>
    /// <para>
    /// A stage that produces a control and declares no slot for it is rejected rather than run. Nothing
    /// else can reach that control, so such a run would wait for a producer or a receiver that cannot
    /// exist — a hang, which is a worse answer than a sentence. It is unreachable through the authoring
    /// API, where declaring the stage is what names the control.
    /// </para>
    /// </remarks>
    private static (ResultSlotId?[] Slots, LocalControl[] Controls) Slots(
        GraphDocument document,
        IReadOnlyList<Sink> sinks,
        Dictionary<NodeId, (LocalIngressQueue? Queue, object Handle)> controls)
    {
        ResultSlotId?[] results = new ResultSlotId?[sinks.Count];
        Dictionary<NodeId, LocalControl> named = [];

        foreach (ResultSlotDefinition declared in document.ResultSlots)
        {
            if (declared.Producer.Port == LocalVocabulary.ControlPort &&
                controls.TryGetValue(declared.Producer.Node, out (LocalIngressQueue? Queue, object Handle) control))
            {
                if (!named.TryAdd(declared.Producer.Node, new LocalControl(declared.Id, control.Handle, control.Queue)))
                {
                    throw Foreign($"the stage '{declared.Producer.Node}' declares more than one control slot");
                }

                continue;
            }

            int ending = Producer(sinks, declared.Producer.Node);

            if (ending < 0)
            {
                throw Foreign(
                    $"the result '{declared.Id}' is produced by '{declared.Producer}', which is neither a terminal of the graph nor the control port of one of its stages");
            }

            if (results[ending] is not null)
            {
                throw Foreign($"the terminal '{declared.Producer.Node}' declares more than one result slot");
            }

            results[ending] = declared.Id;
        }

        foreach (NodeId node in controls.Keys)
        {
            if (!named.ContainsKey(node))
            {
                throw Foreign(
                    $"the stage '{node}' declares no control slot, so nothing could ever reach the control it produces");
            }
        }

        return (results, [.. named.Values]);
    }

    /// <summary>Finds the ending one node terminates, when that node produces a result at all.</summary>
    /// <param name="sinks">The endings this compilation found.</param>
    /// <param name="node">The node a slot names as its producer.</param>
    /// <returns>The ending's position, or minus one when no ending of this graph produces a result there.</returns>
    private static int Producer(IReadOnlyList<Sink> sinks, NodeId node)
    {
        for (int index = 0; index < sinks.Count; index++)
        {
            if (sinks[index].Produces && sinks[index].Node == node)
            {
                return index;
            }
        }

        return -1;
    }

    /// <summary>Builds the exception for a document this runtime cannot execute.</summary>
    /// <param name="reason">The clause naming what is wrong, read after "cannot be materialized because".</param>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException Foreign(string reason) =>
        new($"The graph cannot be materialized by the local runtime because {reason}. This runtime executes one graph of local stages, from one source through its junctions to its terminals.");

    /// <summary>One ending of the plan under construction, as the walk found it.</summary>
    /// <param name="Segment">The position of the segment the branch stops at.</param>
    /// <param name="Seed">The terminal's initial state.</param>
    /// <param name="SeedFactory">The maker of the terminal's initial state, when it cannot be shared.</param>
    /// <param name="Node">The identifier of the terminal, which a result slot names as its producer.</param>
    /// <param name="Produces">Whether that terminal declares a result port.</param>
    /// <remarks>
    /// The identifier and the flag live only long enough to match the document's declared slots to the
    /// endings that resolve them, which is why they are here and not on <see cref="LocalEnding"/>: a plan
    /// states what a run executes, and a node identifier is not something a run ever reads.
    /// </remarks>
    private readonly record struct Sink(
        int Segment,
        object? Seed,
        Func<object?>? SeedFactory,
        NodeId Node,
        bool Produces);
}
