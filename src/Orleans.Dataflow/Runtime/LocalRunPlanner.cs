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
/// check real: every shape this checkpoint cannot execute — a node fed by two others that is not a fan-in,
/// a node that is not a fan-out feeding two, two sources whose streams never meet, a cycle, a component
/// nothing reaches — is rejected here, which is the defense that keeps the run loops free of cases they
/// cannot execute.
/// </para>
/// <para>
/// <b>Several sources are one graph exactly when they converge.</b> A document may begin in as many places
/// as it has junctions to join them again: a walk starts at every node nothing feeds, and what makes those
/// walks one run rather than several is that the whole document is one connected component. Two chains
/// side by side in one document are still refused, and the refusal now says what is actually wrong with
/// them — not that there are two sources, but that nothing joins what they feed, so one outcome would have
/// to speak for two streams that never meet.
/// </para>
/// <para>
/// <b>Branches and fusion.</b> The graph is walked as branches: one from every source, one from every leg
/// of every fan-out, and one from below every fan-in. A branch is cut into segments at boundaries and
/// nowhere else, so adjacent synchronous stages end up in one segment and one loop, exactly as a whole
/// linear graph does. A <c>buffer</c> declares the channel of the next cut; an asynchronous stage cuts and
/// heads the segment that follows, whether it maps its elements or is the callback sink that ends the
/// branch; a junction cuts and is its own segment, because its pump shape is what defines it. A branch that
/// runs into a fan-in ends there, and the junction itself is built by the last branch to arrive at it,
/// because a pump that reads several channels cannot exist until every one of them does. A buffer standing
/// immediately before an asynchronous stage or a junction is that stage's own input channel, and a buffer
/// standing immediately below a junction is that junction's own output channel, rather than a second one
/// with an empty relay segment between them — which is what an author who writes <c>Buffer(8)</c> there
/// means, and it keeps the count of channels equal to the count of boundaries the author wrote.
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
        Dictionary<PortAddress, GraphEdge> downstream = new(document.Edges.Count);
        Dictionary<NodeId, List<GraphEdge>> leaving = new(document.Nodes.Count);
        Dictionary<NodeId, List<GraphEdge>> arriving = new(document.Nodes.Count);

        foreach (GraphEdge edge in document.Edges)
        {
            if (!downstream.TryAdd(edge.From, edge))
            {
                throw Foreign($"the port '{edge.From}' feeds more than one node");
            }

            Attach(leaving, edge.From.Node, edge);
            Attach(arriving, edge.To.Node, edge);
        }

        // Read over the document's own node order rather than over a dictionary, so that a document with
        // two of these says the same thing every time it is refused.
        foreach (StageNode fed in document.Nodes)
        {
            if (arriving.TryGetValue(fed.Id, out List<GraphEdge>? into) && into.Count > 1 && !Joins(fed.Id))
            {
                throw Foreign(
                    $"the node '{fed.Id}' is fed by more than one node, and joining several streams is what a fan-in junction is for");
            }
        }

        List<NodeId> heads = Heads(document, arriving);
        List<LocalSegment> segments = [];
        List<LocalBoundary> boundaries = [];
        List<int> producers = [];
        List<Sink> sinks = [];
        List<int> completesAtStart = [];
        Dictionary<NodeId, (LocalIngressQueue? Queue, object Handle)> controls = [];
        Dictionary<NodeId, int[]> joined = [];
        HashSet<NodeId> walked = new(document.Nodes.Count);
        Queue<(NodeId Start, int Input, PortId Entry)> branches = new();

        for (int index = 0; index < heads.Count; index++)
        {
            branches.Enqueue((heads[index], -1, LocalVocabulary.InputPort));
        }

        while (branches.Count > 0)
        {
            (NodeId start, int input, PortId entry) = branches.Dequeue();

            Branch(start, input, entry);
        }

        if (walked.Count != document.Nodes.Count)
        {
            // A cycle is what this usually is, and it is refused here rather than by a rule of its own:
            // every node of a cycle is fed, so no walk from a source reaches one, and a fan-in whose inputs
            // a cycle feeds is never built at all because the last of its arrivals never comes.
            throw Foreign(
                $"its {document.Nodes.Count} nodes do not form one graph, and following the edges from its sources reached {walked.Count} of them");
        }

        if (Separated(document, leaving, arriving, heads) is { } split)
        {
            throw Foreign(
                split.Begins
                    ? $"both '{split.First}' and '{split.Other}' begin a chain and no junction joins what they feed, so it is two runs written in one document"
                    : $"no path of edges joins '{split.First}' to '{split.Other}', so it is two runs written in one document");
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
        // junction that ends it. A head branch begins at a source and reads no channel; every other branch
        // begins on a leg or below a junction and reads the channel that boundary is.
        void Branch(NodeId start, int input, PortId entry)
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
            PortId port = entry;

            while (true)
            {
                // A joining junction ends this branch wherever it stands, including at the very first node
                // of one: what a branch hands it is a channel, and which of its inputs that channel is is
                // said by the port this branch arrived at.
                if (Joins(current))
                {
                    Meet(declarations[current], port);

                    return;
                }

                // Refused rather than skipped, because skipping would leave the channel this branch was
                // entered on with nobody reading it, and a channel nobody reads is a run that waits
                // forever. It is unreachable for a document whose nodes each have at most one edge into
                // them unless they join several, which is checked above; this is what keeps that reasoning
                // from being load-bearing.
                if (!walked.Add(current))
                {
                    throw Foreign($"the node '{current}' is reached from more than one place");
                }

                StageNode declaration = declarations[current];

                // What a shape may be is decided by what the document connects it to and not by a position
                // in a list, because a graph has no single list to count along. For a chain the two agree
                // exactly: the node nothing feeds is the first and the node that feeds nothing is the last.
                bool first = !arriving.ContainsKey(current);
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
                        fanIn: null,
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
                port = onwards[0].To.Port;
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
                    fanIn: null,
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

            // Ends this branch at a splitting junction. The junction is a segment of its own, because its
            // pump shape is what it is and nothing fuses with it: whatever was under construction is closed
            // at a boundary first, the legs are allocated in the specification's port order — which is
            // rotation order — and one branch is queued behind each of them.
            void Split(StageNode node, LocalStageKind kind, LocalFanOut junction)
            {
                Open(pending ?? LocalBoundary.Handoff);

                int segment = segments.Count;
                IReadOnlyList<OutputPortSpecification> ports = LocalVocabulary.OutputPortsOf(kind);
                List<int> legs = [];
                List<(NodeId Start, int Input, PortId Entry)> queued = [];

                for (int leg = 0; leg < ports.Count; leg++)
                {
                    if (!downstream.TryGetValue(PortAddress.Create(node.Id, ports[leg].Id), out GraphEdge onwards))
                    {
                        continue;
                    }

                    (LocalBoundary boundary, NodeId begins, PortId entered) = Below(onwards);
                    int channel = boundaries.Count;

                    boundaries.Add(boundary);
                    producers.Add(segment);
                    legs.Add(channel);
                    queued.Add((begins, channel, entered));
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
                    fanIn: null,
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

            // Ends this branch at a joining junction, and builds that junction when this is the last branch
            // to arrive at it. Everything under construction is closed at a boundary first, exactly as it
            // is at a splitting junction; the channel that closing produced is the input this branch feeds,
            // and which input that is is the port the branch arrived at. The junction cannot be built any
            // earlier than the last arrival, because a pump that reads several channels needs all of them.
            void Meet(StageNode node, PortId entry)
            {
                Open(pending ?? LocalBoundary.Handoff);

                if (inputs.Count == 0)
                {
                    throw Foreign(
                        $"the junction '{node.Id}' is fed by nothing at the port '{entry}', and a junction joins at least {LocalVocabulary.MinFanIn} inputs");
                }

                LocalStageKind kind = bindings[node.Id].Kind;
                IReadOnlyList<InputPortSpecification> ports = LocalVocabulary.InputPortsOf(kind);
                int arrival = -1;

                for (int input = 0; input < ports.Count && arrival < 0; input++)
                {
                    if (ports[input].Id == entry)
                    {
                        arrival = input;
                    }
                }

                if (arrival < 0)
                {
                    throw Foreign(
                        $"the junction '{node.Id}' is wired at a port the stage '{node.Stage}' does not declare as an input");
                }

                if (!joined.TryGetValue(node.Id, out int[]? arrived))
                {
                    arrived = new int[ports.Count];

                    Array.Fill(arrived, -1);
                    joined.Add(node.Id, arrived);
                }

                if (arrived[arrival] >= 0)
                {
                    throw Foreign($"the junction '{node.Id}' is reached at the port '{entry}' from more than one place");
                }

                arrived[arrival] = inputs[0];

                List<int> streams = [];

                for (int input = 0; input < arrived.Length; input++)
                {
                    if (arrived[input] >= 0)
                    {
                        streams.Add(arrived[input]);
                    }
                }

                if (streams.Count != arriving[node.Id].Count)
                {
                    return;
                }

                if (streams.Count < LocalVocabulary.MinFanIn)
                {
                    throw Foreign(
                        $"the junction '{node.Id}' joins {streams.Count} of its inputs, and a junction joins at least {LocalVocabulary.MinFanIn}");
                }

                if (!leaving.TryGetValue(node.Id, out List<GraphEdge>? onwards) || onwards.Count != 1)
                {
                    throw Foreign(
                        $"the junction '{node.Id}' feeds {onwards?.Count ?? 0} nodes, and a joining junction feeds exactly one");
                }

                _ = walked.Add(node.Id);

                int segment = segments.Count;
                (LocalBoundary boundary, NodeId begins, PortId entered) = Below(onwards[0]);
                int channel = boundaries.Count;

                boundaries.Add(boundary);
                producers.Add(segment);
                segments.Add(new LocalSegment(
                    elements: null,
                    async: null,
                    fanOut: null,
                    Joining(node, kind),
                    [],
                    terminal: null,
                    streams,
                    [channel],
                    -1));
                branches.Enqueue((begins, channel, entered));
            }
        }

        // Reads the boundary the channel below one port of a junction gets, and where the branch behind it
        // begins. A buffer standing immediately below a junction — on a leg of a fan-out, or under the one
        // output of a fan-in — is that channel rather than a second one behind an implicit handoff, which
        // is the rule a buffer in front of an asynchronous stage already follows and what keeps "total
        // memory is the sum of the declared capacities" true across a junction.
        (LocalBoundary Boundary, NodeId Start, PortId Entry) Below(GraphEdge onwards)
        {
            NodeId target = onwards.To.Node;

            if (!bindings.TryGetValue(target, out LocalStageDescriptor? buffer) ||
                buffer.Kind is not LocalStageKind.Buffer ||
                !leaving.TryGetValue(target, out List<GraphEdge>? edges) ||
                edges.Count != 1)
            {
                return (LocalBoundary.Handoff, target, onwards.To.Port);
            }

            _ = walked.Add(target);

            return (Boundary(declarations[target]), edges[0].To.Node, edges[0].To.Port);
        }

        // Reports whether a node is a joining junction, which is the one shape a branch ends at without
        // being walked: it is walked by whichever branch arrives at it last.
        bool Joins(NodeId node) =>
            bindings.TryGetValue(node, out LocalStageDescriptor? descriptor) &&
            LocalVocabulary.PlaceOf(descriptor.Kind) is LocalStagePlace.FanIn;

        // Builds the strategy of one joining junction. The rotation's segment size is read from the
        // document rather than from the binding, for the reason every number is: what the catalog validates
        // has to be exactly what the runtime executes.
        LocalFanIn Joining(StageNode node, LocalStageKind kind) => kind switch
        {
            LocalStageKind.Merge => LocalFanIn.Merge(),
            LocalStageKind.Concat => LocalFanIn.Concat(),
            _ => LocalFanIn.Interleave(Interleaved(node)),
        };
    }

    /// <summary>Adds one edge to the list a node keeps of the edges on one of its sides.</summary>
    /// <param name="edges">The table being built, of edges leaving nodes or of edges arriving at them.</param>
    /// <param name="node">The node the edge belongs to on that side.</param>
    /// <param name="edge">The edge.</param>
    /// <remarks>
    /// Both directions of the document are indexed, and both are needed: the downstream one is the walk,
    /// and the upstream one is what says whether a node is fed by more than one stream and how many
    /// arrivals a junction is waiting for.
    /// </remarks>
    private static void Attach(Dictionary<NodeId, List<GraphEdge>> edges, NodeId node, GraphEdge edge)
    {
        if (edges.TryGetValue(node, out List<GraphEdge>? attached))
        {
            attached.Add(edge);
        }
        else
        {
            edges.Add(node, [edge]);
        }
    }

    /// <summary>Finds the nodes a document's edges begin at.</summary>
    /// <param name="document">The document being compiled.</param>
    /// <param name="arriving">The edges terminating at each node that some edge terminates at.</param>
    /// <returns>The identifiers of the nodes nothing feeds, in the document's own node order.</returns>
    /// <exception cref="InvalidOperationException">There is no such node.</exception>
    /// <remarks>
    /// Several sources are legal now that junctions can join them, and the check that used to live here —
    /// exactly one head — has moved to where the real question is: not how many places a document begins
    /// in, but whether what they feed ever meets. A document with no head at all is still refused here,
    /// because every one of its nodes is fed by another and a run of it could never start; that is what a
    /// document which is nothing but a cycle looks like from this end.
    /// </remarks>
    private static List<NodeId> Heads(GraphDocument document, Dictionary<NodeId, List<GraphEdge>> arriving)
    {
        List<NodeId> heads = [];

        foreach (StageNode node in document.Nodes)
        {
            if (!arriving.ContainsKey(node.Id))
            {
                heads.Add(node.Id);
            }
        }

        return heads.Count > 0 ? heads : throw Foreign("no node begins a chain");
    }

    /// <summary>Finds two nodes of a document that no path of edges joins.</summary>
    /// <param name="document">The document being compiled.</param>
    /// <param name="leaving">The edges leaving each node that has any.</param>
    /// <param name="arriving">The edges arriving at each node that has any.</param>
    /// <param name="heads">The nodes nothing feeds, of which there is at least one.</param>
    /// <returns>
    /// The two nodes and whether the second of them begins a chain, or <see langword="null"/> when the
    /// document is one connected component.
    /// </returns>
    /// <remarks>
    /// <para>
    /// The check that replaced "exactly one source". Following the edges downstream from every head reaches
    /// every node of a graph whose sources converge and of one whose sources do not, so reachability alone
    /// cannot tell the two apart; what tells them apart is whether the document is connected when the edges
    /// are read in both directions, which is exactly "do these streams ever meet".
    /// </para>
    /// <para>
    /// The second node is a head whenever one is available, because that is the honest way to describe two
    /// chains side by side: each of them begins something the other never reaches. The fallback names
    /// whatever node was not reached instead of asserting that a head must exist, so that the reasoning
    /// about why one always does is not load-bearing.
    /// </para>
    /// </remarks>
    private static (NodeId First, NodeId Other, bool Begins)? Separated(
        GraphDocument document,
        Dictionary<NodeId, List<GraphEdge>> leaving,
        Dictionary<NodeId, List<GraphEdge>> arriving,
        List<NodeId> heads)
    {
        HashSet<NodeId> reached = [heads[0]];
        Queue<NodeId> pending = new();

        pending.Enqueue(heads[0]);

        while (pending.Count > 0)
        {
            NodeId node = pending.Dequeue();

            if (leaving.TryGetValue(node, out List<GraphEdge>? onwards))
            {
                for (int index = 0; index < onwards.Count; index++)
                {
                    if (reached.Add(onwards[index].To.Node))
                    {
                        pending.Enqueue(onwards[index].To.Node);
                    }
                }
            }

            if (arriving.TryGetValue(node, out List<GraphEdge>? into))
            {
                for (int index = 0; index < into.Count; index++)
                {
                    if (reached.Add(into[index].From.Node))
                    {
                        pending.Enqueue(into[index].From.Node);
                    }
                }
            }
        }

        if (reached.Count == document.Nodes.Count)
        {
            return null;
        }

        for (int index = 0; index < heads.Count; index++)
        {
            if (!reached.Contains(heads[index]))
            {
                return (heads[0], heads[index], true);
            }
        }

        foreach (StageNode node in document.Nodes)
        {
            if (!reached.Contains(node.Id))
            {
                return (heads[0], node.Id, false);
            }
        }

        return null;
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

    /// <summary>Reads an interleave node's payload as the segment size it declares.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <returns>The number of elements the rotation takes from one input before moving on.</returns>
    /// <exception cref="InvalidOperationException">The payload is not an interleave payload.</exception>
    /// <remarks>
    /// The one number a junction carries, read from the document for the reason every number is read from
    /// it: a segment size that lived only in the binding table would make two graphs that produce different
    /// sequences look identical, and a hand-built document's segment size would be decoration.
    /// </remarks>
    private static int Interleaved(StageNode node) =>
        LocalInterleaveParameters.TryRead(node.Parameters, out int segmentSize, out IReadOnlyList<string> violations)
            ? segmentSize
            : throw Foreign(
                $"the interleave '{node.Id}' carries parameters this runtime cannot read: {string.Join("; ", violations)}");

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
