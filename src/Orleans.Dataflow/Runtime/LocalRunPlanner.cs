using System.Collections;
using System.Globalization;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

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
/// check real: every shape this runtime cannot execute — a node fed by two others that is not a fan-in,
/// a node that is not a fan-out feeding two, two sources whose streams never meet, a cycle that waits for
/// itself, a cycle nothing feeds, a graph with no terminal, a component nothing reaches — is rejected here,
/// which is the defense that keeps the run loops free of cases they cannot execute.
/// </para>
/// <para>
/// <b>A cycle is a shape the walk is told about before it starts.</b> ADR 0005 makes a loop legal exactly
/// when it passes a boundary that can answer without room below it, and that is decided first, over the
/// graph with those boundaries deleted, so a loop that waits for itself is named as one rather than
/// discovered as nodes nobody reached. What survives is compiled by cutting the same edges any depth-first
/// walk would call back edges: those inputs of a joining junction are places kept rather than channels
/// waited for, because the branch that carries one begins below the junction it feeds and could not exist
/// until the junction did. Everything else about a cyclic plan is the acyclic one.
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
    /// <param name="clock">The host's clock, which every stage of this run that reads one reads.</param>
    /// <returns>The plan.</returns>
    /// <exception cref="InvalidOperationException">
    /// The document is not a shape this runtime executes, a node has no binding, a binding does not have
    /// the shape the stage it is bound to requires, or a parameterized stage carries a payload this runtime
    /// cannot read.
    /// </exception>
    internal static LocalRunPlan Compile(
        RunnableGraph graph,
        StageRuntimeBinder binder,
        string runIdentity,
        TimeProvider clock) =>
        Compile(graph.Document, graph.LocalBindings, binder, runIdentity, clock);

    /// <summary>Compiles a document into the plan for one run.</summary>
    /// <param name="document">The document, already validated against the host's catalog.</param>
    /// <param name="bindings">The authoring-side behavior of every locally bound node.</param>
    /// <param name="binder">The resolver of every node no local behavior is bound to.</param>
    /// <param name="runIdentity">What the run this plan is for is called in this deployment.</param>
    /// <param name="clock">The host's clock, which every stage of this run that reads one reads.</param>
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
        string runIdentity,
        TimeProvider clock)
    {
        Dictionary<NodeId, StageNode> declarations = Declarations(document);
        Dictionary<NodeId, ProvidedStage> provided = [];
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

        // The cycle rule of ADR 0005, before anything is walked and before a head is even looked for: a
        // cycle every one of whose boundaries waits for room below it is a deadlock by construction, and
        // naming it as one is more useful than whatever a graph with no source is told afterwards. The
        // reduced graph is the whole of the rule — drop the nodes that can answer without room and any
        // cycle that survives is a cycle that passes none of them.
        HashSet<NodeId> relieving = Relieving(document, bindings, declarations);

        if (Cycle(document, leaving, node => !relieving.Contains(node)) is { } deadlocked)
        {
            throw Foreign(
                $"the cycle {Path(deadlocked)} passes no boundary that can answer without room below it, and in a pull engine such a loop waits for room only its own waiter could make; give one of its edges a buffer whose overflow policy is not backpressure");
        }

        List<NodeId> heads = Heads(document, arriving);
        HashSet<NodeId> reachable = Reachable(leaving, heads);

        // A legal cycle nothing outside feeds can never hold an element, so a run of it would idle
        // forever. It is refused here rather than left to the connectivity message below, which would say
        // only that some nodes were not reached and not why they never could be.
        if (Cycle(document, leaving, node => !reachable.Contains(node)) is { } sealedOff)
        {
            throw Foreign(
                $"the cycle {Path(sealedOff)} is fed by nothing outside it, so no element could ever enter it");
        }

        HashSet<PortAddress> feedback = Feedback(leaving, heads);

        foreach (PortAddress port in feedback)
        {
            // The invariant the plan below rests on, checked rather than reasoned about. A back edge of a
            // walk rooted at the heads always terminates at a node that also has a tree edge into it, so
            // that node is fed by more than one stream and the rule above has already required it to be a
            // fan-in; this is what keeps that argument from being load-bearing.
            if (!Joins(port.Node))
            {
                throw Foreign(
                    $"the node '{port.Node}' closes a cycle at the port '{port.Port}' and is not a fan-in junction, so nothing in it could join what comes round with what comes in");
            }
        }

        List<LocalSegment> segments = [];
        List<LocalBoundary> boundaries = [];
        List<int> producers = [];
        List<Sink> sinks = [];
        List<int> completesAtStart = [];
        List<int> feedbackChannels = [];
        Dictionary<NodeId, (LocalIngressQueue? Queue, object Handle)> controls = [];
        Dictionary<NodeId, Arrivals> joined = [];
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
            // A cycle used to be what this was, and no longer is: a legal cycle is walked through its
            // reserved feedback inputs, and an unreachable one is refused above by name. What is left is a
            // node no walk arrives at for some reason nothing else has caught, and every argument that
            // there is no such reason ends in "because a node that is fed by nothing is a head, and one
            // that is fed only from inside a loop no source reaches has already been refused". That
            // argument is not load-bearing, which is what this guard is for.
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

        // Newly reachable now that a cycle can be planned: a graph every one of whose branches runs back
        // into a junction has no terminal at all, and a run of it would move elements forever with nobody
        // to report an outcome. Without cycles this is impossible, because following the edges of a finite
        // acyclic graph always reaches a node that feeds nothing.
        if (sinks.Count == 0)
        {
            throw Foreign("no branch of it ends in a terminal, so nothing consumes what its stages produce");
        }

        foreach ((NodeId node, Arrivals arrivals) in joined)
        {
            if (arrivals.Streams is { } streams && streams.Contains(-1))
            {
                throw Foreign(
                    $"the junction '{node}' closes a cycle at an input no branch of the graph ever reached");
            }
        }

        // The two planes have to be talking about the same node, and until this checkpoint nothing said
        // so where the shapes were indistinguishable: a document naming a merge could carry a binding that
        // is a zip, and the binding — which is the statement of behavior — simply won, so the run's
        // fingerprint, its validation, and its diagnostics all described a graph it was not executing.
        // The disagreement then surfaced as whatever the other pump happens to do, which for a junction of
        // the same arity is a different completion rule rather than an error.
        //
        // Asked last, and that placement is the rule rather than an accident. Every check above names
        // something the runtime actually cannot do — this node is fed by two streams and a mapping cannot
        // join them, this junction is wired at a port its stage does not declare, this shape cannot stand
        // where the document puts it — and those sentences are sharper than "the two planes disagree", so
        // they keep speaking first for every mismatch whose shapes differ enough to be told apart.
        // What reaches here is the residue: two stages with the same ports, the same place, and the same
        // payload contract, which nothing structural could ever separate. Reading over the document's own
        // node order rather than over a dictionary keeps a document with two of these refused by the same
        // one every time.
        foreach (StageNode declaration in document.Nodes)
        {
            if (bindings.TryGetValue(declaration.Id, out LocalStageDescriptor? bound) &&
                bound.Stage != declaration.Stage)
            {
                throw Foreign(
                    $"the node '{declaration.Id}' is an occurrence of the stage '{declaration.Stage}' and is bound to the behavior of '{bound.Stage}', so its document and its binding table describe two different nodes; the binding is what a run would execute, which would make the document — and the fingerprint taken over it — a description of a graph nobody ran");
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

        return new LocalRunPlan(
            segments,
            boundaries,
            producers,
            endings,
            declared,
            completesAtStart,
            feedbackChannels,
            clock);

        // Compiles one maximal junction-free chain, from the node that begins it to the terminal or the
        // junction that ends it. A head branch begins at a source and reads no channel; every other branch
        // begins on a leg or below a junction and reads the channel that boundary is.
        void Branch(NodeId start, int input, PortId entry)
        {
            LocalSource? elements = null;
            LocalAsyncStage? asynchronous = null;
            LocalMergeMapStage? merging = null;
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
                    ProvidedStage built = Provided(declaration);
                    StageRuntime runtime = built.Runtime;

                    switch (runtime.Shape)
                    {
                        case StageRuntimeShape.Source when first && !last:
                        {
                            StageSourceOpener open = runtime.Opener!;

                            elements = context => LocalSequence.Async(
                                _ => new LocalAsyncCursor<object?>(
                                    open(new StageRunTokens(runIdentity, context.RunToken, context.StopToken))
                                        .GetAsyncEnumerator(context.RunToken)),
                                context);

                            break;
                        }

                        case StageRuntimeShape.Element when !first && !last:
                            Fuse(LocalElementStage.Select(runtime.Map!));
                            break;
                        case StageRuntimeShape.ElementAsync when !first && !last:
                            Open(pending ?? LocalBoundary.Handoff);
                            asynchronous = new LocalAsyncStage(
                                runtime.AsAsyncCallback(),
                                runtime.MaxConcurrency,
                                runtime.Ordered);
                            break;
                        case StageRuntimeShape.Terminal when last && !first:
                            Settle();
                            terminal = LocalTerminal.Provided(runtime.Fold!, runtime.Finish);
                            seedFactory = runtime.Seed;
                            produces = runtime.ProducesResult;
                            break;

                        // A registered junction is planned exactly as a local one, and its legs are its own
                        // specification's output ports in the catalog's canonical order rather than the
                        // local vocabulary's 'out-n'. That is the whole of what a provider had to be given
                        // to register a junction: the pump, the bounds, and the completion rules are the
                        // engine's and are the same ones a local broadcast runs under.
                        case StageRuntimeShape.FanOut when !first && !last:
                            Split(declaration, built.Specification.OutputPorts, runtime.Splitting!);
                            return;
                        default:
                            throw Foreign(
                                $"the node '{declaration.Id}' is an occurrence of the stage '{declaration.Stage}', whose runtime factory built a '{runtime.Shape}' shape, and that shape cannot stand at position {position} of {document.Nodes.Count}");
                    }
                }

                // Asked before the switch rather than as thirteen arms of it, because these are the shapes a
                // keyed stage also has to build — one instance of each per key — and one factory read by
                // both is what keeps the two builds from drifting. A shape that answers here but stands
                // where it cannot falls through to the switch, which has no arm for it and reports the
                // position exactly as it did when the arms were there.
                else if (!first && !last &&
                    Fusible(
                        descriptor.Kind,
                        declaration.Parameters,
                        descriptor.Behavior,
                        descriptor.Seed,
                        $"the node '{declaration.Id}'") is { } fusible)
                {
                    Fuse(fusible());
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

                        case LocalStageKind.Tick when first && !last:
                        {
                            (TimeSpan initialDelay, TimeSpan interval) = Ticking(declaration);

                            elements = context => LocalSequence.Ticks(initialDelay, interval, context);

                            break;
                        }

                        case LocalStageKind.GroupBy when !first && !last:
                            Fuse(Keyed(declaration, descriptor));
                            break;
                        case LocalStageKind.MergeMap when !first && !last:
                            // A boundary of its own, like an asynchronous stage and for a stronger version
                            // of the same reason: this loop sleeps on one outstanding step per open inner
                            // sequence, and no pass of somebody else's loop could ever take that wait. A
                            // buffer written in front of it is its input channel rather than a second one,
                            // which is the rule every boundary of this vocabulary follows.
                            Open(pending ?? LocalBoundary.Handoff);
                            merging = Merging(declaration, descriptor);
                            break;
                        case LocalStageKind.ScanAsync when !first && !last:
                            Fuse(LocalAttachedStages.ScanAsync(
                                descriptor.Seed,
                                LocalDelegateAdapter.AsyncFolder(descriptor.Behavior, descriptor.Kind)));
                            break;
                        case LocalStageKind.GroupedWithin when !first && !last:
                        {
                            (int maxElements, TimeSpan window) = Batching(declaration);

                            // A boundary of its own, like an asynchronous stage and unlike every other stage
                            // that reads a clock. This one has to emit while nothing is arriving, and the
                            // only segment a timer can wake is one asleep on its own input channel: fused
                            // into a source's loop it would sit behind whatever the source is doing, and a
                            // window that closed would wait for the next element to notice it, which is the
                            // one case this operator exists for.
                            Open(pending ?? LocalBoundary.Handoff);
                            Fuse(LocalAttachedStages.GroupedWithin(
                                maxElements,
                                maxWeight: 0,
                                window,
                                cost: null,
                                LocalDelegateAdapter.Freeze(descriptor.Behavior, descriptor.Kind)));

                            break;
                        }

                        case LocalStageKind.GroupedWeightedWithin when !first && !last:
                        {
                            (int maxElements, int maxWeight, TimeSpan window) = Weighing(declaration);
                            (Func<object?, int> weight, Func<object?, object?> freeze) =
                                LocalDelegateAdapter.Weighted(descriptor.Behavior);

                            Open(pending ?? LocalBoundary.Handoff);
                            Fuse(LocalAttachedStages.GroupedWithin(maxElements, maxWeight, window, weight, freeze));

                            break;
                        }

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
                        case LocalStageKind.Delay when !first && !last:
                        {
                            (TimeSpan shift, BufferOptions holdback) = Delaying(declaration);

                            // The declared capacity is the window — how many elements are waiting out their
                            // delay at once — and not the channel in front of it, which is the ordinary
                            // handoff every asynchronous stage gets. Giving the channel the declared capacity
                            // too would hold twice what the author wrote down. What the channel does carry is
                            // the declared policy, because that is the moment the policy is about: an element
                            // arriving when the window is full and the handoff occupied is the element a
                            // holdback has to answer for. A buffer the author placed in front of the delay is
                            // that channel instead, exactly as it is for any asynchronous stage, and its
                            // capacity adds to the window the way two declared boundaries always add.
                            Open(pending ?? new LocalBoundary(capacity: 1, holdback.OverflowPolicy));
                            asynchronous = new LocalAsyncStage(
                                Holding(shift, clock),
                                holdback.Capacity,
                                ordered: true);

                            break;
                        }

                        case LocalStageKind.InitialDelay when !first && !last:
                            Fuse(LocalAttachedStages.InitialDelay(Duration(declaration)));
                            break;
                        case LocalStageKind.Timeout when !first && !last:
                            Fuse(LocalAttachedStages.Timeout(Duration(declaration)));
                            break;
                        case LocalStageKind.TakeWithin when !first && !last:
                            Fuse(LocalAttachedStages.TakeWithin(Duration(declaration)));
                            break;
                        case LocalStageKind.SkipWithin when !first && !last:
                            Fuse(LocalAttachedStages.SkipWithin(Duration(declaration)));
                            break;
                        case LocalStageKind.Throttle when !first && !last:
                        {
                            ThrottleOptions rate = Throttling(declaration);

                            Fuse(LocalAttachedStages.Throttle(
                                rate.Elements,
                                rate.Per,
                                rate.MaximumBurst!.Value,
                                rate.Mode is ThrottleMode.Enforcing,
                                descriptor.Behavior is null
                                    ? null
                                    : LocalDelegateAdapter.Cost(descriptor.Behavior)));

                            break;
                        }

                        case LocalStageKind.Valve when !first && !last:
                        {
                            LocalValve valve = new(Valving(declaration));

                            controls.Add(current, (null, valve));
                            Fuse(LocalAttachedStages.Valve(valve));

                            break;
                        }

                        case LocalStageKind.Broadcast when !first && !last:
                            Split(declaration, LocalVocabulary.OutputPortsOf(descriptor.Kind), LocalFanOut.Broadcast());
                            return;
                        case LocalStageKind.Balance when !first && !last:
                            Split(declaration, LocalVocabulary.OutputPortsOf(descriptor.Kind), LocalFanOut.Balance());
                            return;
                        case LocalStageKind.Partition when !first && !last:
                            Split(
                                declaration,
                                LocalVocabulary.OutputPortsOf(descriptor.Kind),
                                LocalFanOut.Partition(LocalDelegateAdapter.Router(descriptor.Behavior)));
                            return;
                        case LocalStageKind.Unzip when !first && !last:
                            Split(
                                declaration,
                                LocalVocabulary.OutputPortsOf(descriptor.Kind),
                                LocalFanOut.Unzip(LocalDelegateAdapter.Halves(descriptor.Behavior)));
                            return;
                        case LocalStageKind.Fold when last:
                            Settle();
                            terminal = LocalTerminal.Folding(
                                LocalDelegateAdapter.Folder(descriptor.Behavior, descriptor.Kind));
                            seed = descriptor.Seed;
                            produces = true;
                            break;
                        case LocalStageKind.FoldAsync when last:
                            Settle();
                            terminal = LocalTerminal.FoldingAsync(
                                LocalDelegateAdapter.AsyncFolder(descriptor.Behavior, descriptor.Kind));
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
                                LocalDelegateAdapter.Freeze(descriptor.Behavior, descriptor.Kind));
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
                        merging,
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
                    merging,
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
                merging = null;
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
                if (elements is not null ||
                    asynchronous is not null ||
                    merging is not null ||
                    stages.Count > 0 ||
                    pending is not null)
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
            void Split(StageNode node, IReadOnlyList<OutputPortSpecification> ports, LocalFanOut junction)
            {
                Open(pending ?? LocalBoundary.Handoff);

                int segment = segments.Count;
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
                    mergeMap: null,
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

            // Ends this branch at a joining junction, and builds that junction when the last branch that
            // can arrive at it before it exists has arrived. Everything under construction is closed at a
            // boundary first, exactly as it is at a splitting junction; the channel that closing produced
            // is the input this branch feeds, and which input that is is the port the branch arrived at.
            //
            // A cycle is the whole reason "the last arrival" is not simply "every arrival". The branch
            // that carries a feedback edge begins below this very junction, so it cannot be walked until
            // the junction exists, and the junction cannot wait for it: the two would wait for each other
            // and the plan would never be built. So a feedback input is a place kept rather than a channel
            // waited for — the junction is built when every input that comes from outside the cycle has
            // arrived, with its feedback inputs holding a reserved slot, and the arrival that eventually
            // comes round fills that slot in the list the segment is already reading. Which inputs those
            // are is decided before the walk begins, by the back edges of a walk rooted at the heads.
            void Meet(StageNode node, PortId entry)
            {
                Open(pending ?? LocalBoundary.Handoff);

                if (inputs.Count == 0)
                {
                    throw Foreign(
                        $"the junction '{node.Id}' is fed by nothing at the port '{entry}', and a junction joins at least {LocalVocabulary.MinFanIn} inputs");
                }

                // The ports of a junction come from whichever plane declares its behavior: the local
                // vocabulary's fixed 'in-n' for a bound one, and the specification's own input ports, in the
                // catalog's canonical order, for a registered one.
                bool bound = bindings.TryGetValue(node.Id, out LocalStageDescriptor? joining);
                IReadOnlyList<InputPortSpecification> ports = bound
                    ? LocalVocabulary.InputPortsOf(joining!.Kind)
                    : Provided(node).Specification.InputPorts;
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

                if (!joined.TryGetValue(node.Id, out Arrivals? arrived))
                {
                    arrived = new Arrivals(ports.Count);

                    for (int input = 0; input < ports.Count; input++)
                    {
                        arrived.Feedback[input] =
                            feedback.Contains(PortAddress.Create(node.Id, ports[input].Id));
                    }

                    joined.Add(node.Id, arrived);
                }

                if (arrived.Channels[arrival] >= 0)
                {
                    throw Foreign($"the junction '{node.Id}' is reached at the port '{entry}' from more than one place");
                }

                arrived.Channels[arrival] = inputs[0];

                if (arrived.Streams is { } opened)
                {
                    // The arrival that closed a cycle. The junction has been reading this list since it
                    // was built, and this is the channel the slot was kept for; the run needs the channel
                    // again by itself, because cutting a feedback edge is how a graceful stop ends a loop.
                    // A slot is always there — the junction is built only once every wired input has
                    // either arrived or been marked as feedback — and the guard is here so that the
                    // argument stays an argument rather than an index nobody checked.
                    if (arrived.Slots[arrival] < 0)
                    {
                        throw Foreign(
                            $"the junction '{node.Id}' was built before the port '{entry}' was known to be one of its inputs");
                    }

                    opened[arrived.Slots[arrival]] = inputs[0];
                    feedbackChannels.Add(inputs[0]);

                    return;
                }

                int joins = 0;

                for (int input = 0; input < ports.Count; input++)
                {
                    if (arrived.Channels[input] >= 0 || arrived.Feedback[input])
                    {
                        joins++;
                    }
                }

                if (joins != arriving[node.Id].Count)
                {
                    return;
                }

                List<int> streams = [];

                for (int input = 0; input < ports.Count; input++)
                {
                    if (arrived.Channels[input] < 0 && !arrived.Feedback[input])
                    {
                        continue;
                    }

                    arrived.Slots[input] = streams.Count;
                    streams.Add(arrived.Channels[input]);
                }

                arrived.Streams = streams;

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
                    mergeMap: null,
                    fanOut: null,
                    bound ? Joining(node, joining!) : Provided(node).Runtime.Joining!,
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

            if (!bindings.TryGetValue(target, out LocalStageDescriptor? below))
            {
                return (LocalBoundary.Handoff, target, onwards.To.Port);
            }

            // A delay standing here is not skipped — it is a stage and not a channel — but the channel it
            // reads is the one this junction writes, so the policy it declared has to be that channel's or
            // it would silently not apply. Its capacity is its window rather than this channel's, which is
            // the same split a delay makes anywhere else.
            if (below.Kind is LocalStageKind.Delay)
            {
                (TimeSpan _, BufferOptions holdback) = Delaying(declarations[target]);

                return (new LocalBoundary(capacity: 1, holdback.OverflowPolicy), target, onwards.To.Port);
            }

            if (below.Kind is not LocalStageKind.Buffer ||
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
            bindings.TryGetValue(node, out LocalStageDescriptor? descriptor)
                ? LocalVocabulary.PlaceOf(descriptor.Kind) is LocalStagePlace.FanIn
                : Resolved(node)?.Runtime.Shape is StageRuntimeShape.FanIn;

        // Resolves a node through the seam without refusing anything, for the questions asked before the
        // walk begins. A node that does not resolve is not "not a junction" — it is a node the walk will
        // refuse by name, with the binder's own account of which of the two lookups failed — so answering
        // here would replace that diagnostic with a vaguer one.
        ProvidedStage? Resolved(NodeId node)
        {
            if (provided.TryGetValue(node, out ProvidedStage? cached))
            {
                return cached;
            }

            if (bindings.ContainsKey(node) ||
                !binder.TryCreate(
                    declarations[node],
                    out StageRuntime? runtime,
                    out StageSpecification? specification,
                    out _))
            {
                return null;
            }

            ProvidedStage built = new(runtime, specification);

            provided.Add(node, built);

            return built;
        }

        // Resolves a node through the seam, refusing the plan when it does not resolve. Memoized on the
        // node, because the seam's contract is that a factory is asked once per node per materialization:
        // a junction is asked about before the walk and again while it is planned, and two calls would be
        // two stage runtimes with two lots of per-run state.
        ProvidedStage Provided(StageNode node)
        {
            if (provided.TryGetValue(node.Id, out ProvidedStage? cached))
            {
                return cached;
            }

            if (!binder.TryCreate(
                node,
                out StageRuntime? runtime,
                out StageSpecification? specification,
                out string? refusal))
            {
                throw Foreign(
                    $"the node '{node.Id}' is an occurrence of the stage '{node.Stage}', and no local behavior is bound to it; {refusal}");
            }

            ProvidedStage built = new(runtime, specification);

            provided.Add(node.Id, built);

            return built;
        }

        // Builds the strategy of one joining junction. The rotation's segment size is read from the
        // document rather than from the binding, for the reason every number is: what the catalog validates
        // has to be exactly what the runtime executes. The combiner of a row-building junction comes the
        // other way, from the binding, for the reason every projection does: it is a statement about element
        // types, and a document never names one.
        LocalFanIn Joining(StageNode node, LocalStageDescriptor descriptor) => descriptor.Kind switch
        {
            LocalStageKind.Merge => LocalFanIn.Merge(),
            LocalStageKind.Concat => LocalFanIn.Concat(),
            LocalStageKind.Zip => LocalFanIn.Zip(
                LocalDelegateAdapter.Combiner(descriptor.Behavior, descriptor.Kind)),
            LocalStageKind.CombineLatest => LocalFanIn.CombineLatest(
                LocalDelegateAdapter.Combiner(descriptor.Behavior, descriptor.Kind)),
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

    /// <summary>Finds one cycle among the nodes a filter admits, and reports the nodes it runs through.</summary>
    /// <param name="document">The document being compiled.</param>
    /// <param name="leaving">The edges leaving each node that has any.</param>
    /// <param name="included">The nodes to walk over; every other node is treated as absent.</param>
    /// <returns>The cycle's nodes in flow order, or <see langword="null"/> when there is no cycle.</returns>
    /// <remarks>
    /// <para>
    /// One depth-first walk from every admitted node, reporting the first back edge it meets. Both of this
    /// runtime's cycle rules are this one search over two different filters, which is the point of writing
    /// it once: ADR 0005's legality rule is "is there a cycle once the boundaries that can answer without
    /// room are removed", and the entry rule is "is there a cycle among the nodes no source reaches". A
    /// filter is exactly how a node is removed from a graph without building a second one.
    /// </para>
    /// <para>
    /// Removing a node rather than an edge is what makes the legality rule correct rather than
    /// approximately correct. "Every cycle passes at least one relieving boundary" is not the same claim as
    /// "some cycle through this component passes one", and enumerating cycles to tell them apart is
    /// exponential; deleting the relieving nodes and asking whether any cycle survives is the same claim,
    /// answered in one walk. A self-loop is a cycle of one node and needs no separate rule, which is
    /// exactly what ADR 0005 says when it subsumes M0's refusal.
    /// </para>
    /// <para>
    /// The walk is iterative because a graph's depth is not this process's stack to spend, and the path is
    /// kept as a list beside the stack so that the answer is the cycle itself rather than the fact that
    /// there is one: a diagnostic that names <c>a -&gt; b -&gt; a</c> is actionable and one that says "a
    /// cycle exists" is not.
    /// </para>
    /// </remarks>
    private static List<NodeId>? Cycle(
        GraphDocument document,
        Dictionary<NodeId, List<GraphEdge>> leaving,
        Func<NodeId, bool> included)
    {
        HashSet<NodeId> finished = [];
        Dictionary<NodeId, int> depth = [];
        List<NodeId> path = [];
        Stack<(NodeId Node, int Next)> pending = new();

        foreach (StageNode start in document.Nodes)
        {
            if (!included(start.Id) || finished.Contains(start.Id))
            {
                continue;
            }

            pending.Push((start.Id, 0));
            depth.Add(start.Id, path.Count);
            path.Add(start.Id);

            while (pending.Count > 0)
            {
                (NodeId node, int next) = pending.Pop();

                if (!leaving.TryGetValue(node, out List<GraphEdge>? onwards) || next == onwards.Count)
                {
                    _ = depth.Remove(node);
                    path.RemoveAt(path.Count - 1);
                    _ = finished.Add(node);

                    continue;
                }

                pending.Push((node, next + 1));

                NodeId target = onwards[next].To.Node;

                if (!included(target) || finished.Contains(target))
                {
                    continue;
                }

                if (depth.TryGetValue(target, out int entered))
                {
                    return path.GetRange(entered, path.Count - entered);
                }

                depth.Add(target, path.Count);
                path.Add(target);
                pending.Push((target, 0));
            }
        }

        return null;
    }

    /// <summary>Finds the nodes a walk from the heads reaches by following the edges forward.</summary>
    /// <param name="leaving">The edges leaving each node that has any.</param>
    /// <param name="heads">The nodes nothing feeds.</param>
    /// <returns>The reachable nodes.</returns>
    /// <remarks>
    /// Deliberately one-directional, unlike <see cref="Separated"/>: what this answers is whether an
    /// element could ever arrive at a node, and elements travel one way. A cycle among the nodes this does
    /// not reach is a cycle no source feeds.
    /// </remarks>
    private static HashSet<NodeId> Reachable(Dictionary<NodeId, List<GraphEdge>> leaving, List<NodeId> heads)
    {
        HashSet<NodeId> reached = [.. heads];
        Queue<NodeId> pending = new(heads);

        while (pending.Count > 0)
        {
            if (!leaving.TryGetValue(pending.Dequeue(), out List<GraphEdge>? onwards))
            {
                continue;
            }

            for (int index = 0; index < onwards.Count; index++)
            {
                if (reached.Add(onwards[index].To.Node))
                {
                    pending.Enqueue(onwards[index].To.Node);
                }
            }
        }

        return reached;
    }

    /// <summary>Finds the input ports a cycle closes at, as the back edges of a walk from the heads.</summary>
    /// <param name="leaving">The edges leaving each node that has any.</param>
    /// <param name="heads">The nodes nothing feeds.</param>
    /// <returns>The addresses of the input ports those edges terminate at.</returns>
    /// <remarks>
    /// <para>
    /// Cutting these edges is what turns a cyclic document back into the acyclic one the planner's walk
    /// knows how to compile, and every cycle contains at least one of them by construction: a depth-first
    /// walk meets a node of the current path again exactly when the edge it followed closes a cycle. Which
    /// edge of a given cycle is chosen depends on the order the edges are read in and does not matter — any
    /// one of them cut leaves a graph the walk can finish.
    /// </para>
    /// <para>
    /// The answer is ports rather than edges because a port is what the plan needs: an input of a junction
    /// is named by the port an edge terminates at and by nothing else, so a reserved slot is a port's, and
    /// a buffer folded into the leg above it changes which edge arrives without changing which port it
    /// arrives at.
    /// </para>
    /// </remarks>
    private static HashSet<PortAddress> Feedback(Dictionary<NodeId, List<GraphEdge>> leaving, List<NodeId> heads)
    {
        HashSet<PortAddress> feedback = [];
        HashSet<NodeId> open = [];
        HashSet<NodeId> finished = [];
        Stack<(NodeId Node, int Next)> pending = new();

        for (int index = 0; index < heads.Count; index++)
        {
            if (finished.Contains(heads[index]))
            {
                continue;
            }

            pending.Push((heads[index], 0));
            _ = open.Add(heads[index]);

            while (pending.Count > 0)
            {
                (NodeId node, int next) = pending.Pop();

                if (!leaving.TryGetValue(node, out List<GraphEdge>? onwards) || next == onwards.Count)
                {
                    _ = open.Remove(node);
                    _ = finished.Add(node);

                    continue;
                }

                pending.Push((node, next + 1));

                NodeId target = onwards[next].To.Node;

                if (open.Contains(target))
                {
                    _ = feedback.Add(onwards[next].To);

                    continue;
                }

                if (finished.Contains(target))
                {
                    continue;
                }

                _ = open.Add(target);
                pending.Push((target, 0));
            }
        }

        return feedback;
    }

    /// <summary>Finds the nodes that are boundaries able to answer an offer without room below them.</summary>
    /// <param name="document">The document being compiled.</param>
    /// <param name="bindings">The authoring-side behavior of every locally bound node.</param>
    /// <param name="declarations">The document's nodes, keyed by identifier.</param>
    /// <returns>The nodes a cycle may pass through without being a deadlock by construction.</returns>
    /// <remarks>
    /// ADR 0005's predicate, and the whole of what makes a cycle legal: a declared buffer whose overflow
    /// policy is not backpressure answers every offer — by dropping, by discarding what it held, or by
    /// failing the run — so a pump above it never waits for room that only a pump below it could make.
    /// Every other boundary this engine has waits, the implicit handoff between two segments included, so
    /// nothing else relieves a cycle. A payload this runtime cannot read is not a relieving boundary
    /// either: the walk reports that node as unreadable in its own words, and treating an unreadable
    /// capacity as a licence would be the one place a broken document bought a weaker rule. Computed once
    /// rather than asked per edge, because the answer involves reading a parameter payload and the walk
    /// that consumes it visits a node once per edge into it.
    /// </remarks>
    private static HashSet<NodeId> Relieving(
        GraphDocument document,
        IReadOnlyDictionary<NodeId, LocalStageDescriptor> bindings,
        Dictionary<NodeId, StageNode> declarations)
    {
        HashSet<NodeId> relieving = [];

        foreach (StageNode node in document.Nodes)
        {
            if (bindings.TryGetValue(node.Id, out LocalStageDescriptor? descriptor) &&
                descriptor.Kind is LocalStageKind.Buffer &&
                LocalBufferParameters.TryRead(declarations[node.Id].Parameters, out BufferOptions? options, out _) &&
                options!.OverflowPolicy is not OverflowPolicy.Backpressure)
            {
                _ = relieving.Add(node.Id);
            }
        }

        return relieving;
    }

    /// <summary>Renders a cycle's nodes as the loop they are, for a diagnostic.</summary>
    /// <param name="cycle">The nodes in flow order, beginning at the one the cycle returns to.</param>
    /// <returns>The path, closed back onto its first node.</returns>
    private static string Path(List<NodeId> cycle) =>
        string.Join(" -> ", cycle.Select(node => $"'{node}'").Append($"'{cycle[0]}'"));

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
    private static int Count(StageNode node) => Count(node.Parameters, $"the node '{node.Id}'");

    /// <summary>Reads a counted payload as the number of elements it declares.</summary>
    /// <param name="parameters">The payload.</param>
    /// <param name="what">What carries it, for the diagnostic.</param>
    /// <returns>The count.</returns>
    /// <exception cref="InvalidOperationException">The payload is not a count payload.</exception>
    /// <remarks>
    /// The payload and its subject are separate arguments because a count is carried by a node in a document
    /// and by a stage of a group flow, which is not a node and has no identifier: what a reader has to be
    /// told is where the payload was, and that is a sentence rather than a node.
    /// </remarks>
    private static int Count(CanonicalJsonValue parameters, string what) =>
        LocalCountParameters.TryRead(parameters, out int count, out IReadOnlyList<string> violations)
            ? count
            : throw Foreign($"{what} carries parameters this runtime cannot read: {string.Join("; ", violations)}");

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

    /// <summary>Reads a timed node's payload as the duration it declares.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <returns>The duration.</returns>
    /// <exception cref="InvalidOperationException">The payload is not a duration payload.</exception>
    /// <remarks>
    /// Unreachable for a document validated against the local catalog, whose timing stages run the very same
    /// reader as their parameter check. It is here because this type is also handed documents that were
    /// never validated, and a duration it could not read would otherwise become a window of some silently
    /// chosen length.
    /// </remarks>
    private static TimeSpan Duration(StageNode node) =>
        LocalDurationParameters.TryRead(node.Parameters, out TimeSpan duration, out IReadOnlyList<string> violations)
            ? duration
            : throw Foreign(
                $"the node '{node.Id}' carries parameters this runtime cannot read: {string.Join("; ", violations)}");

    /// <summary>Reads a tick node's payload as the two durations it declares.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <returns>The delay before the first tick and the interval between ticks.</returns>
    /// <exception cref="InvalidOperationException">The payload is not a tick payload.</exception>
    private static (TimeSpan InitialDelay, TimeSpan Interval) Ticking(StageNode node) =>
        LocalTickParameters.TryRead(
            node.Parameters,
            out TimeSpan initialDelay,
            out TimeSpan interval,
            out IReadOnlyList<string> violations)
            ? (initialDelay, interval)
            : throw Foreign(
                $"the tick source '{node.Id}' carries parameters this runtime cannot read: {string.Join("; ", violations)}");

    /// <summary>Reads a delay node's payload as the duration and the holdback it declares.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <returns>The delay applied to each element and the bound on how many are held at once.</returns>
    /// <exception cref="InvalidOperationException">The payload is not a delay payload.</exception>
    private static (TimeSpan Delay, BufferOptions Holdback) Delaying(StageNode node) =>
        LocalDelayParameters.TryRead(
            node.Parameters,
            out TimeSpan delay,
            out BufferOptions? holdback,
            out IReadOnlyList<string> violations)
            ? (delay, holdback!)
            : throw Foreign(
                $"the delay '{node.Id}' carries parameters this runtime cannot read: {string.Join("; ", violations)}");

    /// <summary>Reads a valve node's payload as the state it starts in.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <returns>The state the run's valve starts in.</returns>
    /// <exception cref="InvalidOperationException">The payload is not a valve payload.</exception>
    private static ValveMode Valving(StageNode node) =>
        LocalValveParameters.TryRead(node.Parameters, out ValveMode mode, out IReadOnlyList<string> violations)
            ? mode
            : throw Foreign(
                $"the valve '{node.Id}' carries parameters this runtime cannot read: {string.Join("; ", violations)}");

    /// <summary>Reads a throttle node's payload as the rate it declares.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <returns>The options, whose burst the reader has already stated rather than defaulted.</returns>
    /// <exception cref="InvalidOperationException">The payload is not a throttle payload.</exception>
    private static ThrottleOptions Throttling(StageNode node) =>
        LocalThrottleParameters.TryRead(node.Parameters, out ThrottleOptions? options, out IReadOnlyList<string> violations)
            ? options!
            : throw Foreign(
                $"the throttle '{node.Id}' carries parameters this runtime cannot read: {string.Join("; ", violations)}");

    /// <summary>Builds the callback that holds one element of a delay for its duration.</summary>
    /// <param name="delay">The duration each element is held for.</param>
    /// <param name="clock">The run's clock.</param>
    /// <returns>The callback the asynchronous driver runs per element.</returns>
    /// <remarks>
    /// <para>
    /// A delay is the one timing operator that is a window rather than a hold, and that is why it is driven
    /// by the machinery an asynchronous stage is driven by rather than fused as an element stage. The
    /// difference is what an author means by the word: an element admitted here starts its own wait at once
    /// and the results are emitted in input order, so a burst that fits the declared holdback comes out
    /// shifted by the delay with its gaps intact, while a stage that held one element at a time would have
    /// turned the same burst into a stream paced at one element per delay, which is a throttle and not a
    /// delay.
    /// </para>
    /// <para>
    /// The clock is closed over here rather than read from the run's context, because that is what the
    /// callback shape gives: the driver hands a callback its element and the run's token. Both are what a
    /// delay needs — the token is what abandons a wait when the run is cancelled — and the clock is fixed
    /// per materialization, which is exactly when this closure is built.
    /// </para>
    /// <para>
    /// The token is the run's own and not the stop token, which is the asynchronous window's rule rather
    /// than a choice made here: a graceful shutdown drains what a window already admitted, so it waits out
    /// the delays in flight exactly as it waits out an author's callbacks, and a cancellation abandons them.
    /// A pause waits for them for the same reason, which is bounded by the delay itself.
    /// </para>
    /// </remarks>
    private static Func<object?, CancellationToken, Task<object?>> Holding(TimeSpan delay, TimeProvider clock) =>
        async (element, cancellationToken) =>
        {
            await Task.Delay(delay, clock, cancellationToken).ConfigureAwait(false);

            return element;
        };

    /// <summary>Builds the factory of one fused element stage, when the shape is one of those.</summary>
    /// <param name="kind">The stage shape.</param>
    /// <param name="parameters">The payload the document states for it.</param>
    /// <param name="behavior">The delegate, comparer, or projection the binding states for it.</param>
    /// <param name="seed">The initial state, for the shapes that carry one.</param>
    /// <param name="what">What carries the two halves, for the diagnostic.</param>
    /// <returns>
    /// A factory of fresh instances, or <see langword="null"/> when the shape is not one that fuses as an
    /// element stage.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The payload is not one this shape reads, or the binding does not have the shape it requires.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The one place these shapes are built, read by the two callers that build them: a chain of a document,
    /// where each is built exactly once, and a keyed stage, which builds one of each per key. Everything
    /// that costs something — reading the payload, wrapping the author's delegate, the reflection inside
    /// that wrapping — happens here, once, when the plan is built; what the factory does per key is
    /// construct an object over values it is already holding.
    /// </para>
    /// <para>
    /// Answering <see langword="null"/> rather than throwing for every other shape is what lets the caller
    /// go on to say something sharper: a chain's switch reports where a shape cannot stand, and a keyed
    /// stage never asks at all, because its group flow was checked against
    /// <see cref="LocalVocabulary.RunsInsideAGroup"/> before anything was built.
    /// </para>
    /// </remarks>
    private static Func<LocalElementStage>? Fusible(
        LocalStageKind kind,
        CanonicalJsonValue parameters,
        object? behavior,
        object? seed,
        string what)
    {
        switch (kind)
        {
            case LocalStageKind.Select:
            {
                Func<object?, object?> selector = LocalDelegateAdapter.Selector(behavior);

                return () => LocalElementStage.Select(selector);
            }

            case LocalStageKind.Where:
            {
                Func<object?, bool> predicate = LocalDelegateAdapter.Predicate(behavior, kind);

                return () => LocalElementStage.Where(predicate);
            }

            case LocalStageKind.Scan:
            {
                Func<object?, object?, object?> folder = LocalDelegateAdapter.Folder(behavior, kind);

                return () => LocalElementStage.Scan(seed, folder);
            }

            case LocalStageKind.Take:
            {
                int count = Count(parameters, what);

                return () => LocalElementStage.Take(count);
            }

            case LocalStageKind.Skip:
            {
                int count = Count(parameters, what);

                return () => LocalElementStage.Skip(count);
            }

            case LocalStageKind.TakeWhile or LocalStageKind.TakeThrough:
            {
                Func<object?, bool> predicate = LocalDelegateAdapter.Predicate(behavior, kind);
                bool inclusive = kind is LocalStageKind.TakeThrough;

                return () => LocalElementStage.TakeWhile(predicate, inclusive);
            }

            case LocalStageKind.SkipWhile:
            {
                Func<object?, bool> predicate = LocalDelegateAdapter.Predicate(behavior, kind);

                return () => LocalElementStage.SkipWhile(predicate);
            }

            case LocalStageKind.Distinct:
            {
                DistinctOptions deduplication = Distinct(parameters, what);
                IEqualityComparer comparer = LocalDelegateAdapter.Comparer(behavior);
                bool evicting = deduplication.OverflowPolicy is KeyOverflowPolicy.EvictOldest;

                return () =>
                    LocalElementStage.Distinct(deduplication.MaxTrackedKeys, evicting, comparer);
            }

            case LocalStageKind.DeduplicateConsecutive:
            {
                IEqualityComparer comparer = LocalDelegateAdapter.Comparer(behavior);

                return () => LocalElementStage.DeduplicateConsecutive(comparer);
            }

            case LocalStageKind.SelectMany:
            {
                Func<object?, IEnumerable> flattener = LocalDelegateAdapter.Flattener(behavior);

                return () => LocalElementStage.SelectMany(flattener);
            }

            case LocalStageKind.Grouped:
            {
                int size = Count(parameters, what);
                Func<object?, object?> freeze = LocalDelegateAdapter.Freeze(behavior, kind);

                return () => LocalElementStage.Grouped(size, freeze);
            }

            case LocalStageKind.Sliding:
            {
                (int size, int step) = Windowing(parameters, what);
                Func<object?, object?> freeze = LocalDelegateAdapter.Freeze(behavior, kind);

                return () => LocalElementStage.Sliding(size, step, freeze);
            }

            default:
                return null;
        }
    }

    /// <summary>Builds a keyed stage from what the document says it is and what the binding says it does.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <param name="descriptor">The occurrence, which carries the key function, the comparer, and the flow.</param>
    /// <returns>The stage.</returns>
    /// <exception cref="InvalidOperationException">
    /// The payload is not a keyed-stage payload, the binding is not a keyed stage's triple, or the two
    /// planes disagree about what the group flow is.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The two planes have to be talking about the same group flow, and this is where they say so. The
    /// document states which stages it is made of and what each of them is configured with; the binding
    /// states what each of them does. Neither is trusted to imply the other, which is the rule this planner
    /// follows for every node — read one level down, because the stages of a group flow are not nodes and
    /// there is nothing else to check them against.
    /// </para>
    /// <para>
    /// The disagreements are two and are reported apart, because they are different mistakes: a group flow
    /// of a different length is a document and a binding built from two different graphs, and a stage of a
    /// different shape at the same position is one graph whose two halves were edited apart. Both are
    /// unreachable through the authoring API, which writes the payload from the very descriptors it binds.
    /// </para>
    /// </remarks>
    private static LocalElementStage Keyed(StageNode node, LocalStageDescriptor descriptor)
    {
        if (!LocalGroupByParameters.TryRead(
            node.Parameters,
            out GroupByOptions? options,
            out IReadOnlyList<LocalGroupStage> declared,
            out IReadOnlyList<string> violations))
        {
            throw Foreign(
                $"the keyed stage '{node.Id}' carries parameters this runtime cannot read: {string.Join("; ", violations)}");
        }

        (Func<object?, object?> key, IEqualityComparer comparer, IReadOnlyList<LocalStageDescriptor> bound) =
            LocalDelegateAdapter.Keyed(descriptor.Behavior);

        if (bound.Count != declared.Count)
        {
            throw Foreign(
                $"the keyed stage '{node.Id}' declares a group flow of {declared.Count} stages and is bound to one of {bound.Count}");
        }

        Func<LocalElementStage>[] group = new Func<LocalElementStage>[declared.Count];

        for (int stage = 0; stage < group.Length; stage++)
        {
            string what = string.Create(
                CultureInfo.InvariantCulture,
                $"stage {stage + 1} of the group flow of the keyed stage '{node.Id}'");

            if (bound[stage].Kind != declared[stage].Kind)
            {
                throw Foreign(
                    $"{what} is declared as '{LocalVocabulary.StageOf(declared[stage].Kind)}' and bound as '{bound[stage].Stage}'");
            }

            group[stage] =
                Fusible(
                    declared[stage].Kind,
                    declared[stage].Parameters,
                    bound[stage].Behavior,
                    bound[stage].Seed,
                    what) ??
                throw Foreign(
                    $"{what} is an occurrence of the stage '{bound[stage].Stage}', and a group flow runs fused per key, so it holds element stages only");
        }

        return LocalElementStage.GroupBy(
            options!.MaxActiveKeys,
            options.OverflowPolicy is ActiveKeyOverflowPolicy.EvictIdle,
            key,
            comparer,
            group);
    }

    /// <summary>Reads a distinct node's payload as the key bound it declares.</summary>
    /// <param name="parameters">The payload.</param>
    /// <param name="what">What carries it, for the diagnostic.</param>
    /// <returns>The greatest number of keys the stage may remember.</returns>
    /// <exception cref="InvalidOperationException">The payload is not a distinct payload.</exception>
    private static DistinctOptions Distinct(CanonicalJsonValue parameters, string what) =>
        LocalDistinctParameters.TryRead(parameters, out DistinctOptions? options, out IReadOnlyList<string> violations)
            ? options!
            : throw Foreign(
                $"{what} carries parameters this runtime cannot read: {string.Join("; ", violations)}");

    /// <summary>Reads a sliding window's payload as the size and step it declares.</summary>
    /// <param name="parameters">The payload.</param>
    /// <param name="what">What carries it, for the diagnostic.</param>
    /// <returns>How many elements a window carries and how far it advances.</returns>
    /// <exception cref="InvalidOperationException">The payload is not a sliding-window payload.</exception>
    private static (int Size, int Step) Windowing(CanonicalJsonValue parameters, string what) =>
        LocalWindowParameters.TryRead(parameters, out int size, out int step, out IReadOnlyList<string> violations)
            ? (size, step)
            : throw Foreign(
                $"{what} carries parameters this runtime cannot read: {string.Join("; ", violations)}");

    /// <summary>Reads a batch's payload as the element bound and the window it declares.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <returns>How many elements close a group and how long one stays open.</returns>
    /// <exception cref="InvalidOperationException">The payload is not a grouped-within payload.</exception>
    private static (int MaxElements, TimeSpan Window) Batching(StageNode node) =>
        LocalGroupedWithinParameters.TryRead(
            node.Parameters,
            out int maxElements,
            out TimeSpan window,
            out IReadOnlyList<string> violations)
            ? (maxElements, window)
            : throw Foreign(
                $"the grouped-within stage '{node.Id}' carries parameters this runtime cannot read: {string.Join("; ", violations)}");

    /// <summary>Reads a weighted batch's payload as the three bounds it declares.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <returns>How many elements, how much weight, and how long a group stays open.</returns>
    /// <exception cref="InvalidOperationException">The payload is not a weighted-batch payload.</exception>
    private static (int MaxElements, int MaxWeight, TimeSpan Window) Weighing(StageNode node) =>
        LocalGroupedWeightedParameters.TryRead(
            node.Parameters,
            out int maxElements,
            out int maxWeight,
            out TimeSpan window,
            out IReadOnlyList<string> violations)
            ? (maxElements, maxWeight, window)
            : throw Foreign(
                $"the weighted batch '{node.Id}' carries parameters this runtime cannot read: {string.Join("; ", violations)}");

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

    /// <summary>Reads a merge-map node's payload and binding as the stage that heads a segment.</summary>
    /// <param name="node">The node as the document declares it.</param>
    /// <param name="descriptor">The occurrence, which carries the kind and the bound function.</param>
    /// <returns>The stage.</returns>
    /// <exception cref="InvalidOperationException">
    /// The payload is not a parallelism payload, or the binding is not a function answering a sequence.
    /// </exception>
    /// <remarks>
    /// The same split the asynchronous stages make and the same payload: how many inner sequences are open
    /// at once is configuration a document states, and which of the two function shapes the author wrote is
    /// behavior the binding carries. Both shapes are resolved into one opener here and nowhere else, so the
    /// pump above promises what it promises once.
    /// </remarks>
    private static LocalMergeMapStage Merging(StageNode node, LocalStageDescriptor descriptor)
    {
        if (!LocalParallelismParameters.TryRead(
            node.Parameters,
            out ParallelismOptions? options,
            out IReadOnlyList<string> violations))
        {
            throw Foreign(
                $"the merge-map stage '{node.Id}' carries parameters this runtime cannot read: {string.Join("; ", violations)}");
        }

        return new LocalMergeMapStage(LocalDelegateAdapter.Inner(descriptor.Behavior), options!.MaxConcurrency);
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

    /// <summary>One node resolved through the runtime-factory seam, as this compilation remembers it.</summary>
    /// <param name="Runtime">The executable form the provider's factory built.</param>
    /// <param name="Specification">The specification the node's stage resolved to in the host's catalog.</param>
    /// <remarks>
    /// The specification travels beside the runtime because a junction needs both and they come from
    /// different places: what the junction <em>does</em> is the factory's answer, and which ports it is
    /// wired at is the catalog's. Keeping the catalog's answer is what stops a provider's factory from
    /// deciding its own stage's shape, and it is why a registered fan-out's leg order is the specification's
    /// canonical port order rather than anything a factory chose.
    /// </remarks>
    private sealed record class ProvidedStage(StageRuntime Runtime, StageSpecification Specification);

    /// <summary>What one joining junction under construction has been told about its inputs so far.</summary>
    /// <remarks>
    /// Four parallel arrays over the junction's declared input ports, and they exist as one type because a
    /// cycle makes "which branches have arrived" and "which segment is already reading them" two questions
    /// about the same junction at the same time. Without a cycle only the first two would be needed and a
    /// bare array of channels was enough, which is what this replaced.
    /// </remarks>
    /// <param name="ports">How many input ports the junction's stage declares.</param>
    private sealed class Arrivals(int ports)
    {
        /// <summary>Gets the channel each input port was reached on, or minus one when none has.</summary>
        internal int[] Channels { get; } = Filled(ports);

        /// <summary>Gets which input ports a feedback edge terminates at.</summary>
        /// <value>
        /// <see langword="true"/> for a port whose branch begins below this junction, so that waiting for
        /// it before building the junction would be waiting for the junction itself.
        /// </value>
        internal bool[] Feedback { get; } = new bool[ports];

        /// <summary>Gets each input port's position in the junction's list of channels, or minus one.</summary>
        /// <remarks>
        /// The list holds one entry per wired input and a port list holds every port the stage declares,
        /// so the two are not the same index; this is the map between them, and it is what a feedback
        /// arrival writes through when it fills the slot that was kept for it.
        /// </remarks>
        internal int[] Slots { get; } = Filled(ports);

        /// <summary>Gets or sets the very list of channels the built segment reads.</summary>
        /// <value>
        /// The junction's inputs once it has been built, still holding minus one at any slot a feedback
        /// edge has not filled yet; <see langword="null"/> before it is built.
        /// </value>
        /// <remarks>
        /// Held rather than copied, deliberately: the segment is constructed with this list and a feedback
        /// arrival writes into it afterwards, which is how a channel that cannot exist when the junction
        /// is built still becomes one of its inputs. Nothing reads a plan until it is finished, so the
        /// mutation is invisible to everything but this compilation.
        /// </remarks>
        internal List<int>? Streams { get; set; }

        /// <summary>Builds an array of the given length filled with minus one.</summary>
        /// <param name="length">The number of ports.</param>
        /// <returns>The array.</returns>
        private static int[] Filled(int length)
        {
            int[] values = new int[length];

            Array.Fill(values, -1);

            return values;
        }
    }
}
