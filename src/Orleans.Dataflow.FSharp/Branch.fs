namespace Orleans.Dataflow.FSharp

open Orleans.Dataflow.Authoring

// Orleans.Dataflow itself is deliberately not opened: see the note in Source.fs.

/// <summary>Ends a flow in a terminal, making one leg of a junction of it.</summary>
/// <remarks>
/// <para>
/// A branch is built exactly as a graph is closed, and by functions of the same two names for that reason:
/// <c>toSink</c> takes a terminal that declares no result and <c>toResult</c> takes one that does, naming the
/// slot its result resolves under. What differs is only the answer — a leg rather than a closed graph —
/// because a leg is not a graph until a junction call takes it.
/// </para>
/// <para>
/// The flow is the last argument, as every stream-carrying value in this package is, so a leg reads under
/// <c>|&gt;</c> in the direction the elements travel: <c>Flow.filter isLate |&gt; Branch.toSink Sink.ignore</c>.
/// <see cref="P:Orleans.Dataflow.FSharp.Flow.identity"/> is the anchor that fixes the element type of a leg
/// that transforms nothing, and it contributes no occurrence, so such a leg is its terminal and nothing else.
/// </para>
/// <para>
/// There is no junction call on a branch and there never will be: everything an author does with one is done
/// by the call that consumes it, and operators here would invite a second, mirror-image way to write the very
/// same graph.
/// </para>
/// </remarks>
[<RequireQualifiedAccess>]
module Branch =

    /// <summary>Ends a flow in a terminal that declares no result.</summary>
    /// <param name="sink">The terminal consuming what the flow produces.</param>
    /// <param name="flow">The transformation the leg's elements go through, which is unchanged.</param>
    /// <returns>The branch, ready to be handed to a junction call.</returns>
    /// <remarks>
    /// A branch built this way declares nothing, so it is reusable without limit: two junction calls over it
    /// build two graphs, each with its own occurrences of the leg's stages.
    /// </remarks>
    let toSink (sink: Sink<'Out>) (flow: Flow<'In, 'Out>) : Branch<'In> =
        Branch<'In>(LocalStageChain.Concat(flow.Stages, sink.Stages), ValueNone)

    /// <summary>Ends a flow in a result-bearing terminal, naming the slot the result resolves under.</summary>
    /// <param name="slotName">The author-stable name the run handle resolves the result by.</param>
    /// <param name="sink">The terminal folding the leg's elements into the result.</param>
    /// <param name="flow">The transformation the leg's elements go through, which is unchanged.</param>
    /// <returns>The branch and the slot that resolves its result.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="slotName"/> is not a valid single-segment identifier.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The slot exists before any graph does — a leg is written before the junction call that consumes it —
    /// so it names its graph only from that call onwards, and reading it before then says so rather than
    /// answering for a graph that does not exist. That gap is the one thing a branch's slot has that a
    /// chain's does not, and it is why this is the one closing call whose slot is not born knowing its
    /// fingerprint.
    /// </para>
    /// <para>
    /// A branch that declares a result therefore closes exactly one graph: handing it to a second junction
    /// call is refused with a diagnostic instead of quietly repointing the first graph's slot. Build a second
    /// branch for a second graph, which is one more call and gives the second result a name of its own.
    /// </para>
    /// </remarks>
    let toResult
        (slotName: string)
        (sink: SinkWithResult<'Out, 'Result>)
        (flow: Flow<'In, 'Out>)
        : Branch<'In> * Orleans.Dataflow.ResultSlot<'Result> =
        let slotId = Bindings.slotId (nameof slotName) slotName
        let binding = BranchSlotBinding()
        let branch = Branch<'In>(LocalStageChain.Concat(flow.Stages, sink.Stages), ValueSome(slotId, binding))

        branch, Orleans.Dataflow.ResultSlot<'Result>.OnBranch(slotId, binding)

    /// <summary>Ends a flow in one named occurrence of a registered terminal that declares no result.</summary>
    /// <param name="stage">The typed handle of the registered stage terminating the leg.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <param name="flow">The transformation the leg's elements go through, which is unchanged.</param>
    /// <returns>The branch, ready to be handed to a junction call.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// The deployable leg. A registered fan-out whose legs end in lambda terminals closes a graph that still
    /// declares <c>nondeployable</c>, so this is the call that makes a branching pipeline a pipeline: every
    /// occurrence of it is named and resolves from a catalog.
    /// </remarks>
    let toRegistered
        (stage: Orleans.Dataflow.RegisteredSink<'Out>)
        (occurrenceName: string)
        (parameters: Orleans.Dataflow.Serialization.CanonicalJsonValue)
        (flow: Flow<'In, 'Out>)
        : Branch<'In> =
        Branch<'In>(
            LocalStageChain.Append(
                flow.Stages,
                RegisteredAttachment.Occurrence(stage.Specification, occurrenceName, parameters)),
            ValueNone)

    /// <summary>Ends a flow in one named occurrence of a registered result-bearing terminal.</summary>
    /// <param name="slotName">The author-stable name the run handle resolves the result by.</param>
    /// <param name="stage">The typed handle of the registered stage terminating the leg.</param>
    /// <param name="occurrenceName">The author-stable name of this occurrence.</param>
    /// <param name="parameters">The configuration this occurrence carries, in canonical form.</param>
    /// <param name="flow">The transformation the leg's elements go through, which is unchanged.</param>
    /// <returns>The branch and the slot that resolves its result.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="slotName"/> is not a valid single-segment identifier,
    /// <paramref name="occurrenceName"/> is not a valid single-segment node identifier, or
    /// <paramref name="parameters"/> is the default value or the JSON null value.
    /// </exception>
    /// <remarks>
    /// The two names mean different things and neither is derivable from the other: the occurrence name is
    /// the node's durable identity in the graph, and the slot name is what a run handle resolves the result
    /// under. The slot's late binding is <see cref="M:Orleans.Dataflow.FSharp.Branch.toResult``3"/>'s
    /// unchanged — a branch is written before the junction call that consumes it, so its slot names its graph
    /// only from that call onwards, and a second junction call over one result-bearing branch is refused.
    /// </remarks>
    let toRegisteredResult
        (slotName: string)
        (stage: Orleans.Dataflow.RegisteredSinkWithResult<'Out, 'Result>)
        (occurrenceName: string)
        (parameters: Orleans.Dataflow.Serialization.CanonicalJsonValue)
        (flow: Flow<'In, 'Out>)
        : Branch<'In> * Orleans.Dataflow.ResultSlot<'Result> =
        let slotId = Bindings.slotId (nameof slotName) slotName
        let binding = BranchSlotBinding()

        let branch =
            Branch<'In>(
                LocalStageChain.Append(
                    flow.Stages,
                    RegisteredAttachment.Occurrence(stage.Specification, occurrenceName, parameters)),
                ValueSome(slotId, binding))

        branch, Orleans.Dataflow.ResultSlot<'Result>.OnBranch(slotId, binding)
