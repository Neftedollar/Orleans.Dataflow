namespace Orleans.Dataflow.FSharp

open Orleans.Dataflow.Authoring
open Orleans.Dataflow.Identity

// Orleans.Dataflow itself is deliberately not opened: its Source/Flow/Sink are the C# facade's spellings
// of these very concepts, and an open would shadow this package's own types with them. The two shared
// values this module answers with are qualified instead.

/// <summary>Constructs sources and composes them toward a closed graph.</summary>
/// <remarks>
/// The value being transformed is the final argument throughout, so a graph reads top to bottom under
/// <c>|&gt;</c>. Closing a source produces the very <see cref="T:Orleans.Dataflow.RunnableGraph"/> the C#
/// facade produces — one shared closed-graph value, one document, one fingerprint — because both frontends
/// funnel through the one graph builder. The result-bearing close answers a tuple, which is the F# shape
/// of what C# spells with an <c>out</c> parameter.
/// </remarks>
[<RequireQualifiedAccess>]
module Source =

    /// <summary>Creates a source that emits the elements of a sequence, in order.</summary>
    /// <param name="elements">The sequence, enumerated once per run at the run's own pace.</param>
    /// <returns>The source.</returns>
    /// <remarks>
    /// The sequence is the author's: a run enumerates it lazily and disposes the enumerator on every
    /// terminal path, and materializing the graph twice enumerates it twice.
    /// </remarks>
    let ofSeq (elements: seq<'T>) : Source<'T> =
        Source<'T>(LocalGraphShape.OfChain(LocalStageChain.Of(LocalStageDescriptor.FromEnumerable elements)))

    /// <summary>Extends a source with a flow.</summary>
    /// <param name="flow">The transformation to apply.</param>
    /// <param name="source">The source being extended, which is unchanged.</param>
    /// <returns>The extended source.</returns>
    let via (flow: Flow<'In, 'Out>) (source: Source<'In>) : Source<'Out> =
        Source<'Out>(source.State.Concat flow.Stages)

    /// <summary>Transforms every element of a source through a function.</summary>
    /// <param name="mapping">The function applied to each element.</param>
    /// <returns>The transformed source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.map mapping)</c>, producing the identical document.</remarks>
    let map (mapping: 'In -> 'Out) (source: Source<'In>) : Source<'Out> =
        via (Flow.map mapping) source

    /// <summary>Keeps the elements of a source a predicate answers true for.</summary>
    /// <param name="predicate">The predicate deciding each element.</param>
    /// <returns>The filtered source.</returns>
    /// <remarks>Shorthand for <c>Source.via (Flow.filter predicate)</c>, producing the identical document.</remarks>
    let filter (predicate: 'T -> bool) (source: Source<'T>) : Source<'T> =
        via (Flow.filter predicate) source

    /// <summary>Closes a source with a sink that declares no result.</summary>
    /// <param name="sink">The terminal consuming the stream.</param>
    /// <param name="source">The source being closed, which is unchanged.</param>
    /// <returns>The closed graph, ready to materialize.</returns>
    let toSink (sink: Sink<'T>) (source: Source<'T>) : Orleans.Dataflow.RunnableGraph =
        LocalGraphBuilder.Close(source.State.Concat sink.Stages, LocalGraphBuilder.NoSlots)

    /// <summary>Closes a source with a result-bearing sink, naming the slot the result resolves under.</summary>
    /// <param name="slotName">The author-stable name the run handle resolves the result by.</param>
    /// <param name="sink">The terminal folding the stream into the result.</param>
    /// <param name="source">The source being closed, which is unchanged.</param>
    /// <returns>The closed graph and the slot that resolves its result.</returns>
    /// <exception cref="T:System.ArgumentException">
    /// <paramref name="slotName"/> is not a valid single-segment identifier.
    /// </exception>
    /// <remarks>
    /// The slot binds to the document's fingerprint and to this built instance — two graphs of one shape
    /// share a fingerprint, so a slot also remembers which instance declared it, exactly as a C#-declared
    /// slot does (ADR 0004 section 4). The tuple is the composable form; there is no out-parameter
    /// spelling to mirror, because F# already has one.
    /// </remarks>
    let toResult
        (slotName: string)
        (sink: SinkWithResult<'T, 'Result>)
        (source: Source<'T>)
        : Orleans.Dataflow.RunnableGraph * Orleans.Dataflow.ResultSlot<'Result> =
        let slotId =
            match ResultSlotId.TryCreate(slotName) with
            | true, id -> id
            | false, _ ->
                invalidArg
                    (nameof slotName)
                    $"The slot name '{slotName}' is not a valid identifier segment. A result slot is named by a single lowercase segment, such as 'total'."

        let closed = source.State.Concat sink.Stages

        let graph =
            LocalGraphBuilder.Close(
                closed,
                [| LocalSlotRequest(slotId, closed.Stages.Count - 1, null) |])

        graph, Orleans.Dataflow.ResultSlot<'Result>.Create(slotId, graph.Fingerprint, graph.AuthoringNonce)
