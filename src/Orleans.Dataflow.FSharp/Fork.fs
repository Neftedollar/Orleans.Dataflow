namespace Orleans.Dataflow.FSharp

open System
open Orleans.Dataflow.Authoring

// Orleans.Dataflow itself is deliberately not opened: see the note in Source.fs.

/// <summary>Rejoins the two derived streams of a diamond.</summary>
/// <remarks>
/// <para>
/// A fork has two open ends and no way to close a graph, so a program that builds one has to rejoin it —
/// which is what makes these the only two functions the type needs. Both are total and positional: one
/// element into the fork produces one element out, built from that element's two derivations and never from
/// two different elements'. That is what separates this join from
/// <see cref="M:Orleans.Dataflow.FSharp.Source.zip``2"/> over two unrelated sources, which is positional and
/// nothing more.
/// </para>
/// <para>
/// <see cref="M:Orleans.Dataflow.FSharp.Source.forkMerge``2"/> is the other rejoin, for when the two paths
/// produce one element type and the answer wanted is whichever arrives first. It lives on the source because
/// it never produces a fork value at all.
/// </para>
/// </remarks>
[<RequireQualifiedAccess>]
module Fork =

    /// <summary>Rejoins the two derived streams through a function of both.</summary>
    /// <param name="combine">The function building one element from the two derivations of one input element.</param>
    /// <param name="fork">The diamond to close, which is unchanged.</param>
    /// <returns>A source of one element per element the fork was fed.</returns>
    /// <remarks>
    /// The function never enters the document — a local graph states which stage stands where and never how
    /// an element was built — so a graph rejoined this way is <c>nondeployable</c> exactly as every other
    /// local graph holding a lambda is.
    /// </remarks>
    let zipWith (combine: 'T1 -> 'T2 -> 'Out) (fork: Fork<'T1, 'T2>) : Source<'Out> =
        Source<'Out>(
            fork.State.Combine(
                LocalStageDescriptor.Zip(LocalRowCombiner.Of(Func<'T1, 'T2, 'Out> combine)),
                LocalJunctionGuard.FanInPorts LocalVocabulary.MinFanIn))

    /// <summary>Rejoins the two derived streams into a stream of pairs.</summary>
    /// <param name="fork">The diamond to close, which is unchanged.</param>
    /// <returns>A source of one pair per element the fork was fed.</returns>
    /// <remarks>
    /// The pair names its halves by the order the fork was written in: the first member is the left flow's
    /// element and the second is the right flow's. It is a struct tuple because that is the very
    /// <see cref="T:System.ValueTuple`2"/> the C# facade's own rejoin produces, so one graph authored in
    /// either language carries one element type as well as one document.
    /// </remarks>
    let zip (fork: Fork<'T1, 'T2>) : Source<struct ('T1 * 'T2)> =
        zipWith (fun first second -> struct (first, second)) fork
