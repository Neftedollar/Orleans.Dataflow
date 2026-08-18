namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The head of a merge-map segment: the function that answers one sequence per element, and how many of
/// those sequences may be open at one time.
/// </summary>
/// <remarks>
/// <para>
/// A merge-map is the second stage of this runtime that is not applied on the thread that pulled its
/// element, and it is not the first one wearing a different hat. An asynchronous stage's window holds
/// <see cref="System.Threading.Tasks.Task"/>s, frees a slot when a result is <i>emitted</i>, and sleeps on
/// "an element arrived" or "a callback finished". A merge-map's window holds <i>enumerations</i>, frees a
/// slot when an enumeration <b>ends</b>, and sleeps on "any of the open enumerations has an element" — one
/// outstanding step per live inner, re-armed after each of its elements is delivered. None of the three is
/// a setting of the asynchronous stage, which is why this is a head of its own rather than a flag on that
/// one.
/// </para>
/// <para>
/// <see cref="MaxConcurrency"/> counts enumerations and not elements. Each live enumeration holds at most
/// the one element its last step produced, so what a merge-map segment holds outside its declared boundaries
/// is at most one element per open inner plus the one it is placing — which is the bound a test reads as
/// how far a held source got.
/// </para>
/// </remarks>
internal sealed class LocalMergeMapStage
{
    /// <summary>Initializes a new instance of the <see cref="LocalMergeMapStage"/> class.</summary>
    /// <param name="open">The author's function, wrapped to answer an enumeration over boxed elements.</param>
    /// <param name="maxConcurrency">The greatest number of enumerations open at one time; at least one.</param>
    internal LocalMergeMapStage(LocalInnerCursorFactory open, int maxConcurrency)
    {
        Open = open;
        MaxConcurrency = maxConcurrency;
    }

    /// <summary>Gets the function that opens one inner enumeration for one element.</summary>
    /// <value>
    /// The author's own function wrapped over boxed elements, answering an enumeration whether the author
    /// wrote an asynchronous sequence or an ordinary one.
    /// </value>
    /// <remarks>
    /// Called on the segment's own thread, once per admitted element, and what it throws faults the run
    /// exactly as any other stage's exception does — with the enumerations already open released first.
    /// </remarks>
    internal LocalInnerCursorFactory Open { get; }

    /// <summary>Gets the greatest number of inner enumerations that may be open at one time.</summary>
    /// <remarks>
    /// A slot is freed when an enumeration ends and at no other moment: an inner sequence that produces
    /// nothing frees its slot on its first step, and an endless one never frees its own. That is the whole
    /// difference between this bound and an asynchronous stage's, where a slot is freed by an emission.
    /// </remarks>
    internal int MaxConcurrency { get; }
}
