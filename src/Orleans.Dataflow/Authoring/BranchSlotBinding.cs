using Orleans.Dataflow.Definition;

namespace Orleans.Dataflow.Authoring;

/// <summary>
/// The identity a branch's result slot is waiting for: the fingerprint and the authoring nonce of the graph
/// that will contain the branch, filled in when that graph is closed.
/// </summary>
/// <remarks>
/// <para>
/// Every other result slot is handed back by the call that closes a graph, so the graph it binds to already
/// exists when the slot is made. A branch is built before the junction call that consumes it — that is the
/// whole point of ADR 0006's finding, that a branch is a value and its slot is named where the sink is
/// written — so the slot exists for the width of one argument list before there is a graph for it to belong
/// to. This is that gap, made explicit rather than papered over with a slot bound to nothing.
/// </para>
/// <para>
/// A binding is filled exactly once, and atomically, so a branch that declares a result closes exactly one
/// graph however many threads try: consuming it twice would leave the first graph's slot pointing at the
/// second graph, which is precisely the silent cross-graph resolution ADR 0004 section 4 introduced the
/// nonce to prevent. The second attempt is refused with a diagnostic instead. Two closures racing over one
/// branch therefore have one winner and one exception rather than two graphs and one wrong slot, which is
/// what keeps a branch an immutable, freely shared authoring value like every other.
/// </para>
/// <para>
/// Reading an unfilled binding throws. The window in which that is possible is one argument list wide — a
/// branch is written as an argument of the junction call that consumes it — so an author reaches it only by
/// storing the branch in a variable and inspecting the slot before building anything, and being told that
/// the slot names no graph yet is the honest answer to that.
/// </para>
/// </remarks>
internal sealed class BranchSlotBinding
{
    /// <summary>The identity of the graph that closed over the branch, or <see langword="null"/> while none has.</summary>
    /// <remarks>
    /// One reference rather than two fields, so that the whole identity appears at once: a reader either
    /// sees no graph or sees both halves of one, and never a fingerprint beside another graph's nonce.
    /// </remarks>
    private Closure? _closure;

    /// <summary>Gets a value indicating whether the graph that declares this slot has been closed.</summary>
    internal bool IsBound => Volatile.Read(ref _closure) is not null;

    /// <summary>Gets the fingerprint of the graph that declared the slot, without throwing.</summary>
    /// <value>The fingerprint, or the default value while the binding is unfilled.</value>
    /// <remarks>
    /// Equality and text rendering read the components through this and its sibling rather than through the
    /// throwing accessors, because neither operation may fail: a slot has to be comparable and loggable in
    /// every state it can be in, including the one this type exists to represent.
    /// </remarks>
    internal GraphFingerprint GraphOrDefault => Volatile.Read(ref _closure)?.Graph ?? default;

    /// <summary>Gets the authoring nonce of the graph that declared the slot, without throwing.</summary>
    /// <value>The nonce, or <see cref="Guid.Empty"/> while the binding is unfilled.</value>
    internal Guid AuthoringNonceOrDefault => Volatile.Read(ref _closure)?.AuthoringNonce ?? Guid.Empty;

    /// <summary>Gets the fingerprint of the graph that declared the slot.</summary>
    /// <exception cref="InvalidOperationException">The graph has not been closed yet.</exception>
    internal GraphFingerprint Graph => (Volatile.Read(ref _closure) ?? throw Unbound()).Graph;

    /// <summary>Gets the authoring nonce of the graph that declared the slot.</summary>
    /// <exception cref="InvalidOperationException">The graph has not been closed yet.</exception>
    internal Guid AuthoringNonce => (Volatile.Read(ref _closure) ?? throw Unbound()).AuthoringNonce;

    /// <summary>Fills this binding with the identity of the graph that closed over the branch.</summary>
    /// <param name="graph">The closed document's fingerprint.</param>
    /// <param name="authoringNonce">The built graph instance's nonce.</param>
    /// <exception cref="InvalidOperationException">This binding was already filled by another graph.</exception>
    internal void Bind(GraphFingerprint graph, Guid authoringNonce)
    {
        if (Interlocked.CompareExchange(ref _closure, new Closure(graph, authoringNonce), null) is { } declared)
        {
            throw new InvalidOperationException(
                $"This branch already declares a result of the graph {declared.Graph}, and a result slot belongs to one graph. A branch that declares a result closes exactly one graph: build a second branch for the second graph, which is one more call and gives the second result a name of its own. A branch that declares no result is reusable without limit.");
        }
    }

    /// <summary>Builds the exception for reading a binding that no graph has filled yet.</summary>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException Unbound() =>
        new("This result slot was declared by a branch, and the graph that contains the branch has not been closed yet. A branch is consumed by a junction call — BroadcastTo, BalanceTo, PartitionTo, UnzipTo, or AlsoTo — and the slot names its graph from that call onwards.");

    /// <summary>The identity of the one graph a branch's result belongs to.</summary>
    /// <param name="Graph">The closed document's fingerprint.</param>
    /// <param name="AuthoringNonce">The built graph instance's nonce.</param>
    private sealed record class Closure(GraphFingerprint Graph, Guid AuthoringNonce);
}
