namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One boundary of a compiled plan: the bounded channel that joins two segments, and what it does when it
/// is full.
/// </summary>
/// <remarks>
/// <para>
/// A boundary is the only place a local run holds more than one element. There is exactly one per
/// <c>Buffer</c> the author placed and one per asynchronous stage, and nowhere else — which is what makes
/// the memory a run can occupy the sum of the capacities written in the graph, rather than a property of
/// how fast the stages happen to be.
/// </para>
/// <para>
/// The distinction between the two kinds of boundary is only in where the numbers come from. A buffer
/// declares its own capacity and policy; an asynchronous stage that no buffer precedes gets
/// <see cref="Handoff"/>, the smallest bounded channel there is. One element of handoff is the
/// credit-of-one of checkpoint 1 carried across a segment boundary: it decouples the two loops without
/// buying either of them any prefetch, so an author who wants prefetch asks for it with a buffer and gets
/// exactly what they asked for.
/// </para>
/// </remarks>
internal sealed class LocalBoundary
{
    /// <summary>Initializes a new instance of the <see cref="LocalBoundary"/> class.</summary>
    /// <param name="capacity">The greatest number of elements the channel holds; at least one.</param>
    /// <param name="policy">What the channel does when an element is offered to it and it is full.</param>
    internal LocalBoundary(int capacity, OverflowPolicy policy)
    {
        Capacity = capacity;
        Policy = policy;
    }

    /// <summary>Gets the boundary an asynchronous stage gets when no buffer precedes it.</summary>
    /// <value>A channel of one element that never loses one.</value>
    internal static LocalBoundary Handoff { get; } = new(capacity: 1, OverflowPolicy.Backpressure);

    /// <summary>Gets the greatest number of elements the channel holds at one time.</summary>
    internal int Capacity { get; }

    /// <summary>Gets what the channel does when an element is offered to it and it is full.</summary>
    internal OverflowPolicy Policy { get; }
}
