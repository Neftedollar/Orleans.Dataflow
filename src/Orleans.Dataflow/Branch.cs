using System.Globalization;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow;

/// <summary>
/// One leg of a junction, complete: everything the elements that take it go through, ending in the sink
/// that consumes them.
/// </summary>
/// <typeparam name="TIn">The element type the leg receives from the junction.</typeparam>
/// <remarks>
/// <para>
/// A branch is a value and starts nothing, like every other authoring value. It is built by the <c>To</c>
/// family on <see cref="Flow{TIn, TOut}"/> and consumed by a junction call on <see cref="Source{T}"/> —
/// <see cref="Source{T}.BroadcastTo"/>, <see cref="Source{T}.BalanceTo"/>,
/// <see cref="Source{T}.PartitionTo"/>, <see cref="Source.UnzipTo{TLeft, TRight}"/>, or
/// <see cref="Source{T}.AlsoTo"/> — and it exists as a type because a leg has no receiver to hang off:
/// type information flows left to right from sources, and a leg is built right to left from its sink.
/// <see cref="Flow.For{T}"/> is the anchor that fixes the element type, and a leg cannot be written
/// without it.
/// </para>
/// <para>
/// A branch that declares no result is reusable exactly as a flow is: composing it into two graphs
/// contributes its occurrences to both, and composing it twice into one graph contributes them twice and
/// numbers them as the distinct occurrences they are. A branch that <em>does</em> declare a result closes
/// exactly one graph, because its slot binds to the graph that closed it and a second graph would take that
/// binding over; the second attempt is refused with a diagnostic rather than silently repointing the first
/// slot.
/// </para>
/// <para>
/// The type has no members of its own on purpose, for the reason <see cref="Sink{T}"/> has none: everything
/// an author does with a branch is done by the junction call that consumes it, and operators on a branch
/// would invite a second, mirror-image way to build the same graph.
/// </para>
/// </remarks>
public sealed class Branch<TIn>
{
    /// <summary>Initializes a new instance of the <see cref="Branch{TIn}"/> class.</summary>
    /// <param name="stages">The occurrences this branch contributes, in authoring order.</param>
    /// <param name="slotName">The name of the result its terminal declares, when it declares one.</param>
    /// <param name="binding">The binding of that result's slot, waiting for the graph that closes it.</param>
    internal Branch(
        IReadOnlyList<StageOccurrence> stages,
        ResultSlotId? slotName = null,
        BranchSlotBinding? binding = null)
    {
        Stages = stages;
        SlotName = slotName;
        Binding = binding;
    }

    /// <summary>Gets the occurrences this branch contributes to a graph, in authoring order.</summary>
    /// <value>The flow's occurrences followed by the sink's; never empty, because a branch ends in a sink.</value>
    internal IReadOnlyList<StageOccurrence> Stages { get; }

    /// <summary>Gets the name the result of this branch is exposed under.</summary>
    /// <value>The slot name, or <see langword="null"/> when the branch's terminal declares no result.</value>
    internal ResultSlotId? SlotName { get; }

    /// <summary>Gets the binding the closing junction call fills with the graph's identity.</summary>
    /// <value>
    /// The binding shared with the <see cref="ResultSlot{TResult}"/> this branch handed back, or
    /// <see langword="null"/> when it declared no result.
    /// </value>
    internal BranchSlotBinding? Binding { get; }

    /// <summary>Returns a one-line diagnostic summary of this branch.</summary>
    /// <returns>
    /// Text of the form <c>branch (2 stages)</c>, or <c>branch (2 stages, result 'counted')</c> when the
    /// branch declares a result.
    /// </returns>
    /// <remarks>The count is formatted with the invariant culture, and the method never throws.</remarks>
    public override string ToString() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"branch ({Stages.Count} {(Stages.Count == 1 ? "stage" : "stages")}{(SlotName is { } name ? $", result '{name}'" : string.Empty)})");
}
