using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One ending of a plan: the segment a branch stops at, the state its terminal starts from, and the result
/// slot that state resolves.
/// </summary>
/// <remarks>
/// <para>
/// A linear plan has exactly one of these and a branching plan has one per sink, which is the whole of
/// "terminals are counted, not singular". Everything a run used to keep about its single terminal — the
/// seed it folds from, whether it ever saw an element, the slot it settles — is kept per ending instead,
/// so two sinks of one graph accumulate two states and resolve two slots, and neither can observe the
/// other's.
/// </para>
/// <para>
/// What stays shared is the outcome. A run ends once, in one state, however many endings it has: a failure
/// in any branch fails every slot, a cancellation cancels every slot, and the run's completion reports the
/// single answer. That is ADR 0005's first shared rule seen from the far end of the graph — failure wins,
/// and it wins everywhere rather than in the branch that raised it.
/// </para>
/// <para>
/// The seed lives here and the state does not, for the reason the plan holds no run state at all: a plan is
/// built once per materialization and describes what a run starts from, and a run is what a state is fresh
/// per. <see cref="SeedFactory"/> is the same rule applied to a state that accumulates rather than
/// replaces — a collecting sink's list has to be a new list per run even though the plan is one plan.
/// </para>
/// </remarks>
internal sealed class LocalEnding
{
    /// <summary>Initializes a new instance of the <see cref="LocalEnding"/> class.</summary>
    /// <param name="segment">The position of the segment this branch ends at.</param>
    /// <param name="seed">The terminal's initial state, meaningful only when the terminal has one.</param>
    /// <param name="seedFactory">
    /// The maker of the terminal's initial state, for a terminal whose state is mutable.
    /// </param>
    /// <param name="slot">The result slot the terminal's final state resolves, or <see langword="null"/>.</param>
    internal LocalEnding(int segment, object? seed, Func<object?>? seedFactory, ResultSlotId? slot)
    {
        Segment = segment;
        Seed = seed;
        SeedFactory = seedFactory;
        Slot = slot;
    }

    /// <summary>Gets the position in the plan of the segment this branch ends at.</summary>
    /// <value>The index of a segment that writes into no channel, because there is nothing below it.</value>
    internal int Segment { get; }

    /// <summary>Gets the terminal's initial state.</summary>
    /// <value>
    /// The seed the author wrote, the zero a count starts from, or the default value an honest
    /// first-element sink resolves when it saw nothing; any of them may legitimately be
    /// <see langword="null"/>, and the segment's <see cref="LocalSegment.Terminal"/> and not this value
    /// decides whether a state exists at all.
    /// </value>
    internal object? Seed { get; }

    /// <summary>Gets the maker of the terminal's initial state, when the state cannot be shared.</summary>
    /// <value>
    /// The factory for a collecting sink, whose state is a list a run appends to; <see langword="null"/>
    /// for every terminal whose seed is a value two runs may hold at once.
    /// </value>
    internal Func<object?>? SeedFactory { get; }

    /// <summary>Gets the result slot the terminal's final state resolves.</summary>
    /// <value>
    /// The slot name the document declares for this terminal, or <see langword="null"/> when this ending
    /// exposes no result.
    /// </value>
    /// <remarks>
    /// A result-bearing terminal with no slot is a real case rather than a defect: converting such a sink
    /// through <see cref="SinkWithResult{TIn, TResult}.ToSink"/> keeps the terminal and drops the
    /// declaration, so the run still folds every element and simply exposes nothing to ask for.
    /// </remarks>
    internal ResultSlotId? Slot { get; }
}
