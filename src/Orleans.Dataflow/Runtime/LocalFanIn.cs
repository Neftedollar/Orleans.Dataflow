namespace Orleans.Dataflow.Runtime;

/// <summary>
/// What one fan-in junction does with the inputs it joins: which of them it reads next, and what the end of
/// one does to that choice.
/// </summary>
/// <remarks>
/// <para>
/// The pump shape is one and the strategies are small, which is the whole design of ADR 0005's fan-in
/// table read from the engine's side. Every junction here is several readers and one writer on one thread;
/// what a merge, a concat, and an interleave disagree about is which reader the next element comes from,
/// and that is the only question this type answers. The waiting, the pause discipline, the completion of
/// the junction, and the failure rule are the pump's and are therefore stated once.
/// </para>
/// <para>
/// The element bound the table states — one, for all three — holds by construction rather than by counting,
/// exactly as it does for a fan-out: the room downstream is secured first and the read happens second, so
/// the one element such a junction ever holds is the one it is placing. A junction that read first and then
/// waited would hold that element for the whole of the wait, and would have taken it from an input it had
/// no demand to satisfy.
/// </para>
/// <para>
/// <see cref="Segment"/> is the only number a fan-in carries, and it belongs to the interleave alone. It is
/// payload rather than behavior — a count of elements is something a document can state, and stating it is
/// what makes two graphs that interleave differently two different graphs.
/// </para>
/// </remarks>
internal sealed class LocalFanIn
{
    /// <summary>Initializes a new instance of the <see cref="LocalFanIn"/> class.</summary>
    /// <param name="kind">Which junction this is.</param>
    /// <param name="segment">The number of elements taken from one input before the rotation moves on.</param>
    private LocalFanIn(LocalFanInKind kind, int segment)
    {
        Kind = kind;
        Segment = segment;
    }

    /// <summary>Gets which junction this is.</summary>
    internal LocalFanInKind Kind { get; }

    /// <summary>Gets the number of elements this junction takes from one input before moving on.</summary>
    /// <value>
    /// The declared segment size of an interleave, which is at least one; one for a merge, whose rotation
    /// advances after every element it takes; and one for a concat, which never rotates on a count at all.
    /// </value>
    /// <remarks>
    /// Only an interleave reads this. It is stated for the other two rather than left undefined because a
    /// junction is a value and a value with a member nobody may look at is a trap; one is the honest answer
    /// for both — a merge does advance after one element, and a concat advances on the end of an input and
    /// on nothing else.
    /// </remarks>
    internal int Segment { get; }

    /// <summary>Creates the strategy of a junction that emits whichever input has an element.</summary>
    /// <returns>The strategy.</returns>
    /// <remarks>
    /// The rotation is what keeps it fair. A merge that always looked at its inputs in port order would
    /// starve every input but the first whenever the first is never empty, and an element that has already
    /// arrived at a junction waiting behind a producer that is merely faster is exactly what ADR 0005's
    /// round-robin among the ready ones forbids.
    /// </remarks>
    internal static LocalFanIn Merge() => new(LocalFanInKind.Merge, segment: 1);

    /// <summary>Creates the strategy of a junction that emits one input to its end before the next.</summary>
    /// <returns>The strategy.</returns>
    internal static LocalFanIn Concat() => new(LocalFanInKind.Concat, segment: 1);

    /// <summary>Creates the strategy of a junction that emits a fixed number of elements per input.</summary>
    /// <param name="segment">The declared segment size, which is at least one.</param>
    /// <returns>The strategy.</returns>
    /// <remarks>
    /// A merge with determinism bought at the price of head-of-line waiting: the input whose turn it is is
    /// waited for even when another input has an element ready, which is what makes the output sequence a
    /// function of the inputs rather than of the scheduler.
    /// </remarks>
    internal static LocalFanIn Interleave(int segment) => new(LocalFanInKind.Interleave, segment);
}
