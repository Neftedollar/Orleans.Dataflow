namespace Orleans.Dataflow.Runtime;

/// <summary>
/// What one fan-in junction does with the inputs it joins: which of them it reads next, what the end of one
/// does to that choice, and what it emits from what it read.
/// </summary>
/// <remarks>
/// <para>
/// The strategies are small, which is the whole design of ADR 0005's fan-in table read from the engine's
/// side. Every junction here is several readers and one writer on one thread; what they disagree about is a
/// handful of synchronous answers, and the waiting, the pause discipline, the completion of the junction,
/// and the failure rule are the pump's and are therefore stated once.
/// </para>
/// <para>
/// There are two pump shapes rather than one, and <see cref="Combiner"/> is which of them a junction is. A
/// merge, a concat, and an interleave deliver the element they read and hold nothing between elements, so
/// one loop with one read and one delivery per pass is all three of them. A zip and a combine-latest build
/// their element out of several inputs' and therefore hold a row across passes: a zip holds the columns of
/// the row it is still assembling, a combine-latest holds the latest element of every input for as long as
/// it runs. That is a different loop rather than a setting of the first, because what a loop is is how many
/// reads stand between two deliveries.
/// </para>
/// <para>
/// The element bounds the table states hold by construction rather than by counting, exactly as they do for
/// a fan-out: the room downstream is secured first and the reads happen second, so a junction never takes an
/// element it has no demand to satisfy. For the three that deliver what they read that bound is one — the
/// element being placed. For a zip it is N−1: the columns already read stay in the junction's hand until the
/// slowest one arrives, and the arrival that completes the row is the one being placed. For a
/// combine-latest it is N, because remembering one element per input is what the operator is.
/// </para>
/// <para>
/// <see cref="Segment"/> and <see cref="Combiner"/> are the only two things a fan-in carries, and each
/// belongs to some of them and not to others. The segment size is payload — a count of elements is something
/// a document can state, and stating it is what makes two graphs that interleave differently two different
/// graphs. The combiner is behavior — which member of a row each input contributes is a statement about
/// element types, and an element type never appears in a local document.
/// </para>
/// </remarks>
internal sealed class LocalFanIn
{
    /// <summary>Initializes a new instance of the <see cref="LocalFanIn"/> class.</summary>
    /// <param name="kind">Which junction this is.</param>
    /// <param name="segment">The number of elements taken from one input before the rotation moves on.</param>
    /// <param name="combiner">The builder of one row from the inputs' elements, for the row-building pair.</param>
    private LocalFanIn(LocalFanInKind kind, int segment, Func<object?[], object?>? combiner)
    {
        Kind = kind;
        Segment = segment;
        Combiner = combiner;
    }

    /// <summary>Gets which junction this is.</summary>
    internal LocalFanInKind Kind { get; }

    /// <summary>Gets the number of elements this junction takes from one input before moving on.</summary>
    /// <value>
    /// The declared segment size of an interleave, which is at least one; one for a merge, whose rotation
    /// advances after every element it takes; and one for a concat, which never rotates on a count at all,
    /// and for the two row-building junctions, which take one element per input per row.
    /// </value>
    /// <remarks>
    /// Only an interleave reads this. It is stated for the others rather than left undefined because a
    /// junction is a value and a value with a member nobody may look at is a trap; one is the honest answer
    /// for all of them — a merge does advance after one element, a concat advances on the end of an input
    /// and on nothing else, and no row takes two elements from one input.
    /// </remarks>
    internal int Segment { get; }

    /// <summary>Gets the builder of the row this junction emits from the elements it read.</summary>
    /// <value>
    /// The combiner of a zip or a combine-latest, which receives one element per wired input in port order;
    /// <see langword="null"/> for the three junctions that emit the element they read and never look inside
    /// it.
    /// </value>
    /// <remarks>
    /// Also the discriminator between the two pump shapes, and deliberately so: a junction that builds rows
    /// is exactly a junction that has something to build them with, so there is one fact here rather than a
    /// flag beside a delegate that could disagree with it. The array a combiner receives is fresh per row —
    /// the junction copies its held elements into it rather than handing out the slots it goes on writing
    /// into, because an author who keeps the array would otherwise watch later rows change it.
    /// </remarks>
    internal Func<object?[], object?>? Combiner { get; }

    /// <summary>Creates the strategy of a junction that emits whichever input has an element.</summary>
    /// <returns>The strategy.</returns>
    /// <remarks>
    /// The rotation is what keeps it fair. A merge that always looked at its inputs in port order would
    /// starve every input but the first whenever the first is never empty, and an element that has already
    /// arrived at a junction waiting behind a producer that is merely faster is exactly what ADR 0005's
    /// round-robin among the ready ones forbids.
    /// </remarks>
    internal static LocalFanIn Merge() => new(LocalFanInKind.Merge, segment: 1, combiner: null);

    /// <summary>Creates the strategy of a junction that emits one input to its end before the next.</summary>
    /// <returns>The strategy.</returns>
    internal static LocalFanIn Concat() => new(LocalFanInKind.Concat, segment: 1, combiner: null);

    /// <summary>Creates the strategy of a junction that emits a fixed number of elements per input.</summary>
    /// <param name="segment">The declared segment size, which is at least one.</param>
    /// <returns>The strategy.</returns>
    /// <remarks>
    /// A merge with determinism bought at the price of head-of-line waiting: the input whose turn it is is
    /// waited for even when another input has an element ready, which is what makes the output sequence a
    /// function of the inputs rather than of the scheduler.
    /// </remarks>
    internal static LocalFanIn Interleave(int segment) =>
        new(LocalFanInKind.Interleave, segment, combiner: null);

    /// <summary>Creates the strategy of a junction that pairs its inputs' elements positionally.</summary>
    /// <param name="combiner">The builder of one row from one element of every input, in port order.</param>
    /// <returns>The strategy.</returns>
    /// <remarks>
    /// One row per element from each input, which is the whole of its demand rule as well as its ordering
    /// one: an input that has already given the pending row its column is not read again until that row is
    /// emitted, so the elements of one row are the i-th of every input and nothing else. It completes as
    /// soon as any input does, because a zip missing a column can never emit again, and the columns it was
    /// holding at that moment are discarded rather than kept for a row that cannot arrive.
    /// </remarks>
    internal static LocalFanIn Zip(Func<object?[], object?> combiner) =>
        new(LocalFanInKind.Zip, segment: 1, combiner);

    /// <summary>Creates the strategy of a junction that emits every input's latest element on any arrival.</summary>
    /// <param name="combiner">The builder of one row from the latest element of every input, in port order.</param>
    /// <returns>The strategy.</returns>
    /// <remarks>
    /// Rx semantics rather than Akka's <c>zipLatest</c>, as ADR 0005 decides: nothing is emitted until every
    /// input has produced at least once, every arrival after that emits one row, an input that completes
    /// leaves its last element frozen into every later row, and the junction completes only when every input
    /// has. An input that completes without ever producing therefore ends the junction's ability to emit
    /// anything at all, and the run ends cleanly with no rows.
    /// </remarks>
    internal static LocalFanIn CombineLatest(Func<object?[], object?> combiner) =>
        new(LocalFanInKind.CombineLatest, segment: 1, combiner);
}
