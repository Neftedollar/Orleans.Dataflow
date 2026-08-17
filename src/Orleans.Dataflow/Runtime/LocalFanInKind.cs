namespace Orleans.Dataflow.Runtime;

/// <summary>
/// The fan-in junctions this engine executes, which are the rows of ADR 0005's fan-in table.
/// </summary>
/// <remarks>
/// A discriminator rather than five pump implementations, for the reason <see cref="LocalFanOutKind"/> is
/// one: what the junctions of one shape disagree about is a handful of synchronous answers inside one loop,
/// and nothing else. There are two shapes here rather than one, and the line between them is the row.
/// <see cref="Merge"/>, <see cref="Concat"/>, and <see cref="Interleave"/> deliver what they read and hold
/// nothing between elements, so one loop with one read and one delivery per pass is all of them;
/// <see cref="Zip"/> and <see cref="CombineLatest"/> build an element out of several and therefore hold a
/// row across passes, which no arrangement of that loop can do. See <see cref="LocalFanIn"/> for the answers
/// and <see cref="LocalRun"/> for the two loops.
/// </remarks>
internal enum LocalFanInKind
{
    /// <summary>Emits whichever input has an element, in rotation among the ready ones.</summary>
    Merge,

    /// <summary>Emits one input to its end before reading the next one at all.</summary>
    Concat,

    /// <summary>Emits a declared number of elements from each input in fixed rotation.</summary>
    Interleave,

    /// <summary>Emits one row per element from each input, pairing them positionally.</summary>
    Zip,

    /// <summary>Emits a row of every input's latest element whenever one of them arrives.</summary>
    CombineLatest,
}
